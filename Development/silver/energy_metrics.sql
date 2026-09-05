-- ============================================================
-- SILVER LAYER - ENERGY METRICS
-- ============================================================
-- Source:
-- energydbs.bronze.energy_metrics
--
-- Transformations:
-- 1. Remove duplicate records
-- 2. Handle NULL values
-- 3. Trim leading/trailing spaces
-- 4. Standardize categorical values
-- 5. Convert numeric columns to DOUBLE
-- 6. Convert timestamp to TIMESTAMP
-- 7. Fill missing/invalid timestamps with default
-- 8. Derive date/time attributes
-- 9. Validate energy and demand measurements
-- ============================================================


WITH ranked AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                household_id,
                region_name,
                city_name,
                meter_type,
                customer_category,
                grid_zone,
                voltage_reading,
                current_reading,
                active_power_kw,
                reactive_power_kvar,
                energy_usage_kwh,
                frequency_hz,
                load_factor,
                peak_demand_kw,
                offpeak_demand_kw,
                daily_consumption_kwh,
                timestamp

            ORDER BY timestamp
        ) AS rn

    FROM {{ source('bronze', 'energy_metrics') }}
),


-- ============================================================
-- STEP 1: REMOVE EXACT DUPLICATES
-- ============================================================

deduplicated AS (

    SELECT *
    FROM ranked
    WHERE rn = 1

),


-- ============================================================
-- STEP 2-7: CLEAN AND STANDARDIZE
-- ============================================================

cleaned AS (

    SELECT

        -- ====================================================
        -- IDENTIFIER
        -- ====================================================

        TRY_CAST(
            household_id AS BIGINT
        ) AS household_id,


        -- ====================================================
        -- CATEGORICAL COLUMNS
        -- TRIM + STANDARDIZATION + NULL HANDLING
        -- ====================================================

        COALESCE(
            UPPER(TRIM(region_name)),
            'UNKNOWN'
        ) AS region_name,

        COALESCE(
            INITCAP(TRIM(city_name)),
            'Unknown'
        ) AS city_name,

        COALESCE(
            UPPER(TRIM(meter_type)),
            'UNKNOWN'
        ) AS meter_type,

        COALESCE(
            UPPER(TRIM(customer_category)),
            'UNKNOWN'
        ) AS customer_category,

        COALESCE(
            UPPER(TRIM(grid_zone)),
            'UNKNOWN'
        ) AS grid_zone,


        -- ====================================================
        -- NUMERIC COLUMNS
        -- NULL / INVALID → 0.0
        -- ====================================================

        COALESCE(
            TRY_CAST(voltage_reading AS DOUBLE),
            0.0
        ) AS voltage_reading,

        COALESCE(
            TRY_CAST(current_reading AS DOUBLE),
            0.0
        ) AS current_reading,

        COALESCE(
            TRY_CAST(active_power_kw AS DOUBLE),
            0.0
        ) AS active_power_kw,

        COALESCE(
            TRY_CAST(reactive_power_kvar AS DOUBLE),
            0.0
        ) AS reactive_power_kvar,

        COALESCE(
            TRY_CAST(energy_usage_kwh AS DOUBLE),
            0.0
        ) AS energy_usage_kwh,

        COALESCE(
            TRY_CAST(frequency_hz AS DOUBLE),
            0.0
        ) AS frequency_hz,

        COALESCE(
            TRY_CAST(load_factor AS DOUBLE),
            0.0
        ) AS load_factor,

        COALESCE(
            TRY_CAST(peak_demand_kw AS DOUBLE),
            0.0
        ) AS peak_demand_kw,

        COALESCE(
            TRY_CAST(offpeak_demand_kw AS DOUBLE),
            0.0
        ) AS offpeak_demand_kw,

        COALESCE(
            TRY_CAST(daily_consumption_kwh AS DOUBLE),
            0.0
        ) AS daily_consumption_kwh,


        -- ====================================================
        -- TIMESTAMP CONVERSION
        --
        -- Valid timestamp → converted timestamp
        -- NULL / blank / '-' / invalid → default timestamp
        -- ====================================================

        COALESCE(

            TRY_TO_TIMESTAMP(
                NULLIF(
                    TRIM(timestamp),
                    '-'
                ),
                'dd-MM-yyyy HH:mm'
            ),

            TIMESTAMP('1900-01-01 00:00:00')

        ) AS usage_timestamp,


        -- ====================================================
        -- TIMESTAMP QUALITY STATUS
        -- ====================================================

        CASE

            WHEN timestamp IS NULL
                 OR TRIM(timestamp) = ''
                 OR TRIM(timestamp) = '-'
                THEN 'Missing'

            WHEN TRY_TO_TIMESTAMP(
                TRIM(timestamp),
                'dd-MM-yyyy HH:mm'
            ) IS NULL
                THEN 'Invalid'

            ELSE 'Valid'

        END AS timestamp_status

    FROM deduplicated
),


-- ============================================================
-- STEP 8-9: DERIVATIONS AND VALIDATION
-- ============================================================

final AS (

    SELECT

        -- ====================================================
        -- IDENTIFIERS / CATEGORIES
        -- ====================================================

        household_id,

        region_name,

        city_name,

        meter_type,

        customer_category,

        grid_zone,


        -- ====================================================
        -- MEASUREMENTS
        -- ====================================================

        voltage_reading,

        current_reading,

        active_power_kw,

        reactive_power_kvar,

        energy_usage_kwh,

        frequency_hz,

        load_factor,

        peak_demand_kw,

        offpeak_demand_kw,

        daily_consumption_kwh,


        -- ====================================================
        -- TIMESTAMP
        -- ====================================================

        usage_timestamp,


        -- ====================================================
        -- DATE/TIME DERIVATIONS
        -- ====================================================

        TO_DATE(usage_timestamp) AS usage_date,

        YEAR(usage_timestamp) AS usage_year,

        MONTH(usage_timestamp) AS usage_month,

        DAY(usage_timestamp) AS usage_day,

        HOUR(usage_timestamp) AS usage_hour,

        DAYOFWEEK(usage_timestamp) AS day_of_week,

        DATE_FORMAT(
            usage_timestamp,
            'EEEE'
        ) AS day_name,


        -- ====================================================
        -- TIMESTAMP STATUS
        -- ====================================================

        timestamp_status,


        -- ====================================================
        -- ENERGY VALIDATION
        -- ====================================================

        CASE
            WHEN energy_usage_kwh < 0
                THEN 'Invalid'
            ELSE 'Valid'
        END AS energy_status,


        -- ====================================================
        -- DAILY CONSUMPTION VALIDATION
        -- ====================================================

        CASE
            WHEN daily_consumption_kwh < 0
                THEN 'Invalid'
            ELSE 'Valid'
        END AS daily_consumption_status,


        -- ====================================================
        -- PEAK DEMAND VALIDATION
        -- ====================================================

        CASE
            WHEN peak_demand_kw < 0
                THEN 'Invalid'
            ELSE 'Valid'
        END AS peak_demand_status,


        -- ====================================================
        -- OFF-PEAK DEMAND VALIDATION
        -- ====================================================

        CASE
            WHEN offpeak_demand_kw < 0
                THEN 'Invalid'
            ELSE 'Valid'
        END AS offpeak_demand_status

    FROM cleaned
)


-- ============================================================
-- FINAL SILVER TABLE
-- ============================================================

SELECT *
FROM final;