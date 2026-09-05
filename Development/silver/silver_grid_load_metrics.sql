/*
    SILVER LAYER - GRID LOAD METRICS

    Transformations:
    1. Remove duplicate records
    2. Handle NULL / missing grid measurements
    3. Trim and standardize grid attributes
    4. Convert grid metrics to numeric types
    5. Validate load, capacity and measurement values
    6. Validate percentage-based metrics
*/



-- ============================================================
-- SOURCE
-- ============================================================

with source as (

    select *
    from {{ source('bronze', 'sink_grid_load') }}

),

-- ============================================================
-- 1. REMOVE DUPLICATE RECORDS
-- ============================================================

deduped as (

    select *
    from (

        select
            *,
            row_number() over (
                partition by
                    household_id,
                    grid_region,
                    substation_name,
                    feeder_line,
                    distribution_zone,
                    grid_operator,
                    grid_voltage,
                    grid_current,
                    grid_load_kw,
                    transformer_load,
                    line_loss_percent,
                    load_variation,
                    frequency_variation,
                    grid_capacity_kw,
                    demand_forecast_kw,
                    reserve_margin
                order by household_id
            ) as row_num

        from source

    ) ranked

    where row_num = 1

),

-- ============================================================
-- 2. HANDLE NULL / MISSING GRID VALUES
-- ============================================================

nulls_handled as (

    select

        household_id,

        nullif(
            trim(cast(grid_region as string)),
            ''
        ) as grid_region,

        nullif(
            trim(cast(substation_name as string)),
            ''
        ) as substation_name,

        nullif(
            trim(cast(feeder_line as string)),
            ''
        ) as feeder_line,

        nullif(
            trim(cast(distribution_zone as string)),
            ''
        ) as distribution_zone,

        nullif(
            trim(cast(grid_operator as string)),
            ''
        ) as grid_operator,

        nullif(
            trim(cast(grid_voltage as string)),
            ''
        ) as grid_voltage,

        nullif(
            trim(cast(grid_current as string)),
            ''
        ) as grid_current,

        nullif(
            trim(cast(grid_load_kw as string)),
            ''
        ) as grid_load_kw,

        nullif(
            trim(cast(transformer_load as string)),
            ''
        ) as transformer_load,

        nullif(
            trim(cast(line_loss_percent as string)),
            ''
        ) as line_loss_percent,

        nullif(
            trim(cast(load_variation as string)),
            ''
        ) as load_variation,

        nullif(
            trim(cast(frequency_variation as string)),
            ''
        ) as frequency_variation,

        nullif(
            trim(cast(grid_capacity_kw as string)),
            ''
        ) as grid_capacity_kw,

        nullif(
            trim(cast(demand_forecast_kw as string)),
            ''
        ) as demand_forecast_kw,

        nullif(
            trim(cast(reserve_margin as string)),
            ''
        ) as reserve_margin

    from deduped

),

-- ============================================================
-- 3. TRIM AND STANDARDIZE GRID ATTRIBUTES
-- ============================================================

standardized as (

    select

        household_id,

        initcap(lower(trim(grid_region)))
            as grid_region,

        upper(trim(substation_name))
            as substation_name,

        upper(trim(feeder_line))
            as feeder_line,

        upper(trim(distribution_zone))
            as distribution_zone,

        initcap(lower(trim(grid_operator)))
            as grid_operator,

        grid_voltage,
        grid_current,
        grid_load_kw,
        transformer_load,
        line_loss_percent,
        load_variation,
        frequency_variation,
        grid_capacity_kw,
        demand_forecast_kw,
        reserve_margin

    from nulls_handled

),

-- ============================================================
-- 4. CONVERT GRID METRICS TO NUMERIC TYPES
-- ============================================================

casted as (

    select

        household_id,

        grid_region,
        substation_name,
        feeder_line,
        distribution_zone,
        grid_operator,

        try_cast(
            grid_voltage as decimal(12,4)
        ) as grid_voltage,

        try_cast(
            grid_current as decimal(12,4)
        ) as grid_current,

        try_cast(
            grid_load_kw as decimal(14,4)
        ) as grid_load_kw,

        try_cast(
            transformer_load as decimal(14,4)
        ) as transformer_load,

        try_cast(
            line_loss_percent as decimal(10,4)
        ) as line_loss_percent,

        try_cast(
            load_variation as decimal(12,4)
        ) as load_variation,

        try_cast(
            frequency_variation as decimal(12,4)
        ) as frequency_variation,

        try_cast(
            grid_capacity_kw as decimal(14,4)
        ) as grid_capacity_kw,

        try_cast(
            demand_forecast_kw as decimal(14,4)
        ) as demand_forecast_kw,

        try_cast(
            reserve_margin as decimal(12,4)
        ) as reserve_margin

    from standardized

),

-- ============================================================
-- 5. VALIDATE LOAD, CAPACITY AND MEASUREMENT VALUES
-- ============================================================

validated as (

    select

        *,

        -- Grid voltage must be >= 0

        (
            grid_voltage is not null
            and grid_voltage >= 0
        ) as is_valid_grid_voltage,

        -- Grid current must be >= 0

        (
            grid_current is not null
            and grid_current >= 0
        ) as is_valid_grid_current,

        -- Grid load must be >= 0

        (
            grid_load_kw is not null
            and grid_load_kw >= 0
        ) as is_valid_grid_load,

        -- Transformer load must be >= 0

        (
            transformer_load is not null
            and transformer_load >= 0
        ) as is_valid_transformer_load,

        -- Grid capacity must be > 0

        (
            grid_capacity_kw is not null
            and grid_capacity_kw > 0
        ) as is_valid_grid_capacity,

        -- Demand forecast must be >= 0

        (
            demand_forecast_kw is not null
            and demand_forecast_kw >= 0
        ) as is_valid_demand_forecast

    from casted

),

-- ============================================================
-- 6. VALIDATE PERCENTAGE-BASED METRICS
-- ============================================================

percentage_validated as (

    select

        *,

        /*
            Line loss is a percentage.
            Expected range: 0 - 100
        */

        (
            line_loss_percent is not null
            and line_loss_percent between 0 and 100
        ) as is_valid_line_loss_percent,

        /*
            Reserve margin is a percentage.
            Expected range: 0 - 100
        */

        (
            reserve_margin is not null
            and reserve_margin between 0 and 100
        ) as is_valid_reserve_margin

    from validated

),

-- ============================================================
-- FINAL RECORD VALIDATION
-- ============================================================

final as (

    select

        *,

        (
            coalesce(is_valid_grid_voltage, false)
            and coalesce(is_valid_grid_current, false)
            and coalesce(is_valid_grid_load, false)
            and coalesce(is_valid_transformer_load, false)
            and coalesce(is_valid_grid_capacity, false)
            and coalesce(is_valid_demand_forecast, false)
            and coalesce(is_valid_line_loss_percent, false)
            and coalesce(is_valid_reserve_margin, false)
        ) as is_valid_record

    from percentage_validated

)

-- ============================================================
-- FINAL SILVER TABLE
-- ============================================================

select *
from final