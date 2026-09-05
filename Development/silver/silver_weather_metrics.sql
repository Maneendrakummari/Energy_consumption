/*
    SILVER LAYER - WEATHER METRICS

    Transformations:
    1. Remove duplicate records
    2. Handle NULL / missing weather values
    3. Trim and standardize text columns
    4. Clean inconsistent weather-condition values
    5. Convert weather measurements to numeric types
    6. Validate weather measurement ranges
*/



-- ============================================================
-- SOURCE
-- ============================================================

with source as (

    select *
    from {{ source('bronze', 'sink_weather') }}

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
                    weather_region,
                    weather_city,
                    weather_station,
                    climate_zone,
                    condition_type,
                    temperature_celsius,
                    humidity_percent,
                    wind_speed_kmh,
                    rainfall_mm,
                    pressure_hpa,
                    solar_radiation,
                    dew_point,
                    uv_index,
                    visibility_km
                order by household_id
            ) as row_num

        from source

    ) ranked

    where row_num = 1

),

-- ============================================================
-- 2. HANDLE NULL / MISSING WEATHER VALUES
-- ============================================================

nulls_handled as (

    select

        household_id,

        nullif(
            trim(cast(weather_region as string)),
            ''
        ) as weather_region,

        nullif(
            trim(cast(weather_city as string)),
            ''
        ) as weather_city,

        nullif(
            trim(cast(weather_station as string)),
            ''
        ) as weather_station,

        nullif(
            trim(cast(climate_zone as string)),
            ''
        ) as climate_zone,

        nullif(
            trim(cast(condition_type as string)),
            ''
        ) as condition_type,

        nullif(
            trim(cast(temperature_celsius as string)),
            ''
        ) as temperature_celsius,

        nullif(
            trim(cast(humidity_percent as string)),
            ''
        ) as humidity_percent,

        nullif(
            trim(cast(wind_speed_kmh as string)),
            ''
        ) as wind_speed_kmh,

        nullif(
            trim(cast(rainfall_mm as string)),
            ''
        ) as rainfall_mm,

        nullif(
            trim(cast(pressure_hpa as string)),
            ''
        ) as pressure_hpa,

        nullif(
            trim(cast(solar_radiation as string)),
            ''
        ) as solar_radiation,

        nullif(
            trim(cast(dew_point as string)),
            ''
        ) as dew_point,

        nullif(
            trim(cast(uv_index as string)),
            ''
        ) as uv_index,

        nullif(
            trim(cast(visibility_km as string)),
            ''
        ) as visibility_km

    from deduped

),

-- ============================================================
-- 3. TRIM AND STANDARDIZE TEXT COLUMNS
-- ============================================================

standardized as (

    select

        household_id,

        initcap(lower(trim(weather_region)))
            as weather_region,

        initcap(lower(trim(weather_city)))
            as weather_city,

        initcap(lower(trim(weather_station)))
            as weather_station,

        initcap(lower(trim(climate_zone)))
            as climate_zone,

        trim(condition_type)
            as condition_type,

        temperature_celsius,
        humidity_percent,
        wind_speed_kmh,
        rainfall_mm,
        pressure_hpa,
        solar_radiation,
        dew_point,
        uv_index,
        visibility_km

    from nulls_handled

),

-- ============================================================
-- 4. CLEAN INCONSISTENT WEATHER CONDITION VALUES
-- ============================================================

condition_cleaned as (

    select

        household_id,
        weather_region,
        weather_city,
        weather_station,
        climate_zone,

        case

            when lower(trim(condition_type))
                in ('clear', 'sunny')
                then 'Clear'

            when lower(trim(condition_type))
                in (
                    'partly cloudy',
                    'partly_cloudy',
                    'partly-cloudy'
                )
                then 'Partly Cloudy'

            when lower(trim(condition_type))
                in ('cloudy', 'overcast')
                then 'Cloudy'

            when lower(trim(condition_type))
                in (
                    'rain',
                    'rainy',
                    'light rain',
                    'light_rain'
                )
                then 'Rain'

            when lower(trim(condition_type))
                in (
                    'heavy rain',
                    'heavy_rain'
                )
                then 'Heavy Rain'

            when lower(trim(condition_type))
                in (
                    'storm',
                    'thunderstorm',
                    'thunder storm'
                )
                then 'Storm'

            when lower(trim(condition_type))
                in (
                    'snow',
                    'snowy',
                    'light snow'
                )
                then 'Snow'

            when lower(trim(condition_type))
                in (
                    'fog',
                    'foggy',
                    'mist',
                    'misty'
                )
                then 'Fog'

            when lower(trim(condition_type))
                in (
                    'wind',
                    'windy'
                )
                then 'Windy'

            else initcap(trim(condition_type))

        end as condition_type,

        temperature_celsius,
        humidity_percent,
        wind_speed_kmh,
        rainfall_mm,
        pressure_hpa,
        solar_radiation,
        dew_point,
        uv_index,
        visibility_km

    from standardized

),

-- ============================================================
-- 5. CONVERT WEATHER MEASUREMENTS TO NUMERIC TYPES
-- ============================================================

casted as (

    select

        household_id,
        weather_region,
        weather_city,
        weather_station,
        climate_zone,
        condition_type,

        try_cast(
            temperature_celsius as decimal(10,4)
        ) as temperature_celsius,

        try_cast(
            humidity_percent as decimal(10,4)
        ) as humidity_percent,

        try_cast(
            wind_speed_kmh as decimal(10,4)
        ) as wind_speed_kmh,

        try_cast(
            rainfall_mm as decimal(12,4)
        ) as rainfall_mm,

        try_cast(
            pressure_hpa as decimal(12,4)
        ) as pressure_hpa,

        try_cast(
            solar_radiation as decimal(14,4)
        ) as solar_radiation,

        try_cast(
            dew_point as decimal(10,4)
        ) as dew_point,

        try_cast(
            uv_index as decimal(10,4)
        ) as uv_index,

        try_cast(
            visibility_km as decimal(10,4)
        ) as visibility_km

    from condition_cleaned

),

-- ============================================================
-- 6. VALIDATE WEATHER MEASUREMENT RANGES
-- ============================================================

validated as (

    select

        *,

        -- Humidity must be between 0 and 100

        (
            humidity_percent is not null
            and humidity_percent between 0 and 100
        ) as is_valid_humidity,

        -- Wind speed must be >= 0

        (
            wind_speed_kmh is not null
            and wind_speed_kmh >= 0
        ) as is_valid_wind_speed,

        -- Rainfall must be >= 0

        (
            rainfall_mm is not null
            and rainfall_mm >= 0
        ) as is_valid_rainfall,

        -- UV index must be >= 0

        (
            uv_index is not null
            and uv_index >= 0
        ) as is_valid_uv_index,

        -- Visibility must be >= 0

        (
            visibility_km is not null
            and visibility_km >= 0
        ) as is_valid_visibility,

        -- Solar radiation must be >= 0

        (
            solar_radiation is not null
            and solar_radiation >= 0
        ) as is_valid_solar_radiation

    from casted

),

-- ============================================================
-- 7. FINAL RECORD VALIDATION
-- ============================================================

final as (

    select

        *,

        (
            coalesce(is_valid_humidity, false)
            and coalesce(is_valid_wind_speed, false)
            and coalesce(is_valid_rainfall, false)
            and coalesce(is_valid_uv_index, false)
            and coalesce(is_valid_visibility, false)
            and coalesce(is_valid_solar_radiation, false)
        ) as is_valid_record

    from validated

)

-- ============================================================
-- FINAL SILVER TABLE
-- ============================================================

select *
from final