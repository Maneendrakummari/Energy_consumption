/*
    SILVER LAYER - DEVICE METRICS

    Transformations:
    1. Remove duplicate records
    2. Handle NULL / missing device measurements
    3. Trim and standardize device attributes
    4. Convert device metrics to numeric types
    5. Validate device measurements
    6. Validate efficiency values
*/


-- ============================================================
-- SOURCE
-- ============================================================

with source as (

    select *
    from {{ source('bronze', 'sink_device_metrics') }}

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
                    device_category,
                    device_brand,
                    device_model,
                    maintenance_status,
                    installation_region,
                    runtime_hours,
                    device_power_kw,
                    motor_speed_rpm,
                    efficiency_ratio,
                    energy_draw_kwh,
                    heat_output,
                    cooling_load,
                    device_voltage,
                    device_current,
                    device_temperature
                order by household_id
            ) as row_num

        from source

    ) ranked

    where row_num = 1

),


-- ============================================================
-- 2. HANDLE NULL / MISSING DEVICE VALUES
-- ============================================================

nulls_handled as (

    select

        household_id,

        nullif(
            trim(cast(device_category as string)),
            ''
        ) as device_category,

        nullif(
            trim(cast(device_brand as string)),
            ''
        ) as device_brand,

        nullif(
            trim(cast(device_model as string)),
            ''
        ) as device_model,

        nullif(
            trim(cast(maintenance_status as string)),
            ''
        ) as maintenance_status,

        nullif(
            trim(cast(installation_region as string)),
            ''
        ) as installation_region,

        nullif(
            trim(cast(runtime_hours as string)),
            ''
        ) as runtime_hours,

        nullif(
            trim(cast(device_power_kw as string)),
            ''
        ) as device_power_kw,

        nullif(
            trim(cast(motor_speed_rpm as string)),
            ''
        ) as motor_speed_rpm,

        nullif(
            trim(cast(efficiency_ratio as string)),
            ''
        ) as efficiency_ratio,

        nullif(
            trim(cast(energy_draw_kwh as string)),
            ''
        ) as energy_draw_kwh,

        nullif(
            trim(cast(heat_output as string)),
            ''
        ) as heat_output,

        nullif(
            trim(cast(cooling_load as string)),
            ''
        ) as cooling_load,

        nullif(
            trim(cast(device_voltage as string)),
            ''
        ) as device_voltage,

        nullif(
            trim(cast(device_current as string)),
            ''
        ) as device_current,

        nullif(
            trim(cast(device_temperature as string)),
            ''
        ) as device_temperature

    from deduped

),


-- ============================================================
-- 3. TRIM AND STANDARDIZE DEVICE ATTRIBUTES
-- ============================================================

standardized as (

    select

        household_id,

        initcap(lower(trim(device_category)))
            as device_category,

        initcap(lower(trim(device_brand)))
            as device_brand,

        upper(trim(device_model))
            as device_model,

        initcap(lower(trim(maintenance_status)))
            as maintenance_status,

        initcap(lower(trim(installation_region)))
            as installation_region,

        runtime_hours,
        device_power_kw,
        motor_speed_rpm,
        efficiency_ratio,
        energy_draw_kwh,
        heat_output,
        cooling_load,
        device_voltage,
        device_current,
        device_temperature

    from nulls_handled

),


-- ============================================================
-- 4. CONVERT DEVICE METRICS TO NUMERIC TYPES
-- ============================================================

casted as (

    select

        household_id,

        device_category,
        device_brand,
        device_model,
        maintenance_status,
        installation_region,

        try_cast(
            runtime_hours as decimal(12,4)
        ) as runtime_hours,

        try_cast(
            device_power_kw as decimal(12,4)
        ) as device_power_kw,

        try_cast(
            motor_speed_rpm as decimal(14,4)
        ) as motor_speed_rpm,

        try_cast(
            efficiency_ratio as decimal(10,6)
        ) as efficiency_ratio,

        try_cast(
            energy_draw_kwh as decimal(14,4)
        ) as energy_draw_kwh,

        try_cast(
            heat_output as decimal(14,4)
        ) as heat_output,

        try_cast(
            cooling_load as decimal(14,4)
        ) as cooling_load,

        try_cast(
            device_voltage as decimal(12,4)
        ) as device_voltage,

        try_cast(
            device_current as decimal(12,4)
        ) as device_current,

        try_cast(
            device_temperature as decimal(12,4)
        ) as device_temperature

    from standardized

),


-- ============================================================
-- 5 & 6. VALIDATE DEVICE MEASUREMENTS
-- ============================================================

validated as (

    select

        *,

        -- Runtime must be >= 0
        (
            runtime_hours is not null
            and runtime_hours >= 0
        ) as is_valid_runtime_hours,

        -- Device power must be >= 0
        (
            device_power_kw is not null
            and device_power_kw >= 0
        ) as is_valid_device_power,

        -- Motor speed must be >= 0
        (
            motor_speed_rpm is not null
            and motor_speed_rpm >= 0
        ) as is_valid_motor_speed,

        -- Energy draw must be >= 0
        (
            energy_draw_kwh is not null
            and energy_draw_kwh >= 0
        ) as is_valid_energy_draw,

        -- Efficiency ratio must be between 0 and 1
        (
            efficiency_ratio is not null
            and efficiency_ratio >= 0
            and efficiency_ratio <= 1
        ) as is_valid_efficiency

    from casted

),


-- ============================================================
-- FINAL RECORD VALIDATION
-- ============================================================

final as (

    select

        *,

        (
            coalesce(is_valid_runtime_hours, false)
            and coalesce(is_valid_device_power, false)
            and coalesce(is_valid_motor_speed, false)
            and coalesce(is_valid_energy_draw, false)
            and coalesce(is_valid_efficiency, false)
        ) as is_valid_record

    from validated

)


-- ============================================================
-- FINAL SILVER TABLE
-- ============================================================

select *
from final





