/*
    SILVER LAYER - TRAFFIC/TARIFF METRICS

    Transformations:
    1. Remove duplicate tariff records
    2. Handle NULL / missing tariff values
    3. Trim and standardize categorical columns
    4. Convert rate and billing columns from string to numeric
    5. Validate tariff/rate values
*/

with source as (

    select *
    from {{ source('bronze', 'sink_traffic_metrics') }}

),

-- ============================================================
-- 1. REMOVE DUPLICATE TARIFF RECORDS
-- ============================================================

deduped as (

    select *
    from (

        select
            *,
            row_number() over (
                partition by
                    household_id,
                    tariff_region,
                    tariff_city,
                    tariff_plan_type,
                    billing_cycle,
                    utility_provider,
                    unit_rate,
                    peak_rate,
                    offpeak_rate,
                    fixed_charge,
                    tax_amount,
                    subsidy_amount,
                    monthly_bill,
                    billing_units,
                    late_fee,
                    adjustment_amount
                order by household_id
            ) as row_num

        from source

    ) ranked

    where row_num = 1

),

-- ============================================================
-- 2. HANDLE NULL / MISSING TARIFF VALUES
-- ============================================================

nulls_handled as (

    select

        household_id,

        nullif(trim(cast(tariff_region as string)), '')
            as tariff_region,

        nullif(trim(cast(tariff_city as string)), '')
            as tariff_city,

        nullif(trim(cast(tariff_plan_type as string)), '')
            as tariff_plan_type,

        nullif(trim(cast(billing_cycle as string)), '')
            as billing_cycle,

        nullif(trim(cast(utility_provider as string)), '')
            as utility_provider,

        nullif(trim(cast(unit_rate as string)), '')
            as unit_rate,

        nullif(trim(cast(peak_rate as string)), '')
            as peak_rate,

        nullif(trim(cast(offpeak_rate as string)), '')
            as offpeak_rate,

        nullif(trim(cast(fixed_charge as string)), '')
            as fixed_charge,

        nullif(trim(cast(tax_amount as string)), '')
            as tax_amount,

        nullif(trim(cast(subsidy_amount as string)), '')
            as subsidy_amount,

        nullif(trim(cast(monthly_bill as string)), '')
            as monthly_bill,

        nullif(trim(cast(billing_units as string)), '')
            as billing_units,

        nullif(trim(cast(late_fee as string)), '')
            as late_fee,

        nullif(trim(cast(adjustment_amount as string)), '')
            as adjustment_amount

    from deduped

),

-- ============================================================
-- 3. TRIM AND STANDARDIZE CATEGORICAL COLUMNS
-- ============================================================

standardized as (

    select

        household_id,

        initcap(lower(trim(tariff_region)))
            as tariff_region,

        initcap(lower(trim(tariff_city)))
            as tariff_city,

        initcap(lower(trim(tariff_plan_type)))
            as tariff_plan_type,

        case
            when lower(trim(billing_cycle)) = 'monthly'
                then 'Monthly'

            when lower(trim(billing_cycle)) = 'bi-monthly'
                then 'Bi-Monthly'

            else initcap(lower(trim(billing_cycle)))
        end as billing_cycle,

        trim(utility_provider)
            as utility_provider,

        unit_rate,
        peak_rate,
        offpeak_rate,
        fixed_charge,
        tax_amount,
        subsidy_amount,
        monthly_bill,
        billing_units,
        late_fee,
        adjustment_amount

    from nulls_handled

),

-- ============================================================
-- 4. CONVERT RATE AND BILLING COLUMNS TO NUMERIC
-- ============================================================

casted as (

    select

        household_id,
        tariff_region,
        tariff_city,
        tariff_plan_type,
        billing_cycle,
        utility_provider,

        try_cast(unit_rate as decimal(10,4))
            as unit_rate,

        try_cast(peak_rate as decimal(10,4))
            as peak_rate,

        try_cast(offpeak_rate as decimal(10,4))
            as offpeak_rate,

        try_cast(fixed_charge as decimal(10,4))
            as fixed_charge,

        try_cast(tax_amount as decimal(10,4))
            as tax_amount,

        try_cast(subsidy_amount as decimal(10,4))
            as subsidy_amount,

        try_cast(monthly_bill as decimal(12,4))
            as monthly_bill,

        try_cast(billing_units as decimal(12,4))
            as billing_units,

        try_cast(late_fee as decimal(10,4))
            as late_fee,

        try_cast(adjustment_amount as decimal(10,4))
            as adjustment_amount

    from standardized

),

-- ============================================================
-- 5. VALIDATE TARIFF / RATE VALUES
-- ============================================================

validated as (

    select

        *,

        -- Must be >= 0

        (
            unit_rate is not null
            and unit_rate >= 0
        ) as is_valid_unit_rate,

        (
            peak_rate is not null
            and peak_rate >= 0
        ) as is_valid_peak_rate,

        (
            offpeak_rate is not null
            and offpeak_rate >= 0
        ) as is_valid_offpeak_rate,

        (
            fixed_charge is not null
            and fixed_charge >= 0
        ) as is_valid_fixed_charge,

        (
            monthly_bill is not null
            and monthly_bill >= 0
        ) as is_valid_monthly_bill,

        (
            billing_units is not null
            and billing_units >= 0
        ) as is_valid_billing_units,

        (
            late_fee is not null
            and late_fee >= 0
        ) as is_valid_late_fee,

        -- Other numeric value

        (
            tax_amount is not null
            and tax_amount >= 0
        ) as is_valid_tax_amount,

        -- Categorical validation

        (
            tariff_region in (
                'North',
                'South',
                'East',
                'West'
            )
        ) as is_valid_region,

        (
            billing_cycle in (
                'Monthly',
                'Bi-Monthly'
            )
        ) as is_valid_billing_cycle

    from casted

),

-- ============================================================
-- FINAL RECORD VALIDATION
-- ============================================================

final as (

    select

        *,

        (
            coalesce(is_valid_unit_rate, false)
            and coalesce(is_valid_peak_rate, false)
            and coalesce(is_valid_offpeak_rate, false)
            and coalesce(is_valid_fixed_charge, false)
            and coalesce(is_valid_tax_amount, false)
            and coalesce(is_valid_monthly_bill, false)
            and coalesce(is_valid_billing_units, false)
            and coalesce(is_valid_late_fee, false)
            and coalesce(is_valid_region, false)
            and coalesce(is_valid_billing_cycle, false)
        ) as is_valid_record

    from validated

)

select *
from final




