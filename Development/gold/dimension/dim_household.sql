

WITH household_data AS (

    SELECT
        household_id,
        region_name,
        city_name,
        meter_type,
        customer_category,
        grid_zone,

        ROW_NUMBER() OVER (
            PARTITION BY household_id
            ORDER BY household_id
        ) AS rn

    FROM {{ ref('energy_metrics') }}

    WHERE household_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY household_id
    ) AS household_key,

    household_id,
    region_name,
    city_name,
    meter_type,
    customer_category,
    grid_zone

FROM household_data

WHERE rn = 1