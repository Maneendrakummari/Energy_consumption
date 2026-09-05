
WITH zone_data AS (

    SELECT
        distribution_zone,
        grid_region
    FROM {{ ref('silver_grid_load_metrics') }}
    WHERE distribution_zone IS NOT NULL

),

deduplicated AS (

    SELECT
        distribution_zone,
        grid_region,
        ROW_NUMBER() OVER (
            PARTITION BY distribution_zone
            ORDER BY distribution_zone
        ) AS rn
    FROM zone_data

)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY distribution_zone
    ) AS zone_key,

    distribution_zone,
    grid_region

FROM deduplicated

WHERE rn = 1