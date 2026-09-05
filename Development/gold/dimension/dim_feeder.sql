
WITH feeder_data AS (

    SELECT
        feeder_line,
        grid_region
    FROM {{ ref('silver_grid_load_metrics') }}
    WHERE feeder_line IS NOT NULL

),

deduplicated AS (

    SELECT
        feeder_line,
        grid_region,
        ROW_NUMBER() OVER (
            PARTITION BY feeder_line
            ORDER BY feeder_line
        ) AS rn
    FROM feeder_data

)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY feeder_line
    ) AS feeder_key,

    feeder_line,
    grid_region

FROM deduplicated

WHERE rn = 1