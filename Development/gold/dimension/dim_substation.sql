
WITH substation_data AS (

    SELECT
        substation_name,
        grid_region
    FROM {{ ref('silver_grid_load_metrics') }}
    WHERE substation_name IS NOT NULL

),

deduplicated AS (

    SELECT
        substation_name,
        grid_region,
        ROW_NUMBER() OVER (
            PARTITION BY substation_name
            ORDER BY substation_name
        ) AS rn
    FROM substation_data

)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY substation_name
    ) AS substation_key,

    substation_name,
    grid_region

FROM deduplicated

WHERE rn = 1