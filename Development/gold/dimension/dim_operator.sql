
WITH operator_data AS (

    SELECT
        grid_operator
    FROM {{ ref('silver_grid_load_metrics') }}
    WHERE grid_operator IS NOT NULL

),

deduplicated AS (

    SELECT
        grid_operator,
        ROW_NUMBER() OVER (
            PARTITION BY grid_operator
            ORDER BY grid_operator
        ) AS rn
    FROM operator_data

)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY grid_operator
    ) AS operator_key,

    grid_operator AS operator_name

FROM deduplicated

WHERE rn = 1