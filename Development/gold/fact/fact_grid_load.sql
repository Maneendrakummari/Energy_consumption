

SELECT

    -- Dimension Keys
    h.household_key,
    s.substation_key,
    f.feeder_key,
    o.operator_key,
    z.zone_key,

    -- Grid Measures
    g.grid_voltage,
    g.grid_current,
    g.grid_load_kw,
    g.transformer_load,
    g.line_loss_percent,
    g.load_variation,
    g.frequency_variation,
    g.grid_capacity_kw,
    g.demand_forecast_kw,
    g.reserve_margin

FROM {{ ref('silver_grid_load_metrics') }} g

LEFT JOIN {{ ref('dim_household') }} h
    ON g.household_id = h.household_id

LEFT JOIN {{ ref('dim_substation') }} s
    ON g.substation_name = s.substation_name

LEFT JOIN {{ ref('dim_feeder') }} f
    ON g.feeder_line = f.feeder_line

LEFT JOIN {{ ref('dim_operator') }} o
    ON g.grid_operator = o.operator_name

LEFT JOIN {{ ref('dim_zone') }} z
    ON g.distribution_zone = z.distribution_zone

WHERE g.is_valid_record = true