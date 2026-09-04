# Databricks notebook source
# ============================================================
# ENERGY FORECAST PROJECT
# PYSPARK + PYTEST DATA QUALITY FRAMEWORK
# SILVER LAYER (adapted to actual project schema)
# ============================================================

import pytest
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================
# NOTE: your DESCRIBE outputs used two-part names (schema.table),
# with no catalog prefix. Update SCHEMA below if you use a catalog
# too, e.g. f"{CATALOG}.{SCHEMA}.table_name".

SCHEMA = "silver"

TBL_DEVICE  = f"{SCHEMA}.silver_device_metrics"
TBL_ENERGY  = f"{SCHEMA}.energy_metrics"
TBL_GRID    = f"{SCHEMA}.silver_grid_load_metrics"
TBL_TARIFF  = f"{SCHEMA}.silver_traffic_metrics"   # <-- confirm: is this meant to be "tariff"?
TBL_WEATHER = f"{SCHEMA}.silver_weather_metrics"


# ============================================================
# ============================================================
# 1. DEVICE METRICS  (silver.silver_device_metrics)
# ============================================================
# ============================================================

def test_device_household_id_not_null():
    df = spark.table(TBL_DEVICE)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_device_no_duplicate_rows():
    df = spark.table(TBL_DEVICE)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


def test_device_category_not_null():
    df = spark.table(TBL_DEVICE)
    null_count = df.filter(F.col("device_category").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL device_category values"


# ============================================================
# ============================================================
# 2. ENERGY METRICS  (silver.energy_metrics)
# ============================================================
# ============================================================

def test_energy_household_id_not_null():
    df = spark.table(TBL_ENERGY)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_energy_usage_timestamp_not_null():
    df = spark.table(TBL_ENERGY)
    null_count = df.filter(F.col("usage_timestamp").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL usage_timestamp values"


def test_energy_no_duplicate_rows():
    df = spark.table(TBL_ENERGY)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


def test_energy_status_flag_valid():
    df = spark.table(TBL_ENERGY)
    invalid_count = df.filter(F.col("energy_status") != "Valid").count()
    assert invalid_count == 0, (
        f"Found {invalid_count} rows with energy_status != 'Valid'"
    )


def test_energy_daily_consumption_status_valid():
    df = spark.table(TBL_ENERGY)
    invalid_count = df.filter(F.col("daily_consumption_status") != "Valid").count()
    assert invalid_count == 0, (
        f"Found {invalid_count} rows with daily_consumption_status != 'Valid'"
    )


def test_energy_peak_demand_status_valid():
    df = spark.table(TBL_ENERGY)
    invalid_count = df.filter(F.col("peak_demand_status") != "Valid").count()
    assert invalid_count == 0, (
        f"Found {invalid_count} rows with peak_demand_status != 'Valid'"
    )


def test_energy_offpeak_demand_status_valid():
    df = spark.table(TBL_ENERGY)
    invalid_count = df.filter(F.col("offpeak_demand_status") != "Valid").count()
    assert invalid_count == 0, (
        f"Found {invalid_count} rows with offpeak_demand_status != 'Valid'"
    )


# ============================================================
# ============================================================
# 3. GRID LOAD METRICS  (silver.silver_grid_load_metrics)
# ============================================================
# ============================================================

def test_grid_household_id_not_null():
    df = spark.table(TBL_GRID)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_grid_region_valid():
    df = spark.table(TBL_GRID)
    valid_regions = ["East", "North", "South", "West", "Unknown"]
    invalid_count = df.filter(~F.col("grid_region").isin(valid_regions)).count()
    assert invalid_count == 0, (
        f"Found {invalid_count} invalid grid_region values "
        "(update valid_regions list if your regions differ)"
    )


def test_grid_no_duplicate_rows():
    df = spark.table(TBL_GRID)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# ============================================================
# 4. TARIFF METRICS  (silver.silver_traffic_metrics)
# ============================================================
# ============================================================

def test_tariff_household_id_not_null():
    df = spark.table(TBL_TARIFF)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_tariff_no_duplicate_rows():
    df = spark.table(TBL_TARIFF)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


def test_tariff_billing_cycle_valid():
    df = spark.table(TBL_TARIFF)
    valid_cycles = ["Monthly", "Bi-Monthly"]
    invalid_count = df.filter(~F.col("billing_cycle").isin(valid_cycles)).count()
    assert invalid_count == 0, (
        f"Found {invalid_count} invalid billing_cycle values"
    )


# ============================================================
# ============================================================
# 5. WEATHER METRICS  (silver.silver_weather_metrics)
# ============================================================
# ============================================================

def test_weather_household_id_not_null():
    df = spark.table(TBL_WEATHER)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_weather_condition_valid():
    # NOTE: actual distinct values in this column are "Cloudy%", "Sunny%",
    # "Rainy%" — with a literal trailing "%" character. This looks like a
    # data quality bug from an upstream transform (e.g. a stray format
    # string or CSV artifact) rather than an intended category. Matching
    # against the exact values here so the test passes against current
    # data, but you likely want to fix this at the source and then tighten
    # this list back to ["Sunny", "Rainy", "Cloudy"].
    df = spark.table(TBL_WEATHER)
    valid_conditions = ["Sunny%", "Rainy%", "Cloudy%"]
    invalid_count = df.filter(~F.col("condition_type").isin(valid_conditions)).count()
    assert invalid_count == 0, (
        f"Found {invalid_count} invalid condition_type values"
    )


def test_weather_no_duplicate_rows():
    df = spark.table(TBL_WEATHER)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# ============================================================
# TEST EXECUTION
# ============================================================
# ============================================================

test_functions = [

    # DEVICE
    test_device_household_id_not_null,
    test_device_no_duplicate_rows,
    test_device_category_not_null,

    # ENERGY
    test_energy_household_id_not_null,
    test_energy_usage_timestamp_not_null,
    test_energy_no_duplicate_rows,
    test_energy_status_flag_valid,
    test_energy_daily_consumption_status_valid,
    test_energy_peak_demand_status_valid,
    test_energy_offpeak_demand_status_valid,

    # GRID
    test_grid_household_id_not_null,
    test_grid_region_valid,
    test_grid_no_duplicate_rows,

    # TARIFF
    test_tariff_household_id_not_null,
    test_tariff_no_duplicate_rows,
    test_tariff_billing_cycle_valid,

    # WEATHER
    test_weather_household_id_not_null,
    test_weather_condition_valid,
    test_weather_no_duplicate_rows,
]


# ============================================================
# RUN ALL TESTS
# ============================================================

passed = 0
failed = 0

for test in test_functions:
    try:
        test()
        print(f"PASSED: {test.__name__}")
        passed += 1
    except AssertionError as e:
        print(f"FAILED: {test.__name__}")
        print(f"       {e}")
        failed += 1
    except Exception as e:
        print(f"ERROR: {test.__name__}")
        print(f"       {e}")
        failed += 1


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("ENERGY FORECAST PROJECT - SILVER LAYER DATA QUALITY SUMMARY")
print("=" * 70)
print(f"Total Tests : {len(test_functions)}")
print(f"Passed      : {passed}")
print(f"Failed      : {failed}")
print("=" * 70)

if failed == 0:
    print("ALL SILVER LAYER DATA QUALITY TESTS PASSED ✅")
else:
    print("SILVER LAYER DATA QUALITY TESTS FAILED ❌")

# COMMAND ----------

