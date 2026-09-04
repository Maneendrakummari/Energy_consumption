# Databricks notebook source
# ============================================================
# ENERGY FORECAST PROJECT
# PYSPARK + PYTEST DATA QUALITY FRAMEWORK
# BRONZE LAYER (raw ingestion checks)
# ============================================================
#
# NOTE ON PHILOSOPHY:
# Bronze is raw landing data. These tests intentionally do NOT check
# business rules, valid-value categories, or hard-fail on duplicates --
# that enforcement belongs to the silver layer, where you already have
# is_valid_* flags and *_status columns doing that job.
#
# Bronze tests here only confirm that ingestion itself worked:
#   - the table exists and isn't empty
#   - the core identifying key isn't null
#   - row counts are logged for visibility (not hard-failed on)
#
# If your bronze tables also carry is_valid_* / *_status columns
# (as you mentioned), we deliberately do NOT assert on them here --
# that would just duplicate the silver-layer checks you already have.
# ============================================================

import pytest
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

SCHEMA = "bronze"

TBL_DEVICE  = f"{SCHEMA}.sink_device_metrics"
TBL_ENERGY  = f"{SCHEMA}.energy_metrics"
TBL_GRID    = f"{SCHEMA}.sink_grid_load"
TBL_TARIFF  = f"{SCHEMA}.sink_traffic_metrics"
TBL_WEATHER = f"{SCHEMA}.sink_weather"

ALL_BRONZE_TABLES = {
    "device":  TBL_DEVICE,
    "energy":  TBL_ENERGY,
    "grid":    TBL_GRID,
    "tariff":  TBL_TARIFF,
    "weather": TBL_WEATHER,
}


# ============================================================
# ============================================================
# 1. TABLE EXISTS / NOT EMPTY
# ============================================================
# ============================================================

def test_device_table_not_empty():
    df = spark.table(TBL_DEVICE)
    row_count = df.count()
    assert row_count > 0, f"{TBL_DEVICE} has 0 rows -- ingestion may have failed"


def test_energy_table_not_empty():
    df = spark.table(TBL_ENERGY)
    row_count = df.count()
    assert row_count > 0, f"{TBL_ENERGY} has 0 rows -- ingestion may have failed"


def test_grid_table_not_empty():
    df = spark.table(TBL_GRID)
    row_count = df.count()
    assert row_count > 0, f"{TBL_GRID} has 0 rows -- ingestion may have failed"


def test_tariff_table_not_empty():
    df = spark.table(TBL_TARIFF)
    row_count = df.count()
    assert row_count > 0, f"{TBL_TARIFF} has 0 rows -- ingestion may have failed"


def test_weather_table_not_empty():
    df = spark.table(TBL_WEATHER)
    row_count = df.count()
    assert row_count > 0, f"{TBL_WEATHER} has 0 rows -- ingestion may have failed"


# ============================================================
# ============================================================
# 2. CORE KEY NOT NULL
# (household_id is the only column we know for certain is meant
#  to always be populated straight from source, across all 5 tables)
# ============================================================
# ============================================================

def test_device_household_id_not_null():
    df = spark.table(TBL_DEVICE)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_grid_household_id_not_null():
    df = spark.table(TBL_GRID)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_tariff_household_id_not_null():
    df = spark.table(TBL_TARIFF)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_weather_household_id_not_null():
    df = spark.table(TBL_WEATHER)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


# ============================================================
# ============================================================
# 3. ROW COUNT VISIBILITY (informational, never fails)
# Prints row counts for each bronze table so you can eyeball
# whether an ingestion run looks abnormally small/large.
# ============================================================
# ============================================================

def print_bronze_row_counts():
    print("\n" + "-" * 70)
    print("BRONZE LAYER ROW COUNTS (informational only)")
    print("-" * 70)
    for name, table in ALL_BRONZE_TABLES.items():
        count = spark.table(table).count()
        print(f"{name:10s} ({table}): {count} rows")
    print("-" * 70)


# ============================================================
# ============================================================
# TEST EXECUTION
# ============================================================
# ============================================================

test_functions = [

    # NOT EMPTY
    test_device_table_not_empty,
    test_energy_table_not_empty,
    test_grid_table_not_empty,
    test_tariff_table_not_empty,
    test_weather_table_not_empty,

    # HOUSEHOLD_ID NOT NULL
    test_device_household_id_not_null,
    test_grid_household_id_not_null,
    test_tariff_household_id_not_null,
    test_weather_household_id_not_null,
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

print_bronze_row_counts()


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("ENERGY FORECAST PROJECT - BRONZE LAYER DATA QUALITY SUMMARY")
print("=" * 70)
print(f"Total Tests : {len(test_functions)}")
print(f"Passed      : {passed}")
print(f"Failed      : {failed}")
print("=" * 70)

if failed == 0:
    print("ALL BRONZE LAYER DATA QUALITY TESTS PASSED ✅")
else:
    print("BRONZE LAYER DATA QUALITY TESTS FAILED ❌")
