# Databricks notebook source
# ============================================================
# ENERGY FORECAST PROJECT
# PYSPARK + PYTEST DATA QUALITY FRAMEWORK
# GOLD LAYER (adapted to actual project schema)
# ============================================================

import pytest
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

SCHEMA = "gold"

TBL_DIM_HOUSEHOLD  = f"{SCHEMA}.dim_household"
TBL_DIM_SUBSTATION = f"{SCHEMA}.dim_substation"
TBL_DIM_FEEDER     = f"{SCHEMA}.dim_feeder"
TBL_DIM_OPERATOR   = f"{SCHEMA}.dim_operator"
TBL_DIM_ZONE       = f"{SCHEMA}.dim_zone"
TBL_FACT_GRID_LOAD = f"{SCHEMA}.fact_grid_load"


# ============================================================
# ============================================================
# GOLD DIMENSION TESTS
# ============================================================
# ============================================================


# ------------------------------------------------------------
# 1. DIM HOUSEHOLD  (household_key, household_id, region_name,
#    city_name, meter_type, customer_category, grid_zone)
# ------------------------------------------------------------

def test_dim_household_key_not_null():
    df = spark.table(TBL_DIM_HOUSEHOLD)
    null_count = df.filter(F.col("household_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_key values"


def test_dim_household_key_unique():
    df = spark.table(TBL_DIM_HOUSEHOLD)
    total_count = df.count()
    distinct_count = df.select("household_key").distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate household_key values"
    )


def test_dim_household_id_not_null():
    df = spark.table(TBL_DIM_HOUSEHOLD)
    null_count = df.filter(F.col("household_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_id values"


def test_dim_household_no_duplicate_rows():
    df = spark.table(TBL_DIM_HOUSEHOLD)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ------------------------------------------------------------
# 2. DIM SUBSTATION  (substation_key, substation_name, grid_region)
# ------------------------------------------------------------

def test_dim_substation_key_not_null():
    df = spark.table(TBL_DIM_SUBSTATION)
    null_count = df.filter(F.col("substation_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL substation_key values"


def test_dim_substation_key_unique():
    df = spark.table(TBL_DIM_SUBSTATION)
    total_count = df.count()
    distinct_count = df.select("substation_key").distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate substation_key values"
    )


def test_dim_substation_name_not_null():
    df = spark.table(TBL_DIM_SUBSTATION)
    null_count = df.filter(F.col("substation_name").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL substation_name values"


def test_dim_substation_no_duplicate_rows():
    df = spark.table(TBL_DIM_SUBSTATION)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ------------------------------------------------------------
# 3. DIM FEEDER  (feeder_key, feeder_line, grid_region)
# ------------------------------------------------------------

def test_dim_feeder_key_not_null():
    df = spark.table(TBL_DIM_FEEDER)
    null_count = df.filter(F.col("feeder_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL feeder_key values"


def test_dim_feeder_key_unique():
    df = spark.table(TBL_DIM_FEEDER)
    total_count = df.count()
    distinct_count = df.select("feeder_key").distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate feeder_key values"
    )


def test_dim_feeder_name_not_null():
    df = spark.table(TBL_DIM_FEEDER)
    null_count = df.filter(F.col("feeder_line").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL feeder_line values"


def test_dim_feeder_no_duplicate_rows():
    df = spark.table(TBL_DIM_FEEDER)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ------------------------------------------------------------
# 4. DIM OPERATOR  (operator_key, operator_name)
# ------------------------------------------------------------

def test_dim_operator_key_not_null():
    df = spark.table(TBL_DIM_OPERATOR)
    null_count = df.filter(F.col("operator_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL operator_key values"


def test_dim_operator_key_unique():
    df = spark.table(TBL_DIM_OPERATOR)
    total_count = df.count()
    distinct_count = df.select("operator_key").distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate operator_key values"
    )


def test_dim_operator_name_not_null():
    df = spark.table(TBL_DIM_OPERATOR)
    null_count = df.filter(F.col("operator_name").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL operator_name values"


def test_dim_operator_no_duplicate_rows():
    df = spark.table(TBL_DIM_OPERATOR)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ------------------------------------------------------------
# 5. DIM ZONE  (zone_key, distribution_zone, grid_region)
# ------------------------------------------------------------

def test_dim_zone_key_not_null():
    df = spark.table(TBL_DIM_ZONE)
    null_count = df.filter(F.col("zone_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL zone_key values"


def test_dim_zone_key_unique():
    df = spark.table(TBL_DIM_ZONE)
    total_count = df.count()
    distinct_count = df.select("zone_key").distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate zone_key values"
    )


def test_dim_zone_not_null():
    df = spark.table(TBL_DIM_ZONE)
    null_count = df.filter(F.col("distribution_zone").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL distribution_zone values"


def test_dim_zone_no_duplicate_rows():
    df = spark.table(TBL_DIM_ZONE)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# ============================================================
# GOLD FACT TABLE TESTS  (gold.fact_grid_load)
# ============================================================
# ============================================================

# ------------------------------------------------------------
# 6. FACT GRID LOAD - REQUIRED SURROGATE KEYS NOT NULL
# ------------------------------------------------------------

def test_fact_household_key_not_null():
    df = spark.table(TBL_FACT_GRID_LOAD)
    null_count = df.filter(F.col("household_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL household_key values"


def test_fact_substation_key_not_null():
    df = spark.table(TBL_FACT_GRID_LOAD)
    null_count = df.filter(F.col("substation_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL substation_key values"


def test_fact_feeder_key_not_null():
    df = spark.table(TBL_FACT_GRID_LOAD)
    null_count = df.filter(F.col("feeder_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL feeder_key values"


def test_fact_operator_key_not_null():
    df = spark.table(TBL_FACT_GRID_LOAD)
    null_count = df.filter(F.col("operator_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL operator_key values"


def test_fact_zone_key_not_null():
    df = spark.table(TBL_FACT_GRID_LOAD)
    null_count = df.filter(F.col("zone_key").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL zone_key values"


# ------------------------------------------------------------
# 7. FACT DUPLICATE TEST
# ------------------------------------------------------------

def test_fact_no_duplicate_rows():
    df = spark.table(TBL_FACT_GRID_LOAD)
    total_count = df.count()
    distinct_count = df.distinct().count()
    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# ============================================================
# GOLD FACT -> DIMENSION RELATIONSHIP TESTS
# (surrogate-key joins: every key in the fact table must exist
#  in the corresponding dimension table)
# ============================================================
# ============================================================

# ------------------------------------------------------------
# 8. FACT -> HOUSEHOLD
# ------------------------------------------------------------

def test_fact_household_relationship():
    fact = spark.table(TBL_FACT_GRID_LOAD)
    household = spark.table(TBL_DIM_HOUSEHOLD)

    orphan_count = (
        fact.alias("f")
        .join(
            household.alias("h"),
            F.col("f.household_key") == F.col("h.household_key"),
            "left"
        )
        .filter(F.col("h.household_key").isNull())
        .count()
    )

    assert orphan_count == 0, (
        f"Found {orphan_count} fact records without a matching household_key "
        "in dim_household"
    )


# ------------------------------------------------------------
# 9. FACT -> SUBSTATION
# ------------------------------------------------------------

def test_fact_substation_relationship():
    fact = spark.table(TBL_FACT_GRID_LOAD)
    substation = spark.table(TBL_DIM_SUBSTATION)

    orphan_count = (
        fact.alias("f")
        .join(
            substation.alias("s"),
            F.col("f.substation_key") == F.col("s.substation_key"),
            "left"
        )
        .filter(F.col("s.substation_key").isNull())
        .count()
    )

    assert orphan_count == 0, (
        f"Found {orphan_count} fact records without a matching substation_key "
        "in dim_substation"
    )


# ------------------------------------------------------------
# 10. FACT -> FEEDER
# ------------------------------------------------------------

def test_fact_feeder_relationship():
    fact = spark.table(TBL_FACT_GRID_LOAD)
    feeder = spark.table(TBL_DIM_FEEDER)

    orphan_count = (
        fact.alias("f")
        .join(
            feeder.alias("d"),
            F.col("f.feeder_key") == F.col("d.feeder_key"),
            "left"
        )
        .filter(F.col("d.feeder_key").isNull())
        .count()
    )

    assert orphan_count == 0, (
        f"Found {orphan_count} fact records without a matching feeder_key "
        "in dim_feeder"
    )


# ------------------------------------------------------------
# 11. FACT -> OPERATOR
# ------------------------------------------------------------

def test_fact_operator_relationship():
    fact = spark.table(TBL_FACT_GRID_LOAD)
    operator = spark.table(TBL_DIM_OPERATOR)

    orphan_count = (
        fact.alias("f")
        .join(
            operator.alias("o"),
            F.col("f.operator_key") == F.col("o.operator_key"),
            "left"
        )
        .filter(F.col("o.operator_key").isNull())
        .count()
    )

    assert orphan_count == 0, (
        f"Found {orphan_count} fact records without a matching operator_key "
        "in dim_operator"
    )


# ------------------------------------------------------------
# 12. FACT -> ZONE
# ------------------------------------------------------------

def test_fact_zone_relationship():
    fact = spark.table(TBL_FACT_GRID_LOAD)
    zone = spark.table(TBL_DIM_ZONE)

    orphan_count = (
        fact.alias("f")
        .join(
            zone.alias("z"),
            F.col("f.zone_key") == F.col("z.zone_key"),
            "left"
        )
        .filter(F.col("z.zone_key").isNull())
        .count()
    )

    assert orphan_count == 0, (
        f"Found {orphan_count} fact records without a matching zone_key "
        "in dim_zone"
    )


# ============================================================
# ============================================================
# TEST EXECUTION
# ============================================================
# ============================================================

test_functions = [

    # DIM HOUSEHOLD
    test_dim_household_key_not_null,
    test_dim_household_key_unique,
    test_dim_household_id_not_null,
    test_dim_household_no_duplicate_rows,

    # DIM SUBSTATION
    test_dim_substation_key_not_null,
    test_dim_substation_key_unique,
    test_dim_substation_name_not_null,
    test_dim_substation_no_duplicate_rows,

    # DIM FEEDER
    test_dim_feeder_key_not_null,
    test_dim_feeder_key_unique,
    test_dim_feeder_name_not_null,
    test_dim_feeder_no_duplicate_rows,

    # DIM OPERATOR
    test_dim_operator_key_not_null,
    test_dim_operator_key_unique,
    test_dim_operator_name_not_null,
    test_dim_operator_no_duplicate_rows,

    # DIM ZONE
    test_dim_zone_key_not_null,
    test_dim_zone_key_unique,
    test_dim_zone_not_null,
    test_dim_zone_no_duplicate_rows,

    # FACT
    test_fact_household_key_not_null,
    test_fact_substation_key_not_null,
    test_fact_feeder_key_not_null,
    test_fact_operator_key_not_null,
    test_fact_zone_key_not_null,
    test_fact_no_duplicate_rows,

    # FACT -> DIMENSION RELATIONSHIPS
    test_fact_household_relationship,
    test_fact_substation_relationship,
    test_fact_feeder_relationship,
    test_fact_operator_relationship,
    test_fact_zone_relationship,
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
print("ENERGY FORECAST PROJECT - GOLD LAYER DATA QUALITY SUMMARY")
print("=" * 70)
print(f"Total Tests : {len(test_functions)}")
print(f"Passed      : {passed}")
print(f"Failed      : {failed}")
print("=" * 70)

if failed == 0:
    print("ALL GOLD LAYER DATA QUALITY TESTS PASSED ✅")
else:
    print("GOLD LAYER DATA QUALITY TESTS FAILED ❌")