# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Fact Table — Taxi Star Schema
# MAGIC %md
# MAGIC # Fact Table — Taxi Star Schema
# MAGIC
# MAGIC Builds `fact_journey` — one row per taxi booking — by reading from `gold_taxi_data` and looking up surrogate keys from all seven dimension tables.
# MAGIC
# MAGIC | FK | Dimension | Join key(s) |
# MAGIC |---|---|---|
# MAGIC | `*_date_sk` / `*_time_sk` (×5 each) | `dim_date` / `dim_time` | date part / hour + minute |
# MAGIC | `driver_sk` | `dim_driver` | `driver` = `driver_number` |
# MAGIC | `payment_type_sk` | `dim_payment_type` | `payment_type` = `payment_type_name` |
# MAGIC | `booking_source_sk` | `dim_booking_source` | `booking_source` + `booked_by` (null-safe) |
# MAGIC | `pickup_location_sk` | `dim_location` | `pickup_zone` = `zone_name` |
# MAGIC | `destination_location_sk` | `dim_location` | `destination_zone` = `zone_name` |
# MAGIC | `capability_sk` | `dim_capability` | all 13 boolean `has_*` columns |

# COMMAND ----------

# DBTITLE 1,Imports and constants
import dlt
from pyspark.sql import functions as F
from functools import reduce

# Capability columns — must match dim_capability in 04a
CAP_COLS = [
    "has_card_reader", "has_delivery", "has_high_car", "has_low_car",
    "has_wheelchair", "has_minibus", "has_female_driver", "has_vip",
    "has_tour", "has_pet", "has_six_seater", "has_seven_seater",
    "has_eight_seater",
]

# Timestamp columns → (source column in gold, prefix for SK columns in fact)
TIMESTAMP_ROLES = [
    ("pickup_due",            "pickup_due"),
    ("completed",             "completed"),
    ("time_dispatched",       "dispatched"),
    ("time_vehicle_arrived",  "arrived"),
    ("time_picked_up",        "picked_up"),
]

# COMMAND ----------

# DBTITLE 1,fact_journey — one row per taxi journey
@dlt.table(
    name="fact_journey",
    comment="Fact table — one row per taxi journey (booking_id) with surrogate keys to all dimensions",
)
def fact_journey():
    # ── Source and dimension tables ──
    gold    = dlt.read("gold_taxi_data")
    d_date  = dlt.read("dim_date")
    d_time  = dlt.read("dim_time")
    d_drv   = dlt.read("dim_driver")
    d_pay   = dlt.read("dim_payment_type")
    d_src   = dlt.read("dim_booking_source")
    d_loc   = dlt.read("dim_location")
    d_cap   = dlt.read("dim_capability")

    fact = gold

    # ── Date & time surrogate-key lookups (5 timestamps × 2 dims) ──
    for src_col, prefix in TIMESTAMP_ROLES:
        # Extract date and time components
        fact = (
            fact
            .withColumn(f"_{prefix}_dt", F.to_date(F.col(src_col)))
            .withColumn(f"_{prefix}_hr", F.hour(F.col(src_col)))
            .withColumn(f"_{prefix}_mn", F.minute(F.col(src_col)))
        )

        # Join → dim_date
        dd = d_date.select(
            F.col("date_sk").alias(f"{prefix}_date_sk"),
            F.col("full_date").alias(f"_{prefix}_fd"),
        )
        fact = fact.join(dd, F.col(f"_{prefix}_dt") == F.col(f"_{prefix}_fd"), "left")
        fact = fact.drop(f"_{prefix}_dt", f"_{prefix}_fd")

        # Join → dim_time
        dt = d_time.select(
            F.col("time_sk").alias(f"{prefix}_time_sk"),
            F.col("hour").alias(f"_{prefix}_hr2"),
            F.col("minute").alias(f"_{prefix}_mn2"),
        )
        fact = fact.join(
            dt,
            (F.col(f"_{prefix}_hr") == F.col(f"_{prefix}_hr2"))
            & (F.col(f"_{prefix}_mn") == F.col(f"_{prefix}_mn2")),
            "left",
        )
        fact = fact.drop(
            f"_{prefix}_hr", f"_{prefix}_mn",
            f"_{prefix}_hr2", f"_{prefix}_mn2",
        )

    # ── Driver ──
    drv = d_drv.select(
        F.col("driver_sk"),
        F.col("driver_number").alias("_drv_num"),
    )
    fact = fact.join(drv, F.col("driver") == F.col("_drv_num"), "left").drop("_drv_num")

    # ── Payment type ──
    pay = d_pay.select(
        F.col("payment_type_sk"),
        F.col("payment_type_name").alias("_pay_nm"),
    )
    fact = fact.join(pay, F.col("payment_type") == F.col("_pay_nm"), "left").drop("_pay_nm")

    # ── Booking source (null-safe join on optional booked_by) ──
    fact = fact.withColumn(
        "_booked_by_nm",
        F.when(F.col("booked_by") != F.col("booking_source"), F.col("booked_by")),
    )
    src = d_src.select(
        F.col("booking_source_sk"),
        F.col("booking_source_name").alias("_src_nm"),
        F.col("booked_by_name").alias("_src_bb"),
    )
    fact = fact.join(
        src,
        (F.col("booking_source") == F.col("_src_nm"))
        & F.col("_booked_by_nm").eqNullSafe(F.col("_src_bb")),
        "left",
    ).drop("_src_nm", "_src_bb", "_booked_by_nm")

    # ── Location (pickup) ──
    loc_p = d_loc.select(
        F.col("location_sk").alias("pickup_location_sk"),
        F.col("zone_name").alias("_pz"),
    )
    fact = fact.join(loc_p, F.col("pickup_zone") == F.col("_pz"), "left").drop("_pz")

    # ── Location (destination) ──
    loc_d = d_loc.select(
        F.col("location_sk").alias("destination_location_sk"),
        F.col("zone_name").alias("_dz"),
    )
    fact = fact.join(loc_d, F.col("destination_zone") == F.col("_dz"), "left").drop("_dz")

    # ── Capability (join on all 13 boolean flags) ──
    cap = d_cap.select(
        F.col("capability_sk"),
        *[F.col(c).alias(f"_c_{c}") for c in CAP_COLS],
    )
    cap_cond = reduce(
        lambda a, b: a & b,
        [F.col(c).eqNullSafe(F.col(f"_c_{c}")) for c in CAP_COLS],
    )
    fact = fact.join(cap, cap_cond, "left")
    for c in CAP_COLS:
        fact = fact.drop(f"_c_{c}")

    # ── Final projection (star-schema columns only) ──
    return fact.select(
        # Degenerate dimensions
        "booking_id",
        "trip_status",
        "vehicle",
        "priority",

        # Date/time FKs
        "pickup_due_date_sk",   "pickup_due_time_sk",
        "completed_date_sk",    "completed_time_sk",
        "dispatched_date_sk",   "dispatched_time_sk",
        "arrived_date_sk",      "arrived_time_sk",
        "picked_up_date_sk",    "picked_up_time_sk",

        # Dimension FKs
        "driver_sk",
        "payment_type_sk",
        "booking_source_sk",
        "pickup_location_sk",
        "destination_location_sk",
        "capability_sk",

        # Measures
        "price",
        "distance",
        "wait_time_minutes",
        "trip_duration_minutes",
        "total_time_taken",
        "price_per_mile",

        # Coordinates (trip-level precision)
        "pickup_latitude",
        "pickup_longitude",
        "destination_latitude",
        "destination_longitude",

        # Validity flags
        "is_valid_distance",
        "is_valid_timing",
        "is_valid_duration",
        "is_price_outlier",
    )

# COMMAND ----------

# DBTITLE 1,Validation Checks
# MAGIC %md
# MAGIC ## Validation Checks
# MAGIC Post-pipeline checks — run against the published tables to verify grain, joins, and referential integrity.

# COMMAND ----------

# DBTITLE 1,Validation — Row count vs gold + booking_id uniqueness
# MAGIC %sql
# MAGIC -- fact_journey row count should match gold_taxi_data; booking_id must be unique in both
# MAGIC SELECT
# MAGIC   'fact_journey' AS table_name,
# MAGIC   COUNT(*)       AS total_rows,
# MAGIC   COUNT(DISTINCT booking_id) AS distinct_booking_ids,
# MAGIC   COUNT(*) - COUNT(DISTINCT booking_id) AS duplicates
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'gold_taxi_data',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT booking_id),
# MAGIC   COUNT(*) - COUNT(DISTINCT booking_id)
# MAGIC FROM `students_data`.`team-1-data-schema`.gold_taxi_data

# COMMAND ----------

# DBTITLE 1,Validation — FK referential integrity (orphan check)
# MAGIC %sql
# MAGIC -- Every non-null FK must match a real dimension row (orphan_rows should be 0 for all)
# MAGIC SELECT 'driver_sk' AS fk_column, COUNT(*) AS orphan_rows
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE driver_sk IS NOT NULL
# MAGIC   AND driver_sk NOT IN (SELECT driver_sk FROM `students_data`.`team-1-data-schema`.dim_driver)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'payment_type_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE payment_type_sk IS NOT NULL
# MAGIC   AND payment_type_sk NOT IN (SELECT payment_type_sk FROM `students_data`.`team-1-data-schema`.dim_payment_type)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'booking_source_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE booking_source_sk IS NOT NULL
# MAGIC   AND booking_source_sk NOT IN (SELECT booking_source_sk FROM `students_data`.`team-1-data-schema`.dim_booking_source)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'pickup_location_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE pickup_location_sk IS NOT NULL
# MAGIC   AND pickup_location_sk NOT IN (SELECT location_sk FROM `students_data`.`team-1-data-schema`.dim_location)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'destination_location_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE destination_location_sk IS NOT NULL
# MAGIC   AND destination_location_sk NOT IN (SELECT location_sk FROM `students_data`.`team-1-data-schema`.dim_location)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'capability_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE capability_sk IS NOT NULL
# MAGIC   AND capability_sk NOT IN (SELECT capability_sk FROM `students_data`.`team-1-data-schema`.dim_capability)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'pickup_due_date_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE pickup_due_date_sk IS NOT NULL
# MAGIC   AND pickup_due_date_sk NOT IN (SELECT date_sk FROM `students_data`.`team-1-data-schema`.dim_date)
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'pickup_due_time_sk', COUNT(*)
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey
# MAGIC WHERE pickup_due_time_sk IS NOT NULL
# MAGIC   AND pickup_due_time_sk NOT IN (SELECT time_sk FROM `students_data`.`team-1-data-schema`.dim_time)

# COMMAND ----------

# DBTITLE 1,Validation — NULL FK summary (join success rate)
# MAGIC %sql
# MAGIC -- Percentage of NULL FKs per column — high values may indicate join failures
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN driver_sk              IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_driver,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN payment_type_sk         IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_payment_type,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN booking_source_sk        IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_booking_source,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN pickup_location_sk       IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_pickup_loc,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN destination_location_sk  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_dest_loc,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN capability_sk            IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_capability,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN pickup_due_date_sk       IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_pickup_due_date,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN completed_date_sk        IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_completed_date
# MAGIC FROM `students_data`.`team-1-data-schema`.fact_journey