# Databricks notebook source
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