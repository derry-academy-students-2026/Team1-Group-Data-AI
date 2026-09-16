# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dimension Tables — Taxi Star Schema
# MAGIC %md
# MAGIC # Dimension Tables — Taxi Star Schema
# MAGIC
# MAGIC Seven dimension tables for the Kimball star schema, reading from `gold_taxi_data`.
# MAGIC
# MAGIC | Dimension | Grain | Source |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | One row per calendar date (century from earliest record) | All timestamp columns |
# MAGIC | `dim_time` | One row per minute of day (1,440 rows) | Pre-populated |
# MAGIC | `dim_payment_type` | One row per payment method | `payment_type` |
# MAGIC | `dim_driver` | One row per driver | `driver` |
# MAGIC | `dim_booking_source` | One row per source + actor combination | `booking_source`, `booked_by` |
# MAGIC | `dim_location` | One row per zone | `pickup_zone`, `destination_zone` |
# MAGIC | `dim_capability` | One row per unique flag combination | 13 boolean `has_*` columns |

# COMMAND ----------

# DBTITLE 1,Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# COMMAND ----------

# DBTITLE 1,dim_date — century of dates from earliest record
@dlt.table(
    name="dim_date",
    comment="Date dimension — 100 years of calendar dates starting from the earliest record",
)
def dim_date():
    gold = dlt.read("gold_taxi_data")

    timestamp_cols = [
        "pickup_due", "completed", "time_dispatched",
        "time_vehicle_arrived", "time_picked_up",
    ]

    # Find the earliest valid date across all timestamp columns
    agg_exprs = [
        F.min(
            F.when(
                F.year(F.to_date(F.col(c))).between(2020, 2130),
                F.to_date(F.col(c)),
            )
        ).alias(f"min_{c}")
        for c in timestamp_cols
    ]

    bounds = gold.agg(*agg_exprs).collect()[0]
    min_vals = [bounds[f"min_{c}"] for c in timestamp_cols if bounds[f"min_{c}"] is not None]
    min_date = min(min_vals)

    # Generate a continuous century of dates (≈36,525 rows)
    all_dates = spark.sql(
        f"SELECT explode(sequence(DATE '{min_date}', DATE '{min_date}' + INTERVAL 100 YEARS - INTERVAL 1 DAY, INTERVAL 1 DAY)) AS full_date"
    )

    w = Window.orderBy("full_date")

    return all_dates.select(
        F.row_number().over(w).cast("int").alias("date_sk"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofyear("full_date").alias("day_of_year"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )

# COMMAND ----------

# DBTITLE 1,dim_time — one row per minute of the day
@dlt.table(
    name="dim_time",
    comment="Time dimension — one row per minute of the day (1,440 rows)",
)
def dim_time():
    minutes = spark.range(0, 1440)
    hour_expr = F.floor(F.col("id") / 60).cast("int")
    minute_expr = (F.col("id") % 60).cast("int")

    return minutes.select(
        (F.col("id") + 1).cast("int").alias("time_sk"),
        hour_expr.alias("hour"),
        minute_expr.alias("minute"),
        F.concat(
            F.lpad(hour_expr.cast("string"), 2, "0"),
            F.lit(":"),
            F.lpad(minute_expr.cast("string"), 2, "0"),
        ).alias("time_24h"),
        F.when(hour_expr < 6, "Night")
         .when(hour_expr < 12, "Morning")
         .when(hour_expr < 18, "Afternoon")
         .otherwise("Evening")
         .alias("time_of_day"),
        (
            ((hour_expr >= 7) & (hour_expr < 9))
            | ((hour_expr >= 16) & (hour_expr < 18))
        ).alias("is_peak_hour"),
    )

# COMMAND ----------

# DBTITLE 1,dim_payment_type — Cash, Card, Account
@dlt.table(
    name="dim_payment_type",
    comment="Payment type dimension — one row per payment method",
)
def dim_payment_type():
    gold = dlt.read("gold_taxi_data")
    types = gold.select("payment_type").distinct().filter(F.col("payment_type").isNotNull())
    w = Window.orderBy("payment_type")

    return types.select(
        F.row_number().over(w).cast("int").alias("payment_type_sk"),
        F.col("payment_type").alias("payment_type_name"),
    )

# COMMAND ----------

# DBTITLE 1,dim_driver — one row per driver
@dlt.table(
    name="dim_driver",
    comment="Driver dimension — one row per driver number",
)
def dim_driver():
    gold = dlt.read("gold_taxi_data")
    drivers = gold.select("driver").distinct().filter(F.col("driver").isNotNull())
    w = Window.orderBy("driver")

    return drivers.select(
        F.row_number().over(w).cast("int").alias("driver_sk"),
        F.col("driver").alias("driver_number"),
    )

# COMMAND ----------

# DBTITLE 1,dim_booking_source — channel with optional dispatcher
@dlt.table(
    name="dim_booking_source",
    comment="Booking source dimension — channel with optional actor (dispatcher)",
)
def dim_booking_source():
    gold = dlt.read("gold_taxi_data")

    sources = gold.select(
        F.col("booking_source").alias("booking_source_name"),
        F.when(
            F.col("booked_by") != F.col("booking_source"),
            F.col("booked_by"),
        ).alias("booked_by_name"),
    ).distinct()

    w = Window.orderBy("booking_source_name", F.col("booked_by_name").asc_nulls_last())

    return sources.select(
        F.row_number().over(w).cast("int").alias("booking_source_sk"),
        "booking_source_name",
        "booked_by_name",
    )

# COMMAND ----------

# DBTITLE 1,dim_location — one row per zone (role-playing)
@dlt.table(
    name="dim_location",
    comment="Location dimension — one row per zone (shared by pickup and destination)",
)
def dim_location():
    gold = dlt.read("gold_taxi_data")

    # Union pickup and destination zones into a single dimension
    zones = (
        gold.select(F.col("pickup_zone").alias("zone_name"))
        .union(gold.select(F.col("destination_zone").alias("zone_name")))
        .distinct()
        .filter(F.col("zone_name").isNotNull())
    )

    w = Window.orderBy("zone_name")

    return zones.select(
        F.row_number().over(w).cast("int").alias("location_sk"),
        "zone_name",
    )

# COMMAND ----------

# DBTITLE 1,dim_capability — one row per unique flag combination
CAP_COLS = [
    "has_card_reader", "has_delivery", "has_high_car", "has_low_car",
    "has_wheelchair", "has_minibus", "has_female_driver", "has_vip",
    "has_tour", "has_pet", "has_six_seater", "has_seven_seater",
    "has_eight_seater",
]


@dlt.table(
    name="dim_capability",
    comment="Capability dimension — one row per unique combination of vehicle capability flags",
)
def dim_capability():
    gold = dlt.read("gold_taxi_data")
    caps = gold.select(CAP_COLS).distinct()
    w = Window.orderBy(CAP_COLS)

    return caps.select(
        F.row_number().over(w).cast("int").alias("capability_sk"),
        *CAP_COLS,
    )

# COMMAND ----------

# DBTITLE 1,Validation Checks
# MAGIC %md
# MAGIC ## Validation Checks
# MAGIC Post-pipeline checks — run against the published tables to verify grain, completeness, and content.

# COMMAND ----------

# DBTITLE 1,Validation — Dimension PK uniqueness
# MAGIC %sql
# MAGIC -- Every dimension PK must be unique (duplicates should be 0 for all rows)
# MAGIC SELECT 'dim_date' AS dimension, COUNT(*) AS total_rows, COUNT(DISTINCT date_sk) AS distinct_pks, COUNT(*) - COUNT(DISTINCT date_sk) AS duplicates FROM `students_data`.`team-1-data-schema`.dim_date
# MAGIC UNION ALL
# MAGIC SELECT 'dim_time', COUNT(*), COUNT(DISTINCT time_sk), COUNT(*) - COUNT(DISTINCT time_sk) FROM `students_data`.`team-1-data-schema`.dim_time
# MAGIC UNION ALL
# MAGIC SELECT 'dim_payment_type', COUNT(*), COUNT(DISTINCT payment_type_sk), COUNT(*) - COUNT(DISTINCT payment_type_sk) FROM `students_data`.`team-1-data-schema`.dim_payment_type
# MAGIC UNION ALL
# MAGIC SELECT 'dim_driver', COUNT(*), COUNT(DISTINCT driver_sk), COUNT(*) - COUNT(DISTINCT driver_sk) FROM `students_data`.`team-1-data-schema`.dim_driver
# MAGIC UNION ALL
# MAGIC SELECT 'dim_booking_source', COUNT(*), COUNT(DISTINCT booking_source_sk), COUNT(*) - COUNT(DISTINCT booking_source_sk) FROM `students_data`.`team-1-data-schema`.dim_booking_source
# MAGIC UNION ALL
# MAGIC SELECT 'dim_location', COUNT(*), COUNT(DISTINCT location_sk), COUNT(*) - COUNT(DISTINCT location_sk) FROM `students_data`.`team-1-data-schema`.dim_location
# MAGIC UNION ALL
# MAGIC SELECT 'dim_capability', COUNT(*), COUNT(DISTINCT capability_sk), COUNT(*) - COUNT(DISTINCT capability_sk) FROM `students_data`.`team-1-data-schema`.dim_capability

# COMMAND ----------

# DBTITLE 1,Validation — Key fields NOT NULL
# MAGIC %sql
# MAGIC -- Surrogate keys and natural keys must never be NULL (null_count should be 0 for every row)
# MAGIC SELECT 'dim_date' AS dimension, 'date_sk' AS key_column, SUM(CASE WHEN date_sk IS NULL THEN 1 ELSE 0 END) AS null_count FROM `students_data`.`team-1-data-schema`.dim_date
# MAGIC UNION ALL SELECT 'dim_date', 'full_date', SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_date
# MAGIC UNION ALL SELECT 'dim_time', 'time_sk', SUM(CASE WHEN time_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_time
# MAGIC UNION ALL SELECT 'dim_time', 'hour', SUM(CASE WHEN hour IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_time
# MAGIC UNION ALL SELECT 'dim_payment_type', 'payment_type_sk', SUM(CASE WHEN payment_type_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_payment_type
# MAGIC UNION ALL SELECT 'dim_payment_type', 'payment_type_name', SUM(CASE WHEN payment_type_name IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_payment_type
# MAGIC UNION ALL SELECT 'dim_driver', 'driver_sk', SUM(CASE WHEN driver_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_driver
# MAGIC UNION ALL SELECT 'dim_driver', 'driver_number', SUM(CASE WHEN driver_number IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_driver
# MAGIC UNION ALL SELECT 'dim_booking_source', 'booking_source_sk', SUM(CASE WHEN booking_source_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_booking_source
# MAGIC UNION ALL SELECT 'dim_booking_source', 'booking_source_name', SUM(CASE WHEN booking_source_name IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_booking_source
# MAGIC UNION ALL SELECT 'dim_location', 'location_sk', SUM(CASE WHEN location_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_location
# MAGIC UNION ALL SELECT 'dim_location', 'zone_name', SUM(CASE WHEN zone_name IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_location
# MAGIC UNION ALL SELECT 'dim_capability', 'capability_sk', SUM(CASE WHEN capability_sk IS NULL THEN 1 ELSE 0 END) FROM `students_data`.`team-1-data-schema`.dim_capability

# COMMAND ----------

# DBTITLE 1,Validation — Dimension content spot-checks
# MAGIC %sql
# MAGIC -- Spot-checks: dim_time row count, payment types listed, zone count, capability combos, date range
# MAGIC SELECT 'dim_time row count' AS check_name,
# MAGIC        CAST(COUNT(*) AS STRING) AS result,
# MAGIC        CASE WHEN COUNT(*) = 1440 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_time
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'payment_type values',
# MAGIC        CONCAT_WS(', ', COLLECT_LIST(payment_type_name)),
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_payment_type
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'dim_location zone count',
# MAGIC        CAST(COUNT(*) AS STRING),
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_location
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'dim_capability combo count',
# MAGIC        CAST(COUNT(*) AS STRING),
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_capability
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'dim_date contiguous (no gaps)',
# MAGIC        CONCAT(CAST(COUNT(*) AS STRING), ' rows, span = ', CAST(DATEDIFF(MAX(full_date), MIN(full_date)) + 1 AS STRING)),
# MAGIC        CASE WHEN COUNT(*) = DATEDIFF(MAX(full_date), MIN(full_date)) + 1 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'dim_date max = min + 100 years - 1 day',
# MAGIC        CONCAT(CAST(MIN(full_date) AS STRING), ' → ', CAST(MAX(full_date) AS STRING)),
# MAGIC        CASE WHEN MAX(full_date) = CAST(MIN(full_date) + INTERVAL '100' YEAR - INTERVAL '1' DAY AS DATE) THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM `students_data`.`team-1-data-schema`.dim_date