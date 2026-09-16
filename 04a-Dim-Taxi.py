# Databricks notebook source
# DBTITLE 1,Dimension Tables — Taxi Star Schema
# MAGIC %md
# MAGIC # Dimension Tables — Taxi Star Schema
# MAGIC
# MAGIC Seven dimension tables for the Kimball star schema, reading from `gold_taxi_data`.
# MAGIC
# MAGIC | Dimension | Grain | Source |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | One row per calendar date | All timestamp columns |
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

# DBTITLE 1,dim_date — one row per calendar date
@dlt.table(
    name="dim_date",
    comment="Date dimension — one row per calendar date extracted from all timestamp columns",
)
def dim_date():
    gold = dlt.read("gold_taxi_data")

    timestamp_cols = [
        "pickup_due", "completed", "time_dispatched",
        "time_vehicle_arrived", "time_picked_up",
    ]

    # Union distinct dates from every timestamp column
    date_dfs = [
        gold.select(F.to_date(F.col(c)).alias("full_date"))
            .filter(F.col(c).isNotNull())
        for c in timestamp_cols
    ]
    all_dates = reduce(lambda a, b: a.union(b), date_dfs).distinct()

    # Exclude erroneous future dates (e.g. year 2568 in source data)
    all_dates = all_dates.filter(
        F.col("full_date").isNotNull()
        & F.year("full_date").between(2020, 2030)
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