# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Data Profiling Title
# MAGIC %md
# MAGIC # Data Profiling — Taxi Gold Layer
# MAGIC
# MAGIC Automated data quality and profiling output for the Gold taxi data.  
# MAGIC Covers: null analysis, distinct counts, min/max/mean, value distributions, and data type validation.

# COMMAND ----------

# DBTITLE 1,Load Gold taxi data
# Load the Gold taxi data as a DataFrame
df_gold = spark.table("students_data.`team-1-data-schema`.gold_taxi_data")
print(f"Row count: {df_gold.count():,}")
print(f"Column count: {len(df_gold.columns)}")

# COMMAND ----------

# DBTITLE 1,Profile Gold taxi data
# Full data profiling — manual implementation (serverless-compatible)
# Produces: null counts, distinct counts, min/max/mean for every column
from pyspark.sql import functions as F

total_rows = df_gold.count()

# Build profiling expressions for each column
profile_exprs = []
for c in df_gold.columns:
    dtype = dict(df_gold.dtypes)[c]
    profile_exprs.extend([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}__nulls"),
        F.countDistinct(F.col(c)).alias(f"{c}__distinct"),
    ])
    if dtype in ("double", "int", "integer", "long", "float"):
        profile_exprs.extend([
            F.min(F.col(c)).alias(f"{c}__min"),
            F.max(F.col(c)).alias(f"{c}__max"),
            F.round(F.mean(F.col(c)), 2).alias(f"{c}__mean"),
        ])

stats_row = df_gold.agg(*profile_exprs).collect()[0]

# Assemble into a readable profiling report
import pandas as pd

rows = []
for c in df_gold.columns:
    dtype = dict(df_gold.dtypes)[c]
    nulls = stats_row[f"{c}__nulls"]
    distinct = stats_row[f"{c}__distinct"]
    entry = {
        "Column": c,
        "Type": dtype,
        "Nulls": nulls,
        "Null %": round(100.0 * nulls / total_rows, 2),
        "Distinct": distinct,
        "Distinct %": round(100.0 * distinct / total_rows, 2),
    }
    if dtype in ("double", "int", "integer", "long", "float"):
        entry["Min"] = stats_row[f"{c}__min"]
        entry["Max"] = stats_row[f"{c}__max"]
        entry["Mean"] = stats_row[f"{c}__mean"]
    rows.append(entry)

df_profile = pd.DataFrame(rows)
print(f"Profiling {total_rows:,} rows across {len(df_gold.columns)} columns\n")
display(df_profile)

# COMMAND ----------

# DBTITLE 1,Drift detection — compare current profile against baseline
# Drift detection — compare current profile stats against a known baseline
# Baseline values are taken from the initial profiling run above.
# Any column where Null %, Distinct count, or Mean drifts beyond the
# threshold is flagged.

DRIFT_THRESHOLD_NULL_PCT = 2.0    # flag if Null % shifts by more than 2 points
DRIFT_THRESHOLD_MEAN_PCT = 10.0   # flag if Mean shifts by more than 10%

# Baseline snapshot (from the first profiling run)
baseline = {
    "booking_id":            {"Null %": 0,     "Distinct": 763667, "Mean": None},
    "trip_status":            {"Null %": 0,     "Distinct": 3,      "Mean": None},
    "pickup_due":             {"Null %": 0,     "Distinct": 274854, "Mean": None},
    "completed":              {"Null %": 12.62, "Distinct": 274319, "Mean": None},
    "time_dispatched":        {"Null %": 8.62,  "Distinct": 280340, "Mean": None},
    "time_vehicle_arrived":   {"Null %": 14.56, "Distinct": 273722, "Mean": None},
    "time_picked_up":         {"Null %": 12.68, "Distinct": 274303, "Mean": None},
    "driver":                 {"Null %": 8.62,  "Distinct": 434,    "Mean": 260.55},
    "vehicle":                {"Null %": 8.62,  "Distinct": 551,    "Mean": 434.62},
    "price":                  {"Null %": 0,     "Distinct": 709,    "Mean": 6.93},
    "distance":               {"Null %": 0,     "Distinct": 3447,   "Mean": 2.61},
    "priority":               {"Null %": 0,     "Distinct": 10,     "Mean": 1.74},
    "wait_time_minutes":      {"Null %": 14.56, "Distinct": 194,    "Mean": 4.34},
    "trip_duration_minutes":  {"Null %": 12.72, "Distinct": 438,    "Mean": 9.03},
    "total_time_taken":       {"Null %": 12.62, "Distinct": 486,    "Mean": 16.97},
    "price_per_mile":         {"Null %": 0.19,  "Distinct": 924,    "Mean": 3.72},
}

# Compare current profile against baseline
drift_rows = []
for row in rows:
    col = row["Column"]
    if col not in baseline:
        continue
    bl = baseline[col]
    flags = []

    # Check null percentage drift
    null_delta = abs(row["Null %"] - bl["Null %"])
    if null_delta > DRIFT_THRESHOLD_NULL_PCT:
        flags.append(f"Null % shifted by {null_delta:+.2f} pp")

    # Check mean drift (numeric columns only)
    if bl["Mean"] is not None and row.get("Mean") is not None and bl["Mean"] != 0:
        mean_pct_change = abs(row["Mean"] - bl["Mean"]) / abs(bl["Mean"]) * 100
        if mean_pct_change > DRIFT_THRESHOLD_MEAN_PCT:
            flags.append(f"Mean shifted by {mean_pct_change:.1f}%")

    # Check distinct count change (flag if it doubled or halved)
    if bl["Distinct"] > 0:
        distinct_ratio = row["Distinct"] / bl["Distinct"]
        if distinct_ratio > 2.0 or distinct_ratio < 0.5:
            flags.append(f"Distinct changed: {bl['Distinct']:,} → {row['Distinct']:,}")

    drift_rows.append({
        "Column": col,
        "Baseline Null %": bl["Null %"],
        "Current Null %": row["Null %"],
        "Baseline Mean": bl["Mean"],
        "Current Mean": row.get("Mean"),
        "Status": "; ".join(flags) if flags else "Stable",
    })

df_drift = pd.DataFrame(drift_rows)
drifted = df_drift[df_drift["Status"] != "Stable"]

print(f"Drift check: {len(drift_rows)} columns compared against baseline")
print(f"Columns with drift: {len(drifted)}\n")

if len(drifted) > 0:
    print("⚠️  Drifted columns:")
    display(drifted)
else:
    print("✅ No drift detected — all columns within expected thresholds.")

print("\nFull drift report:")
display(df_drift)

# COMMAND ----------

# DBTITLE 1,Failing record handling strategy
# MAGIC %md
# MAGIC ## Data Quality Report — Failing Record Handling
# MAGIC
# MAGIC Records that fail quality rules are handled at three levels across the pipeline:
# MAGIC
# MAGIC | Layer | Rule | Behaviour | Handling |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Silver | `valid_booking_id` (booking_id IS NOT NULL) | `@dlt.expect_or_drop` | **Dropped** — rows silently removed |
# MAGIC | Silver | Minimum fare (price >= 3.20 for completed trips) | Hard filter | **Dropped** — below-threshold fares removed |
# MAGIC | Silver | Bounding box (destination coords within Ireland/UK) | Hard filter | **Dropped** — out-of-region records removed |
# MAGIC | Silver | `valid_pickup_due`, `valid_coordinates`, `non_negative_distance`, `completed_has_arrival_time`, `completed_has_pickup_time` | `@dlt.expect` | **Monitored** — rows kept, violations tracked in pipeline UI |
# MAGIC | Gold | `critical_trip_status_not_null` (trip_status IS NOT NULL) | `@dlt.expect_or_fail` | **Pipeline halted** — critical invariant violation stops all processing |
# MAGIC
# MAGIC No quarantine table is used — failing records are either dropped at Silver or monitored in place. The report below counts passed and dropped rows per rule.

# COMMAND ----------

# DBTITLE 1,Data quality report — passed/dropped counts per rule
# Data quality report — counts of passed / dropped rows per rule
from pyspark.sql import functions as F

# Load Bronze and Silver for comparison
df_bronze = spark.table("students_data.`team-1-data-schema`.bronze_taxi_data")
df_silver = spark.table("students_data.`team-1-data-schema`.silver_taxi_data")

bronze_count = df_bronze.count()
silver_count = df_silver.count()
gold_count = total_rows  # already computed in the profiling cell

# --- Count rows that would fail each drop/filter rule on Bronze ---

# 1. Null booking_id (expect_or_drop)
null_booking = df_bronze.filter(F.col("booking_id").isNull()).count()

# 2. Deduplicate — Bronze may have duplicate booking_ids
from pyspark.sql.window import Window
dedup_window = Window.partitionBy("booking_id").orderBy(F.col("ingestion_timestamp").desc())
df_deduped = df_bronze.withColumn("_rn", F.row_number().over(dedup_window)).filter(F.col("_rn") == 1).drop("_rn")
duplicates_removed = bronze_count - df_deduped.count()

# 3. Minimum fare filter (price < 3.20 for completed trips)
fare_dropped = df_deduped.filter(
    (F.col("source") == "Completed") & (F.regexp_replace(F.col("price"), ",", "").cast("double") < 3.20)
).count()

# 4. Bounding box filter (destination coords outside Ireland/UK)
bbox_dropped = df_deduped.filter(
    ~(
        F.col("destination_latitude").cast("double").between(51.4, 55.5)
        & F.col("destination_longitude").cast("double").between(-10.8, -5.2)
    )
).count()

# --- Count rows that fail monitor-only rules on Silver ---
monitor_rules = {
    "valid_pickup_due": df_silver.filter(F.col("pickup_due").isNull()).count(),
    "valid_coordinates": df_silver.filter(
        F.col("pickup_latitude").isNull() | F.col("pickup_longitude").isNull()
    ).count(),
    "non_negative_distance": df_silver.filter(
        (F.col("distance") < 0) & F.col("distance").isNotNull()
    ).count(),
    "completed_has_arrival_time": df_silver.filter(
        (F.col("trip_status") == "Completed") & F.col("time_vehicle_arrived").isNull()
    ).count(),
    "completed_has_pickup_time": df_silver.filter(
        (F.col("trip_status") == "Completed") & F.col("time_picked_up").isNull()
    ).count(),
}

# --- Assemble the report ---
import pandas as pd

report_rows = [
    {"Layer": "Bronze → Silver", "Rule": "Deduplication (keep latest per booking_id)", "Behaviour": "Dropped", "Rows Affected": duplicates_removed, "Rows Passed": bronze_count - duplicates_removed},
    {"Layer": "Bronze → Silver", "Rule": "valid_booking_id (NOT NULL)",               "Behaviour": "Dropped", "Rows Affected": null_booking,       "Rows Passed": bronze_count - null_booking},
    {"Layer": "Bronze → Silver", "Rule": "Minimum fare (>= £3.20 for completed)",     "Behaviour": "Dropped", "Rows Affected": fare_dropped,        "Rows Passed": df_deduped.count() - fare_dropped},
    {"Layer": "Bronze → Silver", "Rule": "Bounding box (Ireland/UK coords)",           "Behaviour": "Dropped", "Rows Affected": bbox_dropped,        "Rows Passed": df_deduped.count() - bbox_dropped},
]

for rule_name, fail_count in monitor_rules.items():
    report_rows.append({
        "Layer": "Silver",
        "Rule": f"{rule_name} (monitor only)",
        "Behaviour": "Monitored",
        "Rows Affected": fail_count,
        "Rows Passed": silver_count - fail_count,
    })

report_rows.append({
    "Layer": "Gold",
    "Rule": "critical_trip_status_not_null (fail pipeline)",
    "Behaviour": "Fail pipeline",
    "Rows Affected": 0,
    "Rows Passed": gold_count,
})

df_report = pd.DataFrame(report_rows)

print("=" * 60)
print("DATA QUALITY REPORT")
print("=" * 60)
print(f"Bronze rows:  {bronze_count:>10,}")
print(f"Silver rows:  {silver_count:>10,}")
print(f"Gold rows:    {gold_count:>10,}")
print(f"Total dropped: {bronze_count - silver_count:>9,}")
print("=" * 60)
print()
display(df_report)