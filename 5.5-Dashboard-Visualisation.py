# Databricks notebook source
# DBTITLE 1,Section 5.5 – Dashboard & Visualisation
# MAGIC %md
# MAGIC # Section 5.5 – Dashboard & Visualisation
# MAGIC ### Tables
# MAGIC | Layer | Table | Type | Description |
# MAGIC |-------|-------|------|-------------|
# MAGIC | Gold | `fact_journey` | Fact | One row per booking |
# MAGIC | Gold | `dim_date` | Dimension | 100-year date range |
# MAGIC | Gold | `dim_time` | Dimension | 1,440 rows (minute-grain) |
# MAGIC | Gold | `dim_location` | Dimension | Pickup/destination zones |
# MAGIC | Gold | `dim_payment_type` | Dimension | Payment methods |
# MAGIC | Gold | `dim_driver` | Dimension | Driver numbers |
# MAGIC | Gold | `dim_booking_source` | Dimension | Booking channels |
# MAGIC | Gold | `dim_capability` | Dimension | Vehicle capabilities |
# MAGIC
# MAGIC ### Join Keys (Surrogate Key Relationships)
# MAGIC | Fact Column | Dimension Table | Dimension Key | Relationship |
# MAGIC |-------------|-----------------|---------------|--------------|
# MAGIC | `pickup_due_date_sk` | `dim_date` | `date_sk` | Many-to-One |
# MAGIC | `pickup_due_time_sk` | `dim_time` | `time_sk` | Many-to-One |
# MAGIC | `pickup_location_sk` | `dim_location` | `location_sk` | Many-to-One |
# MAGIC | `destination_location_sk` | `dim_location` | `location_sk` | Many-to-One |
# MAGIC | `payment_type_sk` | `dim_payment_type` | `payment_type_sk` | Many-to-One |
# MAGIC | `driver_sk` | `dim_driver` | `driver_sk` | Many-to-One |
# MAGIC | `booking_source_sk` | `dim_booking_source` | `booking_source_sk` | Many-to-One |
# MAGIC | `capability_sk` | `dim_capability` | `capability_sk` | Many-to-One |
# MAGIC
# MAGIC ## Three Stakeholder Business Questions
# MAGIC **Q1 (Business Owner / Dispatcher):** Which pickup zones generate the most work and what is our service performance?
# MAGIC - VIS 1 → 5 KPI counter widgets (total bookings, completed jobs, completion rate, cancel/no-fare rate, avg wait time)
# MAGIC - VIS 3 → Horizontal bar chart (top 15 busiest pickup zones by trip count)
# MAGIC
# MAGIC **Q2 (Drivers / Dispatcher):** When are the busiest periods and optimal driver working times?
# MAGIC - VIS 2 → Monthly trend line (booking demand over time)
# MAGIC - VIS 5 → Day × hour heatmap (weekly demand patterns)
# MAGIC
# MAGIC **Q3 (Business Owner):** Is there a relationship between payment type and cancellation rate — are card payments worth requiring?
# MAGIC - VIS 4 → Horizontal bar chart (cancellation rate by payment method)

# COMMAND ----------

# DBTITLE 1,VIS 1: Executive KPI Summary
# MAGIC %sql
# MAGIC -- VIS 1: Executive KPI Summary
# MAGIC -- Answers: Q1 (operational KPIs and service performance)
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT f.booking_id) AS total_bookings,
# MAGIC   COUNT(DISTINCT CASE WHEN f.trip_status = 'Completed' THEN f.booking_id END) AS completed_jobs,
# MAGIC   ROUND(100.0 * COUNT(DISTINCT CASE WHEN f.trip_status = 'Completed' THEN f.booking_id END) / COUNT(DISTINCT f.booking_id), 2) AS completion_rate_pct,
# MAGIC   ROUND(100.0 * COUNT(DISTINCT CASE WHEN f.trip_status IN ('Cancelled','No Fare') THEN f.booking_id END) / COUNT(DISTINCT f.booking_id), 2) AS cancel_nofare_rate_pct,
# MAGIC   ROUND(AVG(f.wait_time_minutes), 1) AS avg_wait_minutes
# MAGIC FROM `students_data`.`team-1-data-schema`.`fact_journey` f
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_date` d ON f.pickup_due_date_sk = d.date_sk

# COMMAND ----------

# DBTITLE 1,VIS 2: Daily Trip Volume Trend
# MAGIC %sql
# MAGIC -- VIS 2: Daily Trip Volume Trend
# MAGIC -- Answers: Q2 (when are the busiest periods)
# MAGIC SELECT
# MAGIC   d.full_date AS trip_date,
# MAGIC   d.month_name,
# MAGIC   d.day_name,
# MAGIC   COUNT(DISTINCT f.booking_id) AS trip_count,
# MAGIC   ROUND(SUM(f.price), 2) AS total_fares
# MAGIC FROM `students_data`.`team-1-data-schema`.`fact_journey` f
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_date` d ON f.pickup_due_date_sk = d.date_sk
# MAGIC WHERE d.full_date >= '2025-01-01' AND d.full_date <= CURRENT_DATE()
# MAGIC GROUP BY d.full_date, d.month_name, d.day_name
# MAGIC ORDER BY d.full_date

# COMMAND ----------

# DBTITLE 1,VIS 3: Busiest Pickup Zones
# MAGIC %sql
# MAGIC -- VIS 3: Busiest Pickup Zones by Trip Count & Revenue
# MAGIC -- Answers: Q1 (which parts of the business generate the most work)
# MAGIC SELECT
# MAGIC   l.zone_name AS pickup_zone,
# MAGIC   COUNT(DISTINCT f.booking_id) AS trip_count,
# MAGIC   ROUND(SUM(f.price), 2) AS total_fares,
# MAGIC   ROUND(AVG(f.price), 2) AS avg_fare,
# MAGIC   ROUND(AVG(f.wait_time_minutes), 1) AS avg_wait_minutes
# MAGIC FROM `students_data`.`team-1-data-schema`.`fact_journey` f
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_location` l ON f.pickup_location_sk = l.location_sk
# MAGIC GROUP BY l.zone_name
# MAGIC ORDER BY trip_count DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,VIS 4: Cancellation/No-Fare Rate by Payment Type
# MAGIC %sql
# MAGIC -- VIS 4: Cancellation/No-Fare Rate by Payment Type
# MAGIC -- Answers: Q3 (is it worth requiring card payments)
# MAGIC SELECT
# MAGIC   p.payment_type_name AS payment_type,
# MAGIC   COUNT(DISTINCT f.booking_id) AS total_bookings,
# MAGIC   COUNT(DISTINCT CASE WHEN f.trip_status = 'Completed' THEN f.booking_id END) AS completed,
# MAGIC   COUNT(DISTINCT CASE WHEN f.trip_status = 'Cancelled' THEN f.booking_id END) AS cancelled,
# MAGIC   COUNT(DISTINCT CASE WHEN f.trip_status = 'No Fare' THEN f.booking_id END) AS no_fare,
# MAGIC   ROUND(100.0 * COUNT(DISTINCT CASE WHEN f.trip_status IN ('Cancelled','No Fare') THEN f.booking_id END) / COUNT(DISTINCT f.booking_id), 2) AS cancel_nofare_rate_pct
# MAGIC FROM `students_data`.`team-1-data-schema`.`fact_journey` f
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_payment_type` p ON f.payment_type_sk = p.payment_type_sk
# MAGIC GROUP BY p.payment_type_name
# MAGIC ORDER BY cancel_nofare_rate_pct DESC

# COMMAND ----------

# DBTITLE 1,VIS 5: Trips Heatmap — Day of Week x Hour
# MAGIC %sql
# MAGIC -- VIS 5: Trips Heatmap — Day of Week x Hour
# MAGIC -- Answers: Q2 (optimal working periods during a typical week)
# MAGIC SELECT
# MAGIC   CASE d.day_name
# MAGIC     WHEN 'Monday' THEN '1-Mon' WHEN 'Tuesday' THEN '2-Tue' WHEN 'Wednesday' THEN '3-Wed'
# MAGIC     WHEN 'Thursday' THEN '4-Thu' WHEN 'Friday' THEN '5-Fri' WHEN 'Saturday' THEN '6-Sat'
# MAGIC     WHEN 'Sunday' THEN '7-Sun'
# MAGIC   END AS day_of_week,
# MAGIC   t.hour AS pickup_hour,
# MAGIC   COUNT(DISTINCT f.booking_id) AS trip_count
# MAGIC FROM `students_data`.`team-1-data-schema`.`fact_journey` f
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_date` d ON f.pickup_due_date_sk = d.date_sk
# MAGIC JOIN `students_data`.`team-1-data-schema`.`dim_time` t ON f.pickup_due_time_sk = t.time_sk
# MAGIC GROUP BY d.day_name, t.hour
# MAGIC ORDER BY CASE d.day_name WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3 WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 WHEN 'Sunday' THEN 7 END, pickup_hour

# COMMAND ----------

# DBTITLE 1,Dashboard Layout & Visual-to-Question Mapping
# MAGIC %md
# MAGIC ## Visual to Business Question Mapping
# MAGIC
# MAGIC | Question | Visual | Widget Type | Dataset |
# MAGIC |----------|--------|-------------|----------|
# MAGIC | Q1 | VIS 1 | 5 KPI counter widgets | `taxi_kpis` |
# MAGIC | Q1 | VIS 3 | Horizontal bar chart | `taxi_busiest_zones` |
# MAGIC | Q2 | VIS 2 | Monthly trend line | `taxi_monthly_trend` |
# MAGIC | Q2 | VIS 5 | Day x hour heatmap | `taxi_heatmap` |
# MAGIC | Q3 | VIS 4 | Horizontal bar chart | `taxi_payment_cancel` |
# MAGIC | Filters | - | 3 filters (Year, Month, Day) | `filter_years`, `filter_months`, `filter_days` |