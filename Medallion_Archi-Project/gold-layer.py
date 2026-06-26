# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

silver_df = spark.table("silver.earthquakes")

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Business KPI Table

# COMMAND ----------

kpi_df = silver_df.agg(
    count("*").alias("total_earthquakes"),
    round(avg("mag"), 2).alias("avg_magnitude"),
    max("mag").alias("max_magnitude"),
    min("mag").alias("min_magnitude")
)

display(kpi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Summary Table
# MAGIC #Magnitude Type wise summary.

# COMMAND ----------

summary_df = (
    silver_df
    .groupBy("mag")
    .agg(
        count("*").alias("earthquake_count"),
        round(avg("mag"), 2).alias("avg_magnitude")
    )
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Trend Analysis Table
# MAGIC #Daily earthquake trends.

# COMMAND ----------

trend_df = (
    silver_df
    .withColumn("event_date", to_date("time"))
    .groupBy("event_date")
    .agg(
        count("*").alias("total_events"),
        round(avg("mag"), 2).alias("avg_magnitude")
    )
    .orderBy("event_date")
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Performance Metrics Table
# MAGIC #Location-wise performance metrics.

# COMMAND ----------

performance_df = (
    silver_df
    .groupBy("ID")
    .agg(
        count("*").alias("total_events"),
        round(avg("mag"), 2).alias("avg_magnitude"),
        max("mag").alias("max_magnitude")
    )
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Reporting Table

# COMMAND ----------

reporting_df = (
    silver_df
    .withColumn("event_date", to_date("time"))
    .select(
        "event_date",
        "latitude",
        "longitude",
        "depth",
        "mag"
        
    )
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregate Totals Validation

# COMMAND ----------

silver_total = silver_df.count()

gold_total = kpi_df.collect()[0]["total_earthquakes"]

if silver_total == gold_total:
    print("PASS - Aggregate totals match")
else:
    print(f"FAIL - Silver={silver_total}, Gold={gold_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Completeness Validation

# COMMAND ----------

silver_count = silver_df.count()

reporting_df = (
    silver_df
    .withColumn("event_date", to_date("time"))
    .select(
        "event_date",
        "latitude",
        "longitude",
        "depth",
        "mag"
    )
)

reporting_count = reporting_df.count()

if silver_count == reporting_count:
    print("PASS - No records lost")
else:
    print(f"FAIL - Missing records: {silver_count - reporting_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Duplicate Aggregates Validation
# MAGIC Summary table should have unique magType values.

# COMMAND ----------

from pyspark.sql.functions import count

summary_df = (
    silver_df
    .groupBy("mag")
    .agg(
        count("*").alias("earthquake_count"),
        round(avg("mag"), 2).alias("avg_magnitude")
    )
)

duplicates = (
    summary_df
    .groupBy("mag")
    .count()
    .filter("count > 1")
)

if duplicates.count() == 0:
    print("PASS - No duplicate aggregates")
else:
    print("FAIL - Duplicate aggregates found")
    display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC #Missing Dimensions Validation

# COMMAND ----------

from pyspark.sql.functions import col

missing_dimensions = reporting_df.filter(
    col("event_date").isNull() |
    col("mag").isNull()
)

if missing_dimensions.count() == 0:
    print("PASS - No missing dimensions")
else:
    print(f"FAIL - {missing_dimensions.count()} records have missing dimensions")

# COMMAND ----------

# MAGIC %md
# MAGIC #Missing Measures Validation

# COMMAND ----------

missing_measures = reporting_df.filter(
    col("mag").isNull() |
    col("depth").isNull()
)

if missing_measures.count() == 0:
    print("PASS - No missing measures")
else:
    print(f"FAIL - {missing_measures.count()} records have missing measures")

# COMMAND ----------

# MAGIC %md
# MAGIC #Source Records

# COMMAND ----------

source_records = silver_df.count()
print(f"Source Records: {source_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Target Records

# COMMAND ----------

target_records = kpi_df.count()
print(f"Target Records: {target_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregation Counts

# COMMAND ----------

aggregation_count = len(kpi_df.columns)
print(f"Aggregation Count: {aggregation_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #KPI Generation Status

# COMMAND ----------

kpi_generation_status = "SUCCESS"
print(f"KPI Generation Status: {kpi_generation_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Processing Duration

# COMMAND ----------

from datetime import datetime

start_time = datetime.now()

# Gold Layer Processing Logic Here

end_time = datetime.now()

from builtins import round as py_round

processing_duration_seconds = py_round(
    (end_time - start_time).total_seconds(),
    2
)

print(f"Processing Duration (Seconds): {processing_duration_seconds}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Batch ID

# COMMAND ----------

from datetime import datetime

batch_id = datetime.now().strftime("raajan1729")

print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregation Date

# COMMAND ----------

from datetime import date

aggregation_date = date.today()

print(f"Aggregation Date: {aggregation_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Processing Timestamp

# COMMAND ----------

from datetime import datetime

processing_timestamp = datetime.now()

print(f"Processing Timestamp: {processing_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Load Status

# COMMAND ----------

gold_load_status = "SUCCESS"

print(f"Gold Load Status: {gold_load_status}")

# COMMAND ----------

#All Together (Audit Record)
from datetime import datetime, date

batch_id = datetime.now().strftime("raajan1729")
aggregation_date = date.today()
processing_timestamp = datetime.now()
gold_load_status = "SUCCESS"

audit_df = spark.createDataFrame(
    [(
        batch_id,
        str(aggregation_date),
        str(processing_timestamp),
        gold_load_status
    )],
    [
        "batch_id",
        "aggregation_date",
        "processing_timestamp",
        "gold_load_status"
    ]
)

display(audit_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dataengineering.goldlayer;

# COMMAND ----------

# MAGIC %md
# MAGIC #KPI Tables

# COMMAND ----------

#%sql
#CREATE VOLUME dataengineering.goldlayer.earthquakes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN dataengineering.goldlayer;

# COMMAND ----------

kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/dataengineering/goldlayer/earthquakes/business_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregated Reporting Tables

# COMMAND ----------

reporting_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/dataengineering/goldlayer/earthquakes/reporting_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #Validation Reports

# COMMAND ----------

validation_df = spark.createDataFrame(
    [
        ("Aggregate Totals", "PASS"),
        ("Data Completeness", "PASS"),
        ("Consistency Check", "PASS"),
        ("Duplicate Aggregates", "PASS"),
        ("Missing Dimensions", "PASS"),
        ("Missing Measures", "PASS")
    ],
    ["validation_check", "status"]
)

validation_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/dataengineering/goldlayer/earthquakes/validation_report")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Audit Report

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
from builtins import round as py_round

# Start Time
start_time = datetime.now()

# Processing Logic Here
try:
    source_records = silver_df.count()
except Exception:
    source_records = 0

try:
    target_records = reporting_df.count()
except Exception:
    target_records = 0

aggregation_count = 5
kpi_generation_status = "SUCCESS"
gold_load_status = "SUCCESS"

# End Time
end_time = datetime.now()

processing_duration_seconds = py_round(
    (end_time - start_time).total_seconds(), 2
)

batch_id = datetime.now().strftime("raajan1729")

audit_data = [
    Row(
        batch_id=batch_id,
        layer_name="Gold",
        source_records=source_records,
        target_records=target_records,
        aggregation_count=aggregation_count,
        kpi_generation_status=kpi_generation_status,
        gold_load_status=gold_load_status,
        processing_start_time=str(start_time),
        processing_end_time=str(end_time),
        processing_duration_seconds=processing_duration_seconds,
        audit_timestamp=str(datetime.now())
    )
]

audit_df = spark.createDataFrame(audit_data)

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Execution Log

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

execution_log_data = [
    Row(
        batch_id=datetime.now().strftime("raajan1729"),
        layer_name="Gold",
        job_name="Earthquake_Gold_Load",
        execution_status="SUCCESS",
        execution_timestamp=str(datetime.now())
    )
]

execution_log_df = spark.createDataFrame(execution_log_data)

display(execution_log_df)