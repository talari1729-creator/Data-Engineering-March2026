# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data from Ingestion Layer

# COMMAND ----------

df = spark.read.table(
    "dataengineering.ingestionlayer.earthquakes"
)

display(df)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime


df = spark.read \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .csv("/Volumes/dataengineering/ingestionlayer/earthquakes")


display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing Start Time

# COMMAND ----------

import time

start_time = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Batch Information

# COMMAND ----------

from datetime import datetime

batch_id = datetime.now().strftime("raajan1729")
source_system = "Earthquake_CSV"
display(batch_id)



# COMMAND ----------

# MAGIC %md
# MAGIC #Add Audit Columns

# COMMAND ----------

from pyspark.sql import functions as F
bronze_df = df \
    .withColumn("batch_id", F.lit(batch_id)) \
    .withColumn("load_timestamp", F.current_timestamp()) \
    .withColumn("processing_date", F.current_date()) \
    .withColumn("source_system", F.lit(source_system)) \
    .withColumn("created_timestamp",F.current_timestamp())

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation - Total Record Count

# COMMAND ----------

source_record_count = df.count()

print(f"Source Record Count : {source_record_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation - Null Count Per Column

# COMMAND ----------

null_counts = bronze_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in bronze_df.columns
])

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #Validation - Duplicate Record Count

# COMMAND ----------

duplicate_count = bronze_df.groupBy("id") \
                           .count() \
                           .filter(F.col("count") > 1) \
                           .count()

print(f"Duplicate Count : {duplicate_count}")

# COMMAND ----------

from pyspark.sql import functions as F

for col_name in df.columns:
    
    dup_count = (
        df.groupBy(col_name)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    
    print(f"{col_name}: {dup_count}")

# COMMAND ----------

from pyspark.sql import functions as F

for col_name in df.columns:
    
    total_count = df.count()
    distinct_count = df.select(col_name).distinct().count()
    
    duplicate_records = total_count - distinct_count
    
    print(f"{col_name}: {duplicate_records}")

# COMMAND ----------

duplicate_report = []

for col_name in df.columns:
    
    dup_count = df.count() - df.select(col_name).distinct().count()
    
    duplicate_report.append((col_name, dup_count))

duplicate_df = spark.createDataFrame(
    duplicate_report,
    ["column_name", "duplicate_count"]
)

display(duplicate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Validation - Schema Validation

# COMMAND ----------

expected_columns = [
    "id",
    "place",
    "mag",
    "time"
]

actual_columns = df.columns

missing_columns = list(set(expected_columns) - set(actual_columns))

if len(missing_columns) == 0:
    print("Schema Validation Passed")
else:
    print("Missing Columns :", missing_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation - Mandatory Column Validation

# COMMAND ----------

mandatory_columns = ["ID", "Magnitude"]

for col_name in mandatory_columns:
    
    null_count = bronze_df.filter(
        F.col(col_name).isNull()
    ).count()

    print(f"{col_name} Null Count = {null_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation Status:

# COMMAND ----------

mandatory_validation = "PASS"

for col_name in mandatory_columns:
    
    cnt = bronze_df.filter(
        F.col(col_name).isNull()
    ).count()

    if cnt > 0:
        mandatory_validation = "FAIL"

print(mandatory_validation)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Bronze Delta Table

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS dataengineering.bronze
""")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN dataengineering").show(truncate=False)

# COMMAND ----------

bronze_df = bronze_df.toDF(
    *[c.replace(" ", "_") for c in bronze_df.columns]
)

# COMMAND ----------

print(bronze_df.columns)

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dataengineering.bronze.earthquakes")

# COMMAND ----------

spark.sql("SHOW TABLES IN dataengineering.bronze").show()

# COMMAND ----------

display(spark.table("dataengineering.bronze.earthquakes"))

# COMMAND ----------

spark.table("dataengineering.bronze.earthquakes").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Target Record Count

# COMMAND ----------

target_df = spark.table("dataengineering.bronze.earthquakes")

target_record_count = target_df.count()

print(f"Target Record Count : {target_record_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Processing Duration

# COMMAND ----------

end_time = time.time()

processing_duration = round(end_time - start_time, 2)

print(f"Processing Duration : {processing_duration} Seconds")

# COMMAND ----------

end_time = time.time()

processing_duration = round(end_time - start_time, 2)

print(f"Processing Duration : {processing_duration} Seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #Validation Report

# COMMAND ----------

validation_report = {
    "Source_Count": source_record_count,\
    "Target_Count": target_record_count,\
    "Duplicate_Count": duplicate_count,\
    "Mandatory_Validation": mandatory_validation
}

print(validation_report)

# COMMAND ----------

# MAGIC %md
# MAGIC #Audit Report

# COMMAND ----------

audit_report = {
    "Batch_ID": batch_id,
    "Source_System": source_system,
    "Load_Time": str(datetime.now()),
    "Source_Count": source_record_count,
    "Target_Count": target_record_count,
    "Status": "SUCCESS"
}

print(audit_report)

# COMMAND ----------

# MAGIC %md
# MAGIC #Execution Log DataFrame

# COMMAND ----------

log_data = [
    (
        batch_id,
        source_record_count,
        target_record_count,
        duplicate_count,
        mandatory_validation,
        processing_duration,
        "SUCCESS"
    )
]

log_df = spark.createDataFrame(
    log_data,
    [
        "batch_id",
        "source_count",
        "target_count",
        "duplicate_count",
        "validation_status",
        "processing_duration",
        "job_status"
    ]
)

display(log_df)

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.earthquakes")