# Databricks notebook source
bronze_df = spark.table("bronze.earthquakes")
silver_df = bronze_df
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Initial Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

start_time = time.time()

batch_id = int(time.time())
processing_time = current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC #Records Received Count

# COMMAND ----------

records_received = silver_df.count()

print("Records Received :", records_received)

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove Duplicates

# COMMAND ----------

# for single row
df_dedup = silver_df.dropDuplicates(["id"])

duplicates_removed = records_received - df_dedup.count()

print("Duplicates Removed :", duplicates_removed)

# COMMAND ----------

# for multiple rows 
df_dedup = silver_df.dropDuplicates(["id","time"])
silver_df.select("id").show()
silver_df.select("time").show()


# COMMAND ----------

# for all columns
silver_df = silver_df.dropDuplicates()
silver_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Handle Null Values

# COMMAND ----------

# is null boolean value
# for single column
silver_df.select(col("Depth_Seismic_Stations").isNull().alias("name is null")).show()

# COMMAND ----------

# is null boolean values
# for all columns
from pyspark.sql.functions import col

silver_df.select([
    col(c).isNull().alias(f"{c}_is_null")
    for c in silver_df.columns
]).show()

# COMMAND ----------

# null values count count 
from pyspark.sql.functions import col, when, count

silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in silver_df.columns
]).show()

# COMMAND ----------

#filter() with isNotNull()
# Get rows containing NULL values
silver_df.filter(col("Root_Mean_Square").isNotNull()).show()

# COMMAND ----------

# is not null boolean values
from pyspark.sql.functions import col

silver_df.select([
    col(c).isNotNull().alias(f"{c}_is_null")
    for c in silver_df.columns
]).show()

# COMMAND ----------

# is not null count values
from pyspark.sql.functions import col, when, count

silver_df.select([
    count(when(col(c).isNotNull(), c)).alias(c)
    for c in bronze_df.columns
]).show()

# COMMAND ----------

#fillna() - Replace NULL with constant value
silver_df.fillna(1729,subset=["Depth_Seismic_Stations"]).show()



# COMMAND ----------

#fillna() Multiple Columns
silver_df.fillna({
    "Depth_Error":12.34,
    "Depth_Seismic_Stations":1729,
    "Magnitude_Error":7.69,
}).show()

# COMMAND ----------

# dropna(how="any")
# Drop row if ANY column is NULL
silver_df.dropna(how="any").show()

# COMMAND ----------

# dropna(how="all")
# Drop row if ALL columns are NULL
silver_df.dropna(how="all").show()

# COMMAND ----------

# dropna(subset=[])
# Drop rows if specified column contains NULL
silver_df.dropna(subset=["Depth_Seismic_Stations"]).show()

# COMMAND ----------

# dropna(thresh=3)
# Keep rows having at least 3 non-null values
silver_df.dropna(thresh=3).show()

# COMMAND ----------

# coalesce()
# Returns first non-null value
silver_df.withColumn(
    "depth_error_final",
    coalesce(col("Depth_Error"),lit("18.17"))
).show()



# COMMAND ----------

# MAGIC %md
# MAGIC #Standardize Column Names

# COMMAND ----------

for c in valid_df.columns:
    valid_df = valid_df.withColumnRenamed(
        c,
        c.lower().replace(" ","_")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #Standardize Text Values

# COMMAND ----------

valid_df = valid_df.withColumn(
    "azimuthal_gap",
    initcap(trim(col("Azimuthal_Gap")))
)
display(valid_df)

# COMMAND ----------

from pyspark.sql.functions import initcap, trim, col

string_cols = [field.name for field in valid_df.schema.fields if field.dataType.simpleString() == "string"]

for c in string_cols:
    valid_df = valid_df.withColumn(
        c,
        initcap(trim(col(c)))
    )

print("Standardize Text Values is successfully validate")



# COMMAND ----------

print(silver_df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #Correct Data Types

# COMMAND ----------

silver_df = valid_df \
.withColumn("latitude", col("latitude").cast(DoubleType())) \
.withColumn("longitude", col("longitude").cast(DoubleType())) \
.withColumn("depth", col("depth").cast(DoubleType())) \
.withColumn("mag", col("mag").cast(DoubleType())) \
.withColumn("time", to_timestamp(col("time")))



# COMMAND ----------

# MAGIC %md
# MAGIC #Data Type Validation

# COMMAND ----------

from pyspark.sql.functions import col

datatype_errors = silver_df.filter(
    col("latitude").cast("double").isNull() & col("latitude").isNotNull() |
    col("longitude").cast("double").isNull() & col("longitude").isNotNull() |
    col("mag").cast("double").isNull() & col("mag").isNotNull()
)
print("Data Type Validation is passed")

# COMMAND ----------

# MAGIC %md
# MAGIC #Business Rule Validation

# COMMAND ----------

# Rule no 1
invalid_mag = valid_df.filter(
    col("magnitude") > 0
)
print("Rule 1 passed")

# COMMAND ----------

# rule 2 Latitude Range
invalid_lat = silver_df.filter(
    (col("latitude") < -90) |
    (col("latitude") > 90)
)
print("Rule 2 passed")


# COMMAND ----------

# Rule 3 Longitude Range
invalid_long = silver_df.filter(
    (col("longitude") < -180) |
    (col("longitude") > 180)
)
print("Rule 3 passed")


# COMMAND ----------

# MAGIC %md
# MAGIC # Null Percentage Validation

# COMMAND ----------

total_records = silver_df.count()

for c in silver_df.columns:
    null_count = silver_df.filter(col(c).isNull()).count()
    percentage = (null_count / total_records) * 100
    print(f"{c} {percentage:.2f} %")

# COMMAND ----------

# MAGIC %md
# MAGIC #invalid records

# COMMAND ----------

invalid_records = (
    null_records
    .union(datatype_errors)
    .union(invalid_mag)
    .union(invalid_lat)
    .union(invalid_long)
)

# COMMAND ----------

invalid_records = invalid_records.withColumn(
    "reject_reason",
    lit("Data Quality Failure")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Valid Records

# COMMAND ----------

valid_records = silver_df.subtract(invalid_records)

# COMMAND ----------

# MAGIC %md
# MAGIC # Records Received

# COMMAND ----------

records_received = silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Records Processed

# COMMAND ----------

from pyspark.sql.functions import col

processed_df = silver_df.filter(
    col("id").isNotNull() &
    col("magnitude").isNotNull() &
    (col("magnitude") > 0)
)
records_processed = processed_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Records Rejected

# COMMAND ----------

records_rejected = records_received - records_processed

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove Duplicates

# COMMAND ----------

dedup_df = bronze_df.dropDuplicates()

duplicates_removed = records_received - dedup_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Validation Report
# MAGIC

# COMMAND ----------

validation_report = [
    ("Records Received", records_received),
    ("Records Processed", records_processed),
    ("Records Rejected", records_rejected),
    ("Duplicates Removed", duplicates_removed)
]

report_df = spark.createDataFrame(
    validation_report,
    ["Metric","Value"]
)

display(report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Audit Report

# COMMAND ----------

audit_data = [
(
batch_id,
records_received,
records_processed,
records_rejected
)
]

audit_df = spark.createDataFrame(
    audit_data,
    [
        "batch_id",
        "records_received",
        "records_processed",
        "records_rejected"
    ]
)

audit_df.write \
.format("delta") \
.mode("append") \
.saveAsTable(
    "silver.audit_report"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Execution Logs

# COMMAND ----------

end_time = time.time()

duration = end_time - start_time

print("Processing Duration :", duration, "seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Delta Table

# COMMAND ----------

print(silver_df.columns)

# COMMAND ----------

silver_df= silver_df.withColumnRenamed("Magnitude", "mag")

# COMMAND ----------

if "mag" in silver_df.columns:
    print("mag exists")
else:
    print("mag missing")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.earthquakes")

# COMMAND ----------

spark.table("silver.earthquakes").show()