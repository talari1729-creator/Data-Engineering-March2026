# Databricks notebook source
# MAGIC %md
# MAGIC # Source file landing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.earth_quakes;

# COMMAND ----------

df = spark.table("workspace.default.earth_quakes")
display(df)

# COMMAND ----------

#/Volumes/dataengineering/ingestionlayer/earthquakes
df = spark.read.option("header","true").csv(
    "/Volumes/dataengineering/ingestionlayer/earthquakes"
)


# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline Start Time Capture

# COMMAND ----------

from datetime import datetime

pipeline_start_time = datetime.now()

print("Pipeline Started:", pipeline_start_time)

# COMMAND ----------

# MAGIC %md
# MAGIC # Source File Availability Validation

# COMMAND ----------

file_path = "/Volumes/dataengineering/ingestionlayer/earthquakes"

try:
    files = dbutils.fs.ls(file_path)

    if len(files) == 0:
        raise Exception("No files found")

    print("Source File Available")

except Exception as e:
    print("File Not Available")
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC # File Format Validation

# COMMAND ----------

for file in files:
    if not file.name.endswith(".csv"):
        raise Exception(f"Invalid File Format: {file.name}")

        raise Exception(f"Invalid File Format: {file.name}")
print("File Format Validation Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC # File Size Validation

# COMMAND ----------

for file in files:
    print(file.name, file.size)

    if file.size == 0:
        raise Exception(f"Empty File Found: {file.name}")

print("File Size Validation Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Source File

# COMMAND ----------

df = spark.read \
          .option("header","true") \
          .option("inferSchema","true") \
          .csv(file_path)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Capture Ingestion Timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

df.select("ingestion_timestamp").show()

# COMMAND ----------

df = spark.read \
    .option("header","true") \
    .csv("/Volumes/dataengineering/ingestionlayer/earthquakes") \
    .select("*", "_metadata")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Capture Source File Name

# COMMAND ----------

# DBTITLE 1,Cell 18
from pyspark.sql.functions import col

df = df.withColumn(
    "source_file_name",
    col("_metadata.file_path")
)
df.select("source_file_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Capture Batch Identifier

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit

batch_id = datetime.now().strftime("raajan1729")

df = df.withColumn(
    "batch_id",
    lit(batch_id)
)
display(batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Capture Processing Date

# COMMAND ----------

from pyspark.sql.functions import current_date

df = df.withColumn(
    "processing_date",
    current_date()
)
df.select("processing_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Total Records Read

# COMMAND ----------

total_records = df.count()

print("Total Records:", total_records)

# COMMAND ----------

# MAGIC %md
# MAGIC #Empty File Validation

# COMMAND ----------

if total_records == 0:
    raise Exception("File is Empty")

print("Empty File Validation Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Corrupt File Validation

# COMMAND ----------

df = spark.read \
    .option("header","true") \
    .option("inferSchema","true") \
    .option("mode","PERMISSIVE") \
    .csv(file_path)

print("Corrupt File Validation Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC #Duplicate File Validation

# COMMAND ----------

current_file = files[0].name

processed_files = ["earthquake_20240601.csv"]

if current_file in processed_files:
    raise Exception("Duplicate File Detected")

print("Duplicate File Validation Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Validation

# COMMAND ----------

# expected_columns = [
#     "ID",
#     "Magnitude",
#     "Time"
# ] 

# actual_columns = df.columns
# print(f"Actual columns: {actual_columns}")


# if not set(expected_columns).issubset(set(actual_columns)):
#     raise Exception(f"Schema Mismatch: Missing columns {set(expected_columns) - set(actual_columns)}")

# print("Schema Validation Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC #Audit Information Generate

# COMMAND ----------

audit_data = [
    (
        batch_id,
        current_file,
        total_records,
        "SUCCESS"
    )
]

audit_df = spark.createDataFrame(
    audit_data,
    [
        "batch_id",
        "file_name",
        "record_count",
        "status"
    ]
)

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Execution Log Generate

# COMMAND ----------

pipeline_end_time = datetime.now()

log_data = [
    (
        pipeline_start_time,
        pipeline_end_time,
        current_file,
        total_records,
        "SUCCESS",
        ""
    )
]

log_df = spark.createDataFrame(
    log_data,
    [
        "start_time",
        "end_time",
        "file_name",
        "record_count",
        "status",
        "error_message"
    ]
)

display(log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Raw Data Load to Bronze Table

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("append") \
  .option("delta.columnMapping.mode", "name") \
  .saveAsTable(
      "dataengineering.ingestionlayer.earthquakes"
  )

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW SCHEMAS").show(truncate=False)

# COMMAND ----------

print("Current Catalog:", spark.catalog.currentCatalog())
print("Current Database:", spark.catalog.currentDatabase())

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN dataengineering").show(truncate=False)

# COMMAND ----------

print("Catalog:", spark.catalog.currentCatalog())
print("Schema:", spark.catalog.currentDatabase())

# COMMAND ----------

df = spark.read \
    .option("header","true") \
    .option("inferSchema","true") \
    .csv("/Volumes/dataengineering/ingestionlayer/earthquakes")

# COMMAND ----------

display(
    spark.table("dataengineering.ingestionlayer.earthquakes")
)

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS dataengineering.ingestionlayer.earthquakes
""")

# COMMAND ----------

new_columns = []

for col_name in df.columns:
    new_columns.append(
        col_name.replace(" ", "_")
    )

df = df.toDF(*new_columns)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dataengineering.ingestionlayer.earthquakes"
    )

# COMMAND ----------

df = spark.read.table(
    "dataengineering.ingestionlayer.earthquakes"
)

display(df)

# COMMAND ----------

