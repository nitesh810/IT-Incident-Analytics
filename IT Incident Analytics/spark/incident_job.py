from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("IncidentAnalytics").getOrCreate()

# BRONZE LAYER: Read raw CSV from GCS
# Raw ingestion of incident CSV files (no transformations yet)
input_file = "gs://incident-bucket/raw/incident_sample.csv"
df_raw = spark.read.option("header", "true").csv(input_file)


# SILVER LAYER: Data Cleaning
# Remove duplicates and fill missing category values
df_clean = df_raw.dropDuplicates()
df_clean = df_clean.fillna({"priority": "Medium", "category": "Unknown"})

# Renaming column names to lowercase
for c in df_clean.columns:
    df_clean = df_clean.withColumnRenamed(c, c.lower())

# GOLD LAYER: KPI Transformation
# Example KPI: incident count per category
df_kpi = (
    df_clean.groupBy("category")
    .agg(count("*").alias("incident_count"))
    .withColumn("kpi_type", lit("category_count"))
)

# Data Quality Check
row_count = df_clean.count()
if row_count == 0:
    print("No rows after cleaning!")
else:
    print(f"Rows after cleaning: {row_count}")

# Write to BigQuery
bq_table = "incident_project.gold_incident_kpi"
df_kpi.write.format("bigquery") \
    .option("table", bq_table) \
    .mode("overwrite") \
    .save()

print(f"KPI table written to {bq_table}")

# Stop Spark Session
spark.stop()
