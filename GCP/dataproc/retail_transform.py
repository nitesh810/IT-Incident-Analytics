from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count as _count, month, year
)

# ────────────────────────────────────────────────
# Spark Init (Configs MUST be inside builder)
# ────────────────────────────────────────────────
spark = (
    SparkSession.builder
        .appName("retail_transform")
        .config("viewsEnabled", "true")
        .config("materializationDataset", "incident_data")
        .config("temporaryGcsBucket", "supply-dataproc-temp")   # FIXED
        .getOrCreate()
)

project_id = "supply-gbl-ww-pd"
dataset = "incident_data"

# ────────────────────────────────────────────────
# 1. GET ALL retail_* TABLES
# ────────────────────────────────────────────────
tables_df = (
    spark.read.format("bigquery")
    .option(
        "query",
        f"""
        SELECT table_id
        FROM `{project_id}.{dataset}.__TABLES__`
        WHERE STARTS_WITH(table_id, 'retail_')
        """
    )
    .load()
)

table_names = [row.table_id for row in tables_df.collect()]

if not table_names:
    raise Exception("No tables found starting with 'retail_'")

print("Detected tables:", table_names)

# ────────────────────────────────────────────────
# 2. READ + UNION ALL retail_* TABLES
# ────────────────────────────────────────────────
df_all = None

for t in table_names:
    df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.{dataset}.{t}")
        .load()
    )
    
    if df_all is None:
        df_all = df
    else:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

df_all.cache()

# ────────────────────────────────────────────────
# 3. GENERATE DASHBOARD TABLES
# ────────────────────────────────────────────────

# A. REGION SALES
region_sales = (
    df_all.groupBy("region")
    .agg(
        _sum("price").alias("total_revenue"),
        _count("*").alias("total_orders")
    )
)

region_sales.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset}.region_sales_all") \
    .mode("overwrite") \
    .save()

# B. PRODUCT SALES
product_sales = (
    df_all.groupBy("product")
    .agg(
        _count("*").alias("units_sold"),
        _sum("price").alias("revenue")
    )
)

product_sales.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset}.product_sales_all") \
    .mode("overwrite") \
    .save()

# C. MONTHLY TREND
monthly_trend = (
    df_all.withColumn("year", year("order_date"))
          .withColumn("month", month("order_date"))
          .groupBy("year", "month")
          .agg(_sum("price").alias("monthly_revenue"))
)

monthly_trend.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset}.monthly_trend_all") \
    .mode("overwrite") \
    .save()

# D. STATUS SUMMARY
status_summary = (
    df_all.groupBy("status")
    .agg(_count("*").alias("count"))
)

status_summary.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset}.status_summary_all") \
    .mode("overwrite") \
    .save()

# E. PRIORITY × SEVERITY MATRIX
priority_severity = (
    df_all.groupBy("priority", "severity")
    .agg(_count("*").alias("count"))
)

priority_severity.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset}.priority_severity_all") \
    .mode("overwrite") \
    .save()

print("All dashboard tables created successfully!")

spark.stop()