# Databricks notebook source
# MAGIC %md
# MAGIC # 00_Data_Generation
# MAGIC
# MAGIC 検証用データの作成 (Retail Scenario)
# MAGIC * Sales Table: 10M rows
# MAGIC * Products Table: 10k rows
# MAGIC * Includes: Skewed Data & Small Files

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 設定: Load from config
# Variables are injected by %run
# CATALOG_NAME, SCHEMA_NAME, NUM_SALES_ROWS, etc.

spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

# Schema作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

print(f"Generating data in {CATALOG_NAME}.{SCHEMA_NAME}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Products Table (Dimension)
# MAGIC * Small table (Broadcast candidate)
# MAGIC * Contains `PRODUCT_SKEW` for Skew Join testing.

# COMMAND ----------

print("--- 1. Generating Products Table ---")
df_products = spark.range(0, NUM_PRODUCTS).withColumn("product_id", 
    F.concat(F.lit("PROD_"), F.col("id").cast("string"))
).withColumn("product_name", F.concat(F.lit("Product Name "), F.col("id"))) \
 .withColumn("category", (F.rand() * 100).cast("int").cast("string")) \
 .withColumn("price", (F.rand() * 1000).cast("int")) \
 .drop("id")

# skew用商品を明示的に作成
df_skew_prod = spark.createDataFrame([("PRODUCT_SKEW", "Skewed Product", "999", 500)], ["product_id", "product_name", "category", "price"])
df_products = df_products.union(df_skew_prod)

df_products.write.format("delta").mode("overwrite").saveAsTable(TBL_PRODUCTS)
print(f"Products Table Created: {df_products.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sales Table (Fact)
# MAGIC * Large table (10M rows)
# MAGIC * Heavily skewed towards `PRODUCT_SKEW` (for AQE Skew Join verification).

# COMMAND ----------

print("--- 2. Generating Sales Table ---")

# A) Normal Data (90%)
df_normal = spark.range(0, int(NUM_SALES * 0.9)).withColumn("product_id", 
    F.concat(F.lit("PROD_"), (1 + (F.rand() * (NUM_PRODUCTS - 1))).cast("int").cast("string"))
)

# B) Skew Data (10% - All 'PRODUCT_SKEW')
df_skew = spark.range(0, int(NUM_SALES * 0.1)).withColumn("product_id", F.lit("PRODUCT_SKEW"))

df_sales_base = df_normal.union(df_skew)

# Add other columns
# Payload to simulate realistic data size
df_sales = df_sales_base \
    .withColumn("txn_id", F.expr("uuid()")) \
    .withColumn("customer_id", (F.rand() * 100000).cast("int")) \
    .withColumn("txn_date", F.date_add(F.lit("2024-01-01"), (F.rand() * 365).cast("int"))) \
    .withColumn("amount", (F.rand() * 10000).cast("int")) \
    .withColumn("quantity", (F.rand() * 10).cast("int")) \
    .withColumn("payload", F.expr("repeat('X', 50)")) # Data size padding

df_sales.write.format("delta").mode("overwrite").saveAsTable(TBL_SALES)
print(f"Sales Table Created: {df_sales.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Small Files Table
# MAGIC * Simulates "Small File Problem" by repartitioning to 1 record per file.

# COMMAND ----------

print("--- 3. Generating Small Files Table ---")
# 10万件を10万ファイルにする (Extreme Case)
df_small = spark.table(TBL_SALES).limit(100000)
df_small.repartition(100000).write.format("delta").mode("overwrite").saveAsTable(TBL_SALES_SMALL)
print("Small Files Table Created.")

print("\nAll Data Generation Completed!")
