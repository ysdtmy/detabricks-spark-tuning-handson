# Databricks notebook source
# MAGIC %md
# MAGIC # 99_Practice_Data_Generation (Dojo Setup)
# MAGIC
# MAGIC 練習問題(Dojo)用の軽量データを生成します。
# MAGIC * `practice_products`
# MAGIC * `practice_sales_skew`
# MAGIC * `practice_sales_unoptimized`

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# CATALOG_NAME, SCHEMA_NAME from config
NUM_PRODUCTS = 5_000
NUM_SALES = 5_000_000 # 500万件

spark = SparkSession.builder.appName("PracticeDataGen").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

print("Generating Practice Data...")

# COMMAND ----------

# 1. Practice Products
print("--- 1. practice_products ---")
df_products = spark.range(0, NUM_PRODUCTS).withColumn("product_id", 
    F.concat(F.lit("P_"), F.col("id").cast("string"))
).withColumn("product_name", F.concat(F.lit("Product "), F.col("id"))) \
 .withColumn("price", (F.rand() * 1000).cast("int"))

df_products.write.format("delta").mode("overwrite").saveAsTable(TBL_PRACTICE_PRODUCTS)

# COMMAND ----------

# 2. Practice Sales (Skew & Unoptimized)
print(f"--- 2. {TBL_PRACTICE_SALES_SKEW} (Skewed & Unoptimized) ---")

# Skew: ID 'P_0' is 90% of data
df_skew = spark.range(0, int(NUM_SALES * 0.9)).withColumn("product_id", F.lit("P_0"))
df_normal = spark.range(0, int(NUM_SALES * 0.1)).withColumn("product_id", 
    F.concat(F.lit("P_"), (1 + (F.rand() * (NUM_PRODUCTS - 1))).cast("int").cast("string"))
)

df_all = df_skew.union(df_normal) \
    .withColumn("txn_id", F.expr("uuid()")) \
    .withColumn("txn_date", F.date_add(F.lit("2024-01-01"), (F.rand() * 100).cast("int"))) \
    .withColumn("amount", (F.rand() * 10000).cast("int")) \
    .withColumn("quantity", (F.rand() * 10).cast("int"))

# For Skew Join
df_all.write.format("delta").mode("overwrite").saveAsTable(TBL_PRACTICE_SALES_SKEW)

# For Storage Challenge (No Optimization)
df_all.write.format("delta").mode("overwrite").saveAsTable(TBL_PRACTICE_SALES_UNOPT)

print("✅ Practice Data Generation Completed.")
