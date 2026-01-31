# Databricks notebook source
# MAGIC %md
# MAGIC # 03_Join_Strategies
# MAGIC
# MAGIC * **Topic**: Broadcast, SortMerge, ShuffleHash & Skew Joins.
# MAGIC * **Goal**: Choose best join strategy and handle skew.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import config

CATALOG_NAME = config.CATALOG_NAME
SCHEMA_NAME = config.SCHEMA_NAME

spark = SparkSession.builder.appName("JoinStrategies").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")
spark.conf.set("spark.sql.adaptive.enabled", "true")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

df_sales = spark.table(config.TBL_SALES)
df_products = spark.table(config.TBL_PRODUCTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Broadcast Hash Join (BHJ)
# MAGIC Recommended for **Big Table x Small Table**.
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * Look for **BroadcastHashJoin** node in DAG.
# MAGIC * Confirm NO **Exchange** (Shuffle) on the sales table side.

# COMMAND ----------

print("\n=== 1. Broadcast Hash Join ===")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) # 10MB

def run_bhj():
    return df_sales.join(F.broadcast(df_products), "product_id").count()

measure_time("Broadcast Hash Join", run_bhj)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sort Merge (SMJ) vs Shuffle Hash (SHJ)
# MAGIC Comparison for **Big Table x Big Table**.
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * **SMJ**: **Sort** nodes -> **SortMergeJoin** node.
# MAGIC * **SHJ**: **ShuffledHashJoin** node (No Sort). Preferred by Photon.

# COMMAND ----------

print("\n=== 2. Sort Merge (SMJ) vs Shuffle Hash (SHJ) ===")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # Disable Broadcast

# A) Sort Merge
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
print("\n--- A) Sort Merge Join ---")
measure_time("Sort Merge Join", lambda: df_sales.join(df_products.hint("merge"), "product_id").count())

# B) Shuffle Hash
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
print("\n--- B) Shuffle Hash Join (Photon Friendly) ---")
measure_time("Shuffle Hash Join", lambda: df_sales.join(df_products.hint("shuffle_hash"), "product_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Skew Join Handling
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * **Stages** tab -> **Event Timeline**.
# MAGIC * Skew: One long task bar.
# MAGIC * Optimzied: Multiple shorter bars for that partition.

# COMMAND ----------

print("\n=== 3. Skew Join Verification ===")

# 'PRODUCT_SKEW' に売上が集中
df_skew_sales = df_sales.filter(F.col("product_id") == "PRODUCT_SKEW")
df_skew_dim = spark.createDataFrame([("PRODUCT_SKEW", "Hit Product")], ["product_id", "product_name"])

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

print("Running Skew Join (Sales skew on 'PRODUCT_SKEW')...")
measure_time("Skew Join (AQE)", lambda: df_skew_sales.join(df_skew_dim, "product_id").count())
