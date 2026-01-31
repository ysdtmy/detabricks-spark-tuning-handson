# Databricks notebook source
# MAGIC %md
# MAGIC # 04_Compute_and_Cache
# MAGIC
# MAGIC * **Topic**: Spark Cache vs Disk Cache & Photon Engine.
# MAGIC * **Goal**: Optimize repeated access and verify Photon usage.

# COMMAND ----------

from pyspark.sql import SparkSession
import time
import config

CATALOG_NAME = config.CATALOG_NAME
SCHEMA_NAME = config.SCHEMA_NAME

spark = SparkSession.builder.appName("ComputeAndCache").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

df = spark.table(config.TBL_SALES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark Cache Verification
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * **Storage** Tab.
# MAGIC * **Fraction Cached**: Should be 100%.
# MAGIC * **Size in Memory**: Check if it fits in RAM or spills to disk.

# COMMAND ----------

print("\n=== 1. Spark Cache Verification ===")

# 特定条件の売上データを作成
df_subset = df.filter("amount > 100").select("txn_id", "amount")

print("\n--- 1st Run (Cold) ---")
measure_time("No Cache Count", lambda: df_subset.count())

print("\n--- Caching Data ---")
df_subset.cache()

print("--- 2nd Run (Caching in progress) ---")
measure_time("Cache Build Run", lambda: df_subset.count())

print("--- 3rd Run (Hot Cache) ---")
measure_time("Cached Hit Run", lambda: df_subset.count())

df_subset.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Photon Engine Usage Check
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * **SQL** Tab > **Graph**.
# MAGIC * Look for nodes starting with **Photon** (e.g., `PhotonProject`, `PhotonGroupAgg`).

# COMMAND ----------

print("\n=== 3. Photon Engine Usage Check ===")

print("Query: Sum amount by product_id")

query = f"SELECT product_id, sum(amount) FROM {config.TBL_SALES} GROUP BY product_id"
df_agg = spark.sql(query)
plan = df_agg._jdf.queryExecution().executedPlan().toString()

if "Photon" in plan:
    print("\n✅ Photon is ENABLED!")
    photon_ops = [line.strip() for line in plan.split('\n') if "Photon" in line]
    for op in photon_ops[:5]:
        print(f" - {op}")
else:
    print("\n⚠️ Photon is NOT found.")
