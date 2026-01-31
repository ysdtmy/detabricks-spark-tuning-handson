# Databricks notebook source
# MAGIC %md
# MAGIC # 02_Repartition_Strategies
# MAGIC
# MAGIC * **Topic**: Repartition vs Coalesce & AQE Partition Coalescing.
# MAGIC * **Goal**: Understand Shuffle cost and automatic optimization.

# COMMAND ----------

from pyspark.sql import SparkSession
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("RepartitionStrategies").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

df = spark.table("sales") 

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Repartition vs Coalesce
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC 1. **SQL** Tab > **DAG**
# MAGIC    * **Repartition**: Look for **Exchange** node (Shuffle).
# MAGIC    * **Coalesce**: No Exchange node (No shuffle).
# MAGIC 2. **Stages** Tab
# MAGIC    * **Shuffle Write**: Large for Repartition, 0B for Coalesce.

# COMMAND ----------

print("\n=== 1. Basic: Repartition vs Coalesce ===")

# Repartition (Shuffle)
print("\n--- Repartition(200) ---")
print("Forces Shuffle (Exchange). Expensive for 10M rows.")
measure_time("Repartition", lambda: df.repartition(200).count())

# Coalesce (No Shuffle)
print("\n--- Coalesce(10) ---")
print("Merges partitions locally. Very fast.")
measure_time("Coalesce", lambda: df.coalesce(10).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. AQE Partition Coalescing
# MAGIC
# MAGIC Scenario: Spark config says `spark.sql.shuffle.partitions = 2000` (Too many!).
# MAGIC Does AQE reduce it automatically?
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * Check **AQE Split / Coalesce** details in SQL tab details.

# COMMAND ----------

print("\n=== 2. AQE Partition Coalescing Verification ===")

# AQE On
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Bad Config (Too many partitions)
spark.conf.set("spark.sql.shuffle.partitions", 2000)

print("Running Aggregation (Group by product_id)...")
df_agg = df.groupBy("product_id").count()
df_agg.collect() # Trigger Action

final_partitions = df_agg.rdd.getNumPartitions()
print(f"\nConfigured Partitions: 2000")
print(f"Actual AQE Partitions: {final_partitions}")

if final_partitions < 2000:
    print("✅ AQE optimized the partition count!")
else:
    print("⚠️ AQE did not reduce partitions.")
