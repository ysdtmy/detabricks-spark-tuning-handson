# Databricks notebook source
# MAGIC %md
# MAGIC # 01_Storage_Optimization
# MAGIC
# MAGIC * **Target**: Data Layout (Liquid Clustering vs Z-Order) & Compaction
# MAGIC * **Goal**: Reduce "Files Read" (Data Skipping) and fix "Small File Problem".

# COMMAND ----------

from pyspark.sql import SparkSession
import time
import config

# 設定
CATALOG_NAME = config.CATALOG_NAME
SCHEMA_NAME = config.SCHEMA_NAME

spark = SparkSession.builder.appName("StorageOptimization").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

# Disable Delta Cache to ensure we measure I/O skipping, not RAM speed
spark.conf.set("spark.databricks.io.cache.enabled", "false")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Liquid Clustering vs Z-Order
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC 1. Go to **SQL** tab -> **Graph**
# MAGIC 2. Find **Scan parquet/delta** node.
# MAGIC 3. Check **number of files read** metric.
# MAGIC    * Standard: Reads ALL files (No skipping).
# MAGIC    * Liquid/Z-Order: Reads FEW files (Skipping active).

# COMMAND ----------

print("\n=== 1. Liquid Clustering vs Z-Order Verification (Sales Data) ===")

df_source = spark.read.table(config.TBL_SALES)
target_product = "PROD_50" 
target_date_start = "2024-06-01"

def run_query(table_name):
    # フィルタ条件: 特定商品 かつ 特定期間
    sql = f"""
    SELECT sum(amount) 
    FROM {table_name} 
    WHERE product_id = '{target_product}' 
      AND txn_date >= '{target_date_start}'
    """
    return spark.sql(sql).collect()

# COMMAND ----------

# A) 標準 (No Optimization)
print("\n--- A) Standard Table (Unoptimized) ---")
spark.sql("DROP TABLE IF EXISTS sales_standard")
# Force many small files (200 partitions) to simulate "unoptimized" state and emphasize scanning cost
df_source.repartition(200).write.format("delta").saveAsTable("sales_standard")

# COMMAND ----------

# B) Liquid Clustering (Cluster by product_id, txn_date)
print("\n--- B) Liquid Clustering Table ---")
spark.sql("DROP TABLE IF EXISTS sales_liquid")
df_source.write.format("delta").option("clusteringColumns", "product_id, txn_date").saveAsTable("sales_liquid")
df_source.write.format("delta").option("clusteringColumns", "product_id, txn_date").saveAsTable("sales_liquid")
print("Running OPTIMIZE on Liquid table...")
measure_time("Liquid OPTIMIZE (Incremental)", lambda: spark.sql("OPTIMIZE sales_liquid").collect())

# COMMAND ----------

# C) Z-Order (ZORDER BY product_id, txn_date)
print("\n--- C) Z-Order Table ---")
spark.sql("DROP TABLE IF EXISTS sales_zorder")
df_source.write.format("delta").saveAsTable("sales_zorder")
df_source.write.format("delta").saveAsTable("sales_zorder")
print("Running OPTIMIZE ZORDER BY on Z-Order table...")
measure_time("Z-Order OPTIMIZE (Heavier)", lambda: spark.sql("OPTIMIZE sales_zorder ZORDER BY (product_id, txn_date)").collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Comparison
# MAGIC Compare execution duration (Sec) and Files Read (UI).

# COMMAND ----------

print("\n--- Performance Comparison Results ---")
measure_time("Standard: Full Scan", lambda: run_query("sales_standard"))
measure_time("Liquid : Data Skipping", lambda: run_query("sales_liquid"))
measure_time("Z-Order: Data Skipping", lambda: run_query("sales_zorder"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compaction (Small File Problem)
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * Look at **Scan** node in SQL tab.
# MAGIC * **metadata time** (listing overhead) should decrease after OPTIMIZE.

# COMMAND ----------

print("\n=== 2. Compaction Verification ===")

print("Checking 'sales_small_files'...")
num_files = spark.table(config.TBL_SALES_SMALL).inputFiles()
print(f"Files BEFORE: {len(num_files)}")

measure_time("Read (Before)", lambda: spark.table(config.TBL_SALES_SMALL).count())

print("Running OPTIMIZE...")
spark.sql(f"OPTIMIZE {config.TBL_SALES_SMALL}")

print(f"Files AFTER : {len(spark.table(config.TBL_SALES_SMALL).inputFiles())}")
measure_time("Read (After)", lambda: spark.table(config.TBL_SALES_SMALL).count())
