# Databricks Tuning Guide: 01_Storage_Optimization
#
# ==============================================================================
# 解説: ストレージ最適化 (Liquid vs Z-Order)
# ==============================================================================
# ... (シナリオ説明は既存と同じ) ...
# ==============================================================================

from pyspark.sql import SparkSession
import time

# 設定
CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("StorageOptimization").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# ---------------------------------------------------------
# 1. Liquid Clustering vs Z-Order の比較検証
# ---------------------------------------------------------
print("\n=== 1. Liquid Clustering vs Z-Order Verification (Sales Data) ===")
print("【Spark UI チェックポイント】")
print("1. Jobsタブから該当Jobをクリック > SQLタブを開く")
print("2. 実行計画のグラフで 'Scan parquet/delta' (一番下の青い箱) を探す")
print("3. 詳細メトリクス 'number of files read' を確認する")
print("   - Standard: 全ファイル読んでいるはず (Data Skippingなし)")
print("   - Liquid/Z-Order: ファイル数が激減しているはず (Data Skipping成功)")

df_source = spark.read.table("sales")
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

# A) 標準 (No Optimization)
print("\n--- A) Standard Table ---")
spark.sql("DROP TABLE IF EXISTS sales_standard")
df_source.write.format("delta").saveAsTable("sales_standard")

# B) Liquid Clustering (Cluster by product_id, txn_date)
print("\n--- B) Liquid Clustering Table ---")
spark.sql("DROP TABLE IF EXISTS sales_liquid")
df_source.write.format("delta").option("clusteringColumns", "product_id, txn_date").saveAsTable("sales_liquid")
spark.sql("OPTIMIZE sales_liquid")

# C) Z-Order (ZORDER BY product_id, txn_date)
print("\n--- C) Z-Order Table ---")
spark.sql("DROP TABLE IF EXISTS sales_zorder")
df_source.write.format("delta").saveAsTable("sales_zorder")
spark.sql("OPTIMIZE sales_zorder ZORDER BY (product_id, txn_date)")


# --- Performance Comparison ---
print("\n--- Performance Comparison Results ---")
measure_time("Standard: Full Scan", lambda: run_query("sales_standard"))
measure_time("Liquid : Data Skipping", lambda: run_query("sales_liquid"))
measure_time("Z-Order: Data Skipping", lambda: run_query("sales_zorder"))


# ---------------------------------------------------------
# 2. Compaction (Small File Problem)
# ---------------------------------------------------------
print("\n=== 2. Compaction Verification ===")
print("【Spark UI チェックポイント】")
print("1. SQLタブで Scan ノードを見る")
print("2. 'metadata time' (Listing time) が、OPTIMIZE前は長く、後は短くなっていれば成功")

print("Checking 'sales_small_files'...")
num_files = spark.table("sales_small_files").inputFiles()
print(f"Files BEFORE: {len(num_files)}")

measure_time("Read (Before)", lambda: spark.table("sales_small_files").count())

print("Running OPTIMIZE...")
spark.sql("OPTIMIZE sales_small_files")

print(f"Files AFTER : {len(spark.table("sales_small_files").inputFiles())}")
measure_time("Read (After)", lambda: spark.table("sales_small_files").count())

spark.stop()
