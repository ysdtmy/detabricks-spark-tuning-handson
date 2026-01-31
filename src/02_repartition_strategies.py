# Databricks Tuning Guide: 02_Repartition_Strategies
#
# ==============================================================================
# 解説: Repartition / Coalesce / AQE Coalescing (Retail Data)
# ==============================================================================
# ... (背景説明は既存と同じ) ...
# ==============================================================================

from pyspark.sql import SparkSession
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("RepartitionStrategies").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

# データ準備
df = spark.table("sales") 

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# ---------------------------------------------------------
# 1. Repartition vs Coalesce
# ---------------------------------------------------------
print("\n=== 1. Basic: Repartition vs Coalesce ===")
print("【Spark UI チェックポイント】")
print("1. SQLタブ > DAG を見る")
print("   - Repartition: 'Exchange' ノードがあるはず (Shuffle発生)")
print("   - Coalesce: 'Exchange' がないはず")
print("2. Stagesタブ > Shuffle Read / Write")
print("   - Repartition: 数百MB〜GB単位の書き込みがある")
print("   - Coalesce: 0 B")

# Repartition (Shuffle)
print("\n--- Repartition(200) ---")
print("Forces Shuffle (Exchange). Expensive for 10M rows.")
measure_time("Repartition", lambda: df.repartition(200).count())

# Coalesce (No Shuffle)
print("\n--- Coalesce(10) ---")
print("Merges partitions locally. Very fast.")
measure_time("Coalesce", lambda: df.coalesce(10).count())


# ---------------------------------------------------------
# 2. AQE Partition Coalescing
# ---------------------------------------------------------
print("\n=== 2. AQE Partition Coalescing Verification ===")
print("【Spark UI チェックポイント】")
print("1. SQLタブ > AQEの詳細 (DBRバージョンにより見え方が違うが、'AQE Split / Coalesce' のような表示)")
print("2. または print出力された 'getNumPartitions' の結果を見る")

print("Scenario: Grouping Sales by Product ID (Shuffle required).")

# AQE On
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Bad Config (Too many partitions)
spark.conf.set("spark.sql.shuffle.partitions", 2000)

print("Running Aggregation (Group by product_id)...")
df_agg = df.groupBy("product_id").count()
df_agg.collect()

final_partitions = df_agg.rdd.getNumPartitions()
print(f"\nConfigured Partitions: 2000")
print(f"Actual AQE Partitions: {final_partitions}")

if final_partitions < 2000:
    print("✅ AQE optimized the partition count!")
else:
    print("⚠️ AQE did not reduce partitions.")

spark.stop()
