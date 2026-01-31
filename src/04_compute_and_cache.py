# Databricks Tuning Guide: 04_Compute_and_Cache
#
# ==============================================================================
# 解説: Compute & Cache (Sales Data)
# ==============================================================================
# ... (背景説明は既存と同じ) ...
# ==============================================================================

from pyspark.sql import SparkSession
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("ComputeAndCache").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

df = spark.table("sales")

# ---------------------------------------------------------
# 1. Spark Cache Verification
# ---------------------------------------------------------
print("\n=== 1. Spark Cache Verification ===")
print("【Spark UI チェックポイント】")
print("1. 'Storage' タブを見る")
print("   - 'Fraction Cached' が 100% になっているか")
print("   - 'Size in Memory' (JVM Heap) vs 'Size on Disk' (Spill Check)")

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


# ---------------------------------------------------------
# 2. Disk Cache (Explanation)
# ---------------------------------------------------------
print("\n=== 2. Disk Cache Information ===")
print("【Spark UI チェックポイント】")
print("1. 'Storage' タブ > 'Disk Used' カラムを見る")
print("2. または Ganglia > 'Disk Usage' でWorkerのディスク使用率上昇を確認する")

print("Disk Cache is automatic on supported workers (e.g., L4ds, E4ds types).")


# ---------------------------------------------------------
# 3. Photon Engine Usage Check
# ---------------------------------------------------------
print("\n=== 3. Photon Engine Usage Check ===")
print("【Spark UI チェックポイント】")
print("1. SQLタブ > 実行計画(Graph)を見る")
print("2. ノード名に 'Photon' が付いているか確認 (例: PhotonProject, PhotonGroupAgg)")
print("   - 付いていればC++ネイティブ実行されている")
print("   - 付いていなければJVM実行されている (非対応の型や関数がある場合など)")

print("Query: Sum amount by product_id")

query = "SELECT product_id, sum(amount) FROM sales GROUP BY product_id"
df_agg = spark.sql(query)
plan = df_agg._jdf.queryExecution().executedPlan().toString()

if "Photon" in plan:
    print("\n✅ Photon is ENABLED!")
    photon_ops = [line.strip() for line in plan.split('\n') if "Photon" in line]
    for op in photon_ops[:5]:
        print(f" - {op}")
else:
    print("\n⚠️ Photon is NOT found.")

spark.stop()
