# Databricks Tuning Guide: 03_Join_Strategies
#
# ==============================================================================
# 解説: Join Strategies (Sales x Products)
# ==============================================================================
# ... (背景説明は既存と同じ) ...
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("JoinStrategies").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")
spark.conf.set("spark.sql.adaptive.enabled", "true")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

df_sales = spark.table("sales")
df_products = spark.table("products")

# ---------------------------------------------------------
# 1. Broadcast Hash Join (BHJ)
# ---------------------------------------------------------
print("\n=== 1. Broadcast Hash Join (Recommended for Master Tables) ===")
print("【Spark UI チェックポイント】")
print("SQLタブ > DAGで 'BroadcastHashJoin' ノードを確認する。")
print("'Exchange' も 'Sort' も無い場合、これが最速。")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) # 10MB

def run_bhj():
    return df_sales.join(F.broadcast(df_products), "product_id").count()

measure_time("Broadcast Hash Join", run_bhj)


# ---------------------------------------------------------
# 2. Sort Merge Join vs Shuffle Hash Join
# ---------------------------------------------------------
print("\n=== 2. Sort Merge (SMJ) vs Shuffle Hash (SHJ) ===")
print("【Spark UI チェックポイント】")
print("A) Sort Merge: 'SortMergeJoin' ノードの手前に 'Sort' ノードが2つある")
print("B) Shuffle Hash: 'ShuffledHashJoin' ノードがある (Sortはない)")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # Disable Broadcast

# A) Sort Merge
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
print("\n--- A) Sort Merge Join ---")
measure_time("Sort Merge Join", lambda: df_sales.join(df_products.hint("merge"), "product_id").count())

# B) Shuffle Hash
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
print("\n--- B) Shuffle Hash Join (Photon Friendly) ---")
measure_time("Shuffle Hash Join", lambda: df_sales.join(df_products.hint("shuffle_hash"), "product_id").count())


# ---------------------------------------------------------
# 3. Skew Join Handling
# ---------------------------------------------------------
print("\n=== 3. Skew Join Verification ===")
print("【Spark UI チェックポイント】")
print("1. Stagesタブ > 'Event Timeline' を見る。1つのタスクだけ極端に長いバーになっていたらSkew。")
print("2. AQEが効くと、SQLタブ詳細に 'number of skewed partitions' などのメトリクスが出る。")
print("   または DAG上で小さいSplitタスクに分割されている。")

# 'PRODUCT_SKEW' に売上が集中
df_skew_sales = df_sales.filter(F.col("product_id") == "PRODUCT_SKEW")
df_skew_dim = spark.createDataFrame([("PRODUCT_SKEW", "Hit Product")], ["product_id", "product_name"])

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

print("Running Skew Join (Sales skew on 'PRODUCT_SKEW')...")
measure_time("Skew Join (AQE)", lambda: df_skew_sales.join(df_skew_dim, "product_id").count())

spark.stop()
