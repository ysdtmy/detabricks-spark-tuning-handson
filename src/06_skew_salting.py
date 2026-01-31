# Databricks Tuning Guide: 06_Skew_Salting
#
# ==============================================================================
# 解説: Manual Salting (Retail Skew)
# ==============================================================================
# ... (背景説明は既存と同じ) ...
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"
SALT_FACTOR = 10 # 10分割

spark = SparkSession.builder.appName("SkewSalting").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# Skewデータ抽出
df_sales_skew = spark.table("sales").filter(F.col("product_id") == "PRODUCT_SKEW")
df_products_skew = spark.table("products").filter(F.col("product_id") == "PRODUCT_SKEW")

# ---------------------------------------------------------
# Manual Salting Implementation
# ---------------------------------------------------------
print("\n=== Manual Salting Comparison (Sales x Products) ===")
print("【Spark UI チェックポイント】")
print("1. Stagesタブ > 対象ステージの詳細を開く")
print("2. 'Task Execution Time' のヒストグラム(分布)を見る")
print("   - Median(中央値) と Max(最大値) の乖離を確認する。")
print("   - Saltingなし (AQE OFF): Max だけ極端に長いバーが出る (Straggler)")
print("   - Saltingあり: Max が下がり、全体が均される")

def run_salted_join():
    # 1. Fact側: Salt付与 (0~9)
    df_sales_salted = df_sales_skew.withColumn("salt", (F.rand() * SALT_FACTOR).cast("int"))

    # 2. Dim側: Explode (1行を10行に増やす)
    # create array [0, 1, ..., 9] then explode
    df_products_salted = df_products_skew.withColumn("salt_gen", F.array([F.lit(i) for i in range(SALT_FACTOR)])) \
        .withColumn("salt", F.explode("salt_gen")) \
        .drop("salt_gen")

    # 3. Join on Key + Salt
    # これにより 'PRODUCT_SKEW' が10個のタスクに分散される
    return df_sales_salted.join(df_products_salted, ["product_id", "salt"]).count()

# AQE OFF で効果確認
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

print(f"Running Salted Join (Factor={SALT_FACTOR})...")
measure_time("Salted Join", run_salted_join)

# 戻す
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.stop()
