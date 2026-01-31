# Databricks Tuning Guide: 99_Practice_Problems
#
# ==============================================================================
# 実践練習問題: Sparkチューニング・ドリル (全6問)
# ==============================================================================
# これまでの章で学んだ知識を使って、遅いクエリを高速化してください。
#
# 前提:
#   事前に `src/99_practice_data_generation.py` を実行して、
#   練習用データを準備してください。
#
# ルール:
# 1. 各「課題」のセルを実行し、現状のパフォーマンス(実行時間)を確認する。
# 2. ヒントを参考に、コードを修正して高速化する。
# 3. 再実行して、どれくらい速くなったか確認する。
#
# ※ 模範解答は `src/99_practice_problems_solutions.py` にあります。
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("PracticeProblems").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

# ---------------------------------------------------------
# 課題 1: 「日次レポートが遅すぎる！」 (Storage)
# ---------------------------------------------------------
print("\n=== Challenge 1: Daily Report Optimization ===")
print("シナリオ: 特定の日付 ('2024-01-01') の売上を集計したいが、標準テーブルだと全ファイルスキャンが走る。")
# Target: practice_sales_unoptimized (最適化なし)

def task1_slow():
    return spark.sql("SELECT sum(amount) FROM practice_sales_unoptimized WHERE txn_date = '2024-01-01'").collect()

measure_time("Slow Query", task1_slow)

# --- [あなたの解答欄] ---
# ヒント: Data Skippingを効かせるテーブルを作成してください。
def task1_solution():
    pass
    # spark.sql("CREATE TABLE ... CLUSTER BY ...")
    # spark.sql("SELECT ... FROM solution_table ...")


# ---------------------------------------------------------
# 課題 2: 「爆売れ商品の集計が終わらない！」 (Skew Join)
# ---------------------------------------------------------
print("\n=== Challenge 2: Skew Join Fixing ===")
print("シナリオ: 特定商品(P_0)に売上が集中しており、結合処理がスタックする。AQE Skew Joinが無効化されている。")

df_sales_skew = spark.table("practice_sales_skew").filter(F.col("product_id") == "P_0")
df_products = spark.table("practice_products").filter(F.col("product_id") == "P_0")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false") # 設定変更禁止

def task2_slow():
    return df_sales_skew.join(df_products, "product_id").count()

print("Running Slow Join (Might take a while)...")
# measure_time("Slow Join", task2_slow)

# --- [あなたの解答欄] ---
# ヒント: AQEを有効にするか、手動でSaltingを行う。(今回はAQE有効化でOK)
def task2_solution():
    pass
    # spark.conf.set(...)
    # return ...


# ---------------------------------------------------------
# 課題 3: 「消費税計算ごときでなぜ待たされる？」 (UDF)
# ---------------------------------------------------------
print("\n=== Challenge 3: Remove Python UDF ===")
print("シナリオ: 単純な 1.1倍 の計算に Python UDF が使われており、Photonが機能していない。")

df_sales = spark.table("practice_sales_unoptimized").select("amount")
@F.udf("double")
def tax_calc_slow(amount):
    return amount * 1.1

def task3_slow():
    return df_sales.withColumn("tax", tax_calc_slow(F.col("amount"))).count()

measure_time("Slow UDF", task3_slow)

# --- [あなたの解答欄] ---
# ヒント: Python UDF を Spark Native な記述に書き換える。
def task3_solution():
    pass
    # return df_sales.withColumn(...)


# ---------------------------------------------------------
# 課題 4: 「Shuffleしすぎ問題」 (Repartition)
# ---------------------------------------------------------
print("\n=== Challenge 4: Excessive Shuffle ===")
print("シナリオ: データを10パーティションに減らしたいだけなのに、repartitionを使って全Shuffleが発生している。")

df_sales = spark.table("practice_sales_unoptimized")

def task4_slow():
    # 全データをNetwork経由でShuffleしてから10個にまとめている
    return df_sales.repartition(10).count()

print("Running Repartition (Slow)...")
measure_time("Repartition(10)", task4_slow)

# --- [あなたの解答欄] ---
# ヒント: Shuffleせずにパーティションを減らすメソッドは？
def task4_solution():
    pass
    # return df_sales....


# ---------------------------------------------------------
# 課題 5: 「マスタ結合なのにSortしている」 (Join Strategy)
# ---------------------------------------------------------
print("\n=== Challenge 5: Inefficient Join Strategy ===")
print("シナリオ: 商品マスタ(practice_products)は小さい(5000件)のに、Broadcastされずに重いSortMergeJoinが走っている。")

df_sales = spark.table("practice_sales_unoptimized")
df_prod = spark.table("practice_products")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # Broadcast無効化中

def task5_slow():
    # Hint: Merge Join is happening
    return df_sales.join(df_prod, "product_id").count()

print("Running Sort Merge Join (Slow)...")
measure_time("SortMergeJoin", task5_slow)

# --- [あなたの解答欄] ---
# ヒント: Broadcastを強制的に有効化するには？ (Global Config または Hint)
def task5_solution():
    pass
    # return df_sales.join(..., "product_id").count()


# ---------------------------------------------------------
# 課題 6: 「同じ計算を2回するな」 (Cache)
# ---------------------------------------------------------
print("\n=== Challenge 6: Wasteful Re-computation ===")
print("シナリオ: 重いフィルタ処理をしたDataFrameに対して、count() と collect() を別々に呼んでいる。")
print("その結果、重いフィルタ処理が2回走ってしまっている。")

def task6_slow():
    # 重い処理 (ここでは単純にScan)
    df_heavy = spark.table("practice_sales_unoptimized").filter("amount > 5000")
    
    # Action 1
    c = df_heavy.count()
    # Action 2 (ここでまた最初から計算し直しになる)
    r = df_heavy.limit(5).collect()
    return c, r

print("Running Non-Cached Flow (Slow)...")
measure_time("No Cache", task6_slow)

# --- [あなたの解答欄] ---
# ヒント: 1回目の計算結果をメモリまたはディスクに保持するには？
def task6_solution():
    pass
    # df_heavy = ...
    # df_heavy.cache() ...


print("\n--- End of Practice Problems ---")
print("解答は `src/99_practice_problems_solutions.py` を確認してください。")
spark.stop()
