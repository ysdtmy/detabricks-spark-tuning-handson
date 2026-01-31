# Databricks notebook source
# MAGIC %md
# MAGIC # 99_Practice_Problems (Dojo)
# MAGIC
# MAGIC **実践練習問題: Sparkチューニング・ドリル (全6問)**
# MAGIC
# MAGIC 遅いクエリをチューニングして高速化してください。
# MAGIC 解答は `99_practice_problems_solutions` を参照。

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 1: Daily Report Optimization
# MAGIC Data Skippingが効いていないフルスキャンクエリを高速化してください。
# MAGIC Target: `practice_sales_unoptimized`

# COMMAND ----------

print("\n=== Challenge 1: Daily Report Optimization ===")
# Slow Code
def task1_slow():
    return spark.sql("SELECT sum(amount) FROM practice_sales_unoptimized WHERE txn_date = '2024-01-01'").collect()

measure_time("Slow Query", task1_slow)

# --- [あなたの解答欄] ---
def task1_solution():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 2: Skew Join Fixing
# MAGIC AQE Skew Joinが無効化された状況で、結合を完了させてください。

# COMMAND ----------

print("\n=== Challenge 2: Skew Join Fixing ===")

df_sales_skew = spark.table("practice_sales_skew").filter(F.col("product_id") == "P_0")
df_products = spark.table("practice_products").filter(F.col("product_id") == "P_0")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false") # Restriction

def task2_slow():
    return df_sales_skew.join(df_products, "product_id").count()

print("Running Slow Join (Might take a while)...")
# measure_time("Slow Join", task2_slow)

# --- [あなたの解答欄] ---
def task2_solution():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 3: Remove Python UDF
# MAGIC Python UDFをNative関数に置き換えてPhotonを有効化してください。

# COMMAND ----------

print("\n=== Challenge 3: Remove Python UDF ===")

df_sales = spark.table("practice_sales_unoptimized").select("amount")
@F.udf("double")
def tax_calc_slow(amount):
    return amount * 1.1

def task3_slow():
    return df_sales.withColumn("tax", tax_calc_slow(F.col("amount"))).count()

measure_time("Slow UDF", task3_slow)

# --- [あなたの解答欄] ---
def task3_solution():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 4: Excessive Shuffle
# MAGIC `repartition` を `coalesce` に置き換えてShuffleを回避してください。

# COMMAND ----------

print("\n=== Challenge 4: Excessive Shuffle ===")
df_sales = spark.table("practice_sales_unoptimized")

def task4_slow():
    return df_sales.repartition(10).count()

print("Running Repartition (Slow)...")
measure_time("Repartition(10)", task4_slow)

# --- [あなたの解答欄] ---
def task4_solution():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 5: Inefficient Join Strategy
# MAGIC Broadcastされるべき小さなテーブルがBroadcastされていない問題を修正してください。

# COMMAND ----------

print("\n=== Challenge 5: Inefficient Join Strategy ===")
df_sales = spark.table("practice_sales_unoptimized")
df_prod = spark.table("practice_products")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def task5_slow():
    return df_sales.join(df_prod, "product_id").count()

print("Running Sort Merge Join (Slow)...")
measure_time("SortMergeJoin", task5_slow)

# --- [あなたの解答欄] ---
def task5_solution():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 6: Wasteful Re-computation
# MAGIC Cacheを使って重いフィルタ処理の重複実行を防いでください。

# COMMAND ----------

print("\n=== Challenge 6: Wasteful Re-computation ===")

def task6_slow():
    df_heavy = spark.table("practice_sales_unoptimized").filter("amount > 5000")
    c = df_heavy.count()
    r = df_heavy.limit(5).collect()
    return c, r

print("Running Non-Cached Flow (Slow)...")
measure_time("No Cache", task6_slow)

# --- [あなたの解答欄] ---
def task6_solution():
    pass
