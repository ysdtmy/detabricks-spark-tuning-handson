# Databricks Tuning Guide: 99_Practice_Problems_Solutions
#
# ==============================================================================
# 実践練習問題: 模範解答集
# ==============================================================================
# `src/99_practice_problems.py` の解答です。
# data: practice_sales_unoptimized, practice_sales_skew, practice_products
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------
# Challenge 1: Daily Report Optimization
# ---------------------------------------------------------
def task1_solution(spark):
    print("Answer 1: Use Liquid Clustering or Z-Order")
    # Liquid Clustering (Recommended)
    spark.table("practice_sales_unoptimized").write.format("delta") \
        .option("clusteringColumns", "txn_date").mode("overwrite").saveAsTable("practice_sales_liquid")
    
    return spark.sql("SELECT sum(amount) FROM practice_sales_liquid WHERE txn_date = '2024-01-01'").collect()

# ---------------------------------------------------------
# Challenge 2: Skew Join Fixing
# ---------------------------------------------------------
def task2_solution(df_sales_skew, df_products_skew, spark):
    print("Answer 2: Enable AQE Skew Join")
    # 最も簡単な解決策はAQEを有効にすることです
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # 必要に応じてAdvisoryサイズも調整
    # spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
    return df_sales_skew.join(df_products_skew, "product_id").count()

# ---------------------------------------------------------
# Challenge 3: Remove Python UDF
# ---------------------------------------------------------
def task3_solution(df_sales):
    print("Answer 3: Use Native Spark Expressions")
    # withColumnの中で直接計算式を書けば、Photonが最適化してくれます
    return df_sales.withColumn("tax", F.col("amount") * 1.1).count()

# ---------------------------------------------------------
# Challenge 4: Excessive Shuffle (Repartition)
# ---------------------------------------------------------
def task4_solution(df_sales):
    print("Answer 4: Use coalesce() instead of repartition()")
    return df_sales.coalesce(10).count()

# ---------------------------------------------------------
# Challenge 5: Inefficient Join Strategy
# ---------------------------------------------------------
def task5_solution(df_sales, df_products):
    print("Answer 5: Force Broadcast")
    # Dimテーブルが小さいことがわかっているなら、broadcastヒントを使います
    return df_sales.join(F.broadcast(df_products), "product_id").count()

# ---------------------------------------------------------
# Challenge 6: Wasteful Re-computation (Cache)
# ---------------------------------------------------------
def task6_solution(spark):
    print("Answer 6: Cache the DataFrame")
    df_heavy = spark.table("practice_sales_unoptimized").filter("amount > 5000")
    
    # 明示的にCache
    df_heavy.cache()
    
    # 1回目のAction (ここでCacheされる)
    c = df_heavy.count()
    # 2回目のAction (Cacheから読むので爆速)
    r = df_heavy.limit(5).collect()
    
    df_heavy.unpersist()
    return c, r
