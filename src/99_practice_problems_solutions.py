# Databricks notebook source
# MAGIC %md
# MAGIC # 99_Practice_Problems_Solutions
# MAGIC
# MAGIC 練習問題(Dojo)の模範解答集です。

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 Answer: Liquid Clustering / Z-Order
# MAGIC
# MAGIC * `clusteringColumns` を使うことでData Skippingを有効化します。

# COMMAND ----------

def task1_solution(spark):
    spark.table(config.TBL_PRACTICE_SALES_UNOPT).write.format("delta") \
        .option("clusteringColumns", "txn_date").mode("overwrite").saveAsTable("practice_sales_liquid")
    
    return spark.sql("SELECT sum(amount) FROM practice_sales_liquid WHERE txn_date = '2024-01-01'").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 Answer: AQE Skew Join
# MAGIC * `spark.sql.adaptive.skewJoin.enabled = true` で自動解決。

# COMMAND ----------

def task2_solution(df_sales_skew, df_products_skew, spark):
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    return df_sales_skew.join(df_products_skew, "product_id").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 Answer: Native Expressions
# MAGIC * Python関数を使わず、Spark Column式を使うことでPhotonが有効になります。

# COMMAND ----------

def task3_solution(df_sales):
    return df_sales.withColumn("tax", F.col("amount") * 1.1).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 Answer: Coalesce
# MAGIC * Partitionを減らすだけなら `coalesce` が圧倒的に低コストです。

# COMMAND ----------

def task4_solution(df_sales):
    return df_sales.coalesce(10).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5 Answer: Broadcast Hint
# MAGIC * 設定で無効化されていても `broadcast()` ヒントで強制できます。

# COMMAND ----------

def task5_solution(df_sales, df_products):
    return df_sales.join(F.broadcast(df_products), "product_id").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6 Answer: Cache
# MAGIC * 複数Action呼ぶ前に `.cache()` しましょう。

# COMMAND ----------

def task6_solution(spark):
    df_heavy = spark.table(config.TBL_PRACTICE_SALES_UNOPT).filter("amount > 5000")
    df_heavy.cache()
    
    c = df_heavy.count()
    r = df_heavy.limit(5).collect()
    
    df_heavy.unpersist()
    return c, r
