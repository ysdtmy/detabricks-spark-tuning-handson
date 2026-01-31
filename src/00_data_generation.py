# Databricks Tuning Guide: 00_Data_Generation (Retail Scenario)
#
# ==============================================================================
# 解説: 業務データ(小売)を模した検証データの生成
# ==============================================================================
# これまでのチューニング検証を、より実践的な「売上 (Sales)」と「商品 (Products)」
# のデータモデルで行います。
#
# 作成されるテーブル (Catalog: main, Schema: tuning_guide):
# 1. products (商品マスタ) - Dimension Table
#    - 約 10,000 商品
#    - columns: product_id, product_name, category, price
#
# 2. sales (売上トランザクション) - Fact Table
#    - 約 10,000,000 レコード
#    - columns: txn_id, txn_date, customer_id, product_id, quantity, amount
#    - **Skew**: 特定のヒット商品 ('PRODUCT_SKEW') に注文が集中するよう生成します。
#      (Skew Joinの検証用)
#
# 3. sales_small_files (小ファイル用)
#    - 小ファイル問題を再現するために細切れに保存された売上データ
#
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import random

# 設定
CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"
NUM_PRODUCTS = 10_000
NUM_SALES = 10_000_000

spark = SparkSession.builder.appName("DataGeneration_Retail").getOrCreate()

# Schema Setup
print(f"Setting up database: {CATALOG_NAME}.{SCHEMA_NAME}...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

# ---------------------------------------------------------
# 1. Products (商品マスタ) 生成
# ---------------------------------------------------------
print("\n--- Generating 'products' (Dimension Table) ---")

# ランダムな商品カテゴリ
categories = ["Electronics", "Clothing", "Home", "Books", "Beauty"]

# DataFrame作成
# id: 0 ~ 9999
# Skew検証用に 'PRODUCT_SKEW' というIDを一つ混ぜます (ID: 0 をマッピング)
df_products = spark.range(0, NUM_PRODUCTS).withColumn("product_id", 
    F.when(F.col("id") == 0, F.lit("PRODUCT_SKEW"))
     .otherwise(F.concat(F.lit("PROD_"), F.col("id").cast("string")))
).withColumn("product_name", F.concat(F.lit("Product Name "), F.col("id"))) \
 .withColumn("category", F.lit(categories)[(F.rand() * len(categories)).cast("int")]) \
 .withColumn("price", (F.rand() * 10000).cast("int")) \
 .drop("id")

df_products.write.format("delta").mode("overwrite").saveAsTable("products")
print(f"✅ Saved 'products' ({NUM_PRODUCTS} rows).")


# ---------------------------------------------------------
# 2. Sales (売上) 生成 - Skewあり
# ---------------------------------------------------------
print("\n--- Generating 'sales' (Fact Table) with Skew ---")

# 90% の売上は 'PRODUCT_SKEW' (爆売れ商品)
df_skew_sales = spark.range(0, int(NUM_SALES * 0.9)) \
    .withColumn("product_id", F.lit("PRODUCT_SKEW"))

# 10% の売上は ランダムな商品 (PROD_1 ~ PROD_9999)
# ※ PROD_0 は SKEW なので除外したいが、簡易的に 1~9999 を生成
df_normal_sales = spark.range(0, int(NUM_SALES * 0.1)) \
    .withColumn("product_id", F.concat(F.lit("PROD_"), (1 + (F.rand() * (NUM_PRODUCTS - 1))).cast("int").cast("string")))

# 合体して共通カラム付与
df_sales = df_skew_sales.union(df_normal_sales) \
    .withColumn("txn_id", F.expr("uuid()")) \
    .withColumn("txn_date", F.date_add(F.lit("2024-01-01"), (F.rand() * 365).cast("int"))) \
    .withColumn("customer_id", (F.rand() * 100000).cast("int")) \
    .withColumn("quantity", (F.rand() * 10).cast("int") + 1) \
    .withColumn("amount", F.rand() * 50000) \
    .withColumn("payload", F.expr("repeat('LogData', 50)")) # データ容量稼ぎ

# 保存
df_sales.write.format("delta").mode("overwrite").saveAsTable("sales")
print(f"✅ Saved 'sales' ({NUM_SALES} rows). Contains Skew on 'PRODUCT_SKEW'.")


# ---------------------------------------------------------
# 3. Sales Small Files (小ファイル問題用)
# ---------------------------------------------------------
print("\n--- Generating 'sales_small_files' ---")
# 10万件を1000ファイルに分割 (1ファイル100件)
spark.table("sales").sample(0.01).repartition(1000) \
    .write.format("delta").mode("overwrite").saveAsTable("sales_small_files")
print("✅ Saved 'sales_small_files'.")

print("\nAll retail data generation completed.")
