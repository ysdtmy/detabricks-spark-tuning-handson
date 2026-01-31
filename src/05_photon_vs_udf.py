# Databricks notebook source
# MAGIC %md
# MAGIC # 05_Photon_vs_UDF
# MAGIC
# MAGIC * **Topic**: Native Functions vs Python UDF vs Pandas UDF.
# MAGIC * **Goal**: Avoid `BatchEvalPython` (Slow UDFs).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd
import time

CATALOG_NAME = "main"
SCHEMA_NAME = "tuning_guide"

spark = SparkSession.builder.appName("PhotonVsUDF").getOrCreate()
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

def measure_time(query_desc, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"[{query_desc}] Duration: {end - start:.4f} sec")
    return result

df = spark.table("sales").select("txn_id", "amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark
# MAGIC
# MAGIC **【Spark UI Check】**
# MAGIC * **Native**: `PhotonProject` (Fast)
# MAGIC * **Python UDF**: `BatchEvalPython` (Slow, high overhead)
# MAGIC * **Pandas UDF**: `ArrowEvalPython` (Medium)

# COMMAND ----------

# 1. Native Spark Function (Recommended)
print("\n=== 1. Native Spark Function (Tax Calc) ===")
def run_native():
    return df.withColumn("amount_tax", F.col("amount") * 1.1).count()

measure_time("Native (* 1.1)", run_native)

# COMMAND ----------

# 2. Python UDF (Avoid)
print("\n=== 2. Python UDF ===")
@F.udf(DoubleType())
def add_tax_udf(amount):
    if amount is None: return 0.0
    return amount * 1.1

def run_python_udf():
    return df.withColumn("amount_tax", add_tax_udf(F.col("amount"))).count()

measure_time("Python UDF", run_python_udf)

# COMMAND ----------

# 3. Pandas UDF (Better)
print("\n=== 3. Pandas UDF ===")
@F.pandas_udf(DoubleType())
def add_tax_pandas(s: pd.Series) -> pd.Series:
    return s * 1.1

def run_pandas_udf():
    return df.withColumn("amount_tax", add_tax_pandas(F.col("amount"))).count()

measure_time("Pandas UDF", run_pandas_udf)
