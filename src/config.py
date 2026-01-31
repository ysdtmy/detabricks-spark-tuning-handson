# Databricks notebook source
# -------------------------------------------------------------------------
# Common Configuration for Spark Tuning Guide
# -------------------------------------------------------------------------

# Catalog & Schema
CATALOG_NAME = "spark_tuning_handson"
SCHEMA_NAME = "default"

# Main Verification Tables
TBL_PRODUCTS = "products"
TBL_SALES = "sales"
TBL_SALES_SMALL = "sales_small_files"

# Practice (Dojo) Tables
TBL_PRACTICE_PRODUCTS = "practice_products"
TBL_PRACTICE_SALES_SKEW = "practice_sales_skew"
TBL_PRACTICE_SALES_UNOPT = "practice_sales_unoptimized"

# Constants
NUM_SALES_ROWS = 50_000_000
NUM_PRODUCTS_ROWS = 10_000
