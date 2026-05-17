# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Gold: fact_sales Incremental Load
# MAGIC
# MAGIC Joins Silver conformed sales with dimension surrogate keys
# MAGIC and appends only new order_ids to fact_sales.
# MAGIC Implements incremental load using Delta table audit column.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, year, month, dayofmonth, lit
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("RetailETL_FactSales").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
SILVER    = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD      = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SILVER_SALES    = f"{SILVER}/conformed_sales/"
DIM_PRODUCT     = f"{GOLD}/dim_product/"
FACT_SALES_PATH = f"{GOLD}/fact_sales/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Sales

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_SALES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Filter – Only New Orders

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, FACT_SALES_PATH):
    existing_ids = (
        spark.read.format("delta").load(FACT_SALES_PATH)
        .select("order_id")
    )
    new_df = silver_df.join(existing_ids, on="order_id", how="left_anti")
    print(f"New orders to load: {new_df.count()}")
else:
    new_df = silver_df
    print(f"Initial load. Orders: {new_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Dimension: Product (current records only)

# COMMAND ----------

dim_product = (
    spark.read.format("delta").load(DIM_PRODUCT)
    .filter(col("is_current") == True)
    .select(
        col("product_id"),
        col("category").alias("product_category"),
        col("brand").alias("product_brand")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact Table

# COMMAND ----------

fact_df = (
    new_df
    .join(dim_product, on="product_id", how="left")
    .withColumn("year",             year(col("order_date")))
    .withColumn("month",            month(col("order_date")))
    .withColumn("day",              dayofmonth(col("order_date")))
    .withColumn("_fact_loaded_at",  current_timestamp())
    .select(
        "order_id", "order_date", "year", "month", "day",
        "customer_id", "product_id", "product_category", "product_brand",
        "store_id", "channel", "source_system",
        "quantity", "unit_price", "discount_pct", "net_revenue",
        "_fact_loaded_at"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to fact_sales

# COMMAND ----------

(
    fact_df.write
    .format("delta")
    .mode("append")
    .partitionBy("year", "month")
    .save(FACT_SALES_PATH)
)

print(f"Loaded {fact_df.count()} records into fact_sales.")
fact_df.show(5, truncate=False)
