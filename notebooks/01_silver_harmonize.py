# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Silver: Harmonize Multi-Source Retail Data
# MAGIC
# MAGIC Three source systems have different column names, date formats, and field types.
# MAGIC This notebook standardizes all into a single conformed Silver schema.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper, lower,
    coalesce, lit, current_timestamp, round as spark_round,
    regexp_replace
)
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.appName("RetailETL_Silver").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load POS (CSV)

# COMMAND ----------

pos_raw = spark.read.option("header", "true").csv(f"{BRONZE}/pos_sales/")

pos_clean = (
    pos_raw
    .withColumnRenamed("order_id",    "order_id")
    .withColumnRenamed("txn_date",    "_raw_date")
    .withColumn("order_date",         to_date(col("_raw_date"), "dd/MM/yyyy"))
    .withColumn("customer_id",        upper(trim(col("customer_id"))))
    .withColumn("product_id",         upper(trim(col("product_id"))))
    .withColumn("quantity",           col("quantity").cast(IntegerType()))
    .withColumn("unit_price",         col("unit_price").cast(DoubleType()))
    .withColumn("discount_pct",       col("discount_pct").cast(DoubleType()))
    .withColumn("net_revenue",        spark_round(
                                          col("quantity") * col("unit_price") * (lit(1) - col("discount_pct")), 2))
    .withColumn("channel",            lit("In-Store"))
    .withColumn("source_system",      lit("POS"))
    .withColumn("_processed_at",      current_timestamp())
    .select("order_id", "order_date", "store_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct", "net_revenue", "channel", "source_system", "_processed_at")
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .dropDuplicates(["order_id"])
)

print(f"POS clean: {pos_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load E-Commerce (JSON)

# COMMAND ----------

ecom_raw = spark.read.json(f"{BRONZE}/ecommerce_orders/")

ecom_clean = (
    ecom_raw
    .withColumnRenamed("OrderID",        "order_id")
    .withColumnRenamed("CustomerRef",    "customer_id")
    .withColumnRenamed("ProductSKU",     "product_id")
    .withColumnRenamed("Qty",            "quantity")
    .withColumnRenamed("ListPrice",      "unit_price")
    .withColumnRenamed("CouponDiscount", "discount_pct")
    .withColumnRenamed("DeliveryRegion", "store_id")
    .withColumn("order_date",            to_date(col("OrderDate")))
    .withColumn("customer_id",           upper(trim(col("customer_id"))))
    .withColumn("product_id",            upper(trim(col("product_id"))))
    .withColumn("quantity",              col("quantity").cast(IntegerType()))
    .withColumn("unit_price",            col("unit_price").cast(DoubleType()))
    .withColumn("discount_pct",          col("discount_pct").cast(DoubleType()))
    .withColumn("net_revenue",           spark_round(
                                             col("quantity") * col("unit_price") * (lit(1) - col("discount_pct")), 2))
    .withColumn("channel",               lit("Online"))
    .withColumn("source_system",         lit("ECOMMERCE"))
    .withColumn("_processed_at",         current_timestamp())
    .select("order_id", "order_date", "store_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct", "net_revenue", "channel", "source_system", "_processed_at")
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .dropDuplicates(["order_id"])
)

print(f"E-Commerce clean: {ecom_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union All Sources & Write Silver

# COMMAND ----------

silver_df = pos_clean.unionByName(ecom_clean)
print(f"Total Silver records: {silver_df.count()}")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date", "source_system")
    .save(f"{SILVER}/conformed_sales/")
)

print("Silver write complete.")
silver_df.show(5, truncate=False)
