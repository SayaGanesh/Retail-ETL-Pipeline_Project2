# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Gold: SCD Type 2 – dim_product
# MAGIC
# MAGIC Implements Slowly Changing Dimension Type 2 for product master.
# MAGIC When a product's price or category changes:
# MAGIC   - Old record is closed (end_date set, is_current = False)
# MAGIC   - New record inserted with new effective_date, is_current = True

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_date, lit, when, sha2, concat_ws
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("RetailETL_SCD2_Product").getOrCreate()

STORAGE_ACCOUNT = "yourstorageaccount"
BRONZE    = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/dim_product/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Incoming Product Master

# COMMAND ----------

incoming = (
    spark.read.parquet(f"{BRONZE}/product_master/")
    .withColumn("row_hash", sha2(concat_ws("|",
        col("product_name"), col("category"), col("brand"),
        col("list_price").cast("string")), 256))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 MERGE

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
    # First load – write all records as current
    (
        incoming
        .withColumn("effective_date", current_date())
        .withColumn("end_date",       lit(None).cast("date"))
        .withColumn("is_current",     lit(True))
        .write.format("delta").mode("overwrite").save(GOLD_PATH)
    )
    print("Initial dim_product load complete.")
else:
    dim_table = DeltaTable.forPath(spark, GOLD_PATH)

    # Step 1: Mark changed current records as expired
    (
        dim_table.alias("target")
        .merge(
            incoming.alias("source"),
            "target.product_id = source.product_id AND target.is_current = true"
        )
        .whenMatchedUpdate(
            condition="target.row_hash <> source.row_hash",
            set={
                "is_current": lit(False),
                "end_date":   current_date()
            }
        )
        .execute()
    )

    # Step 2: Insert new versions for changed records
    changed = (
        incoming.alias("source")
        .join(
            dim_table.toDF().filter(col("is_current") == False).alias("target"),
            (col("source.product_id") == col("target.product_id")) &
            (col("source.row_hash")   != col("target.row_hash")),
            "inner"
        )
        .select("source.*")
        .withColumn("effective_date", current_date())
        .withColumn("end_date",       lit(None).cast("date"))
        .withColumn("is_current",     lit(True))
    )

    changed.write.format("delta").mode("append").save(GOLD_PATH)
    print(f"SCD2 MERGE complete. {changed.count()} new versions inserted.")

# COMMAND ----------

spark.read.format("delta").load(GOLD_PATH).show(10, truncate=False)
