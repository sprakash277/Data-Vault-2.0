# Databricks notebook source
# DBTITLE 1,Bronze to Raw Data Vault
# MAGIC %python
# MAGIC ######################  hub_product  ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_product"
# MAGIC dest_table_name = "main.dv2_0.hub_product"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("product_id")))).alias("sha1_hub_product"),
# MAGIC             col("product_id").alias("product_id"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Product").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true") 
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ######################  sat_product ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_product"
# MAGIC dest_table_name = "main.dv2_0.sat_product"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("product_id")))).alias("sha1_hub_product"),
# MAGIC             col("type").alias("type"),
# MAGIC             col("SKU").alias("SKU"),
# MAGIC             col("name").alias("name"),
# MAGIC             col("description").alias("description"),
# MAGIC             col("sale_price").alias("sale_price"),
# MAGIC             col("regular_price").alias("regular_price"),
# MAGIC             col("start_at").alias("start_at"),
# MAGIC             sha1(concat(upper(trim(col("type"))),upper(trim(col("SKU"))),upper(trim(col("name")))
# MAGIC                   ,upper(trim(col("sale_price"))),upper(trim(col("regular_price"))))).alias("hash_diff"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Product").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true") 
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )
