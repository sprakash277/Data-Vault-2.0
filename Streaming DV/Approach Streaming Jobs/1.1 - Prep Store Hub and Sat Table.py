# Databricks notebook source
# DBTITLE 1,Bronze to Raw Data Vault
# MAGIC %python
# MAGIC 
# MAGIC ######################  hub_store  ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_store"
# MAGIC dest_table_name = "main.dv2_0.hub_store"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("store_id")))).alias("sha1_hub_store"),
# MAGIC             col("store_id").alias("store_id"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Store").alias("source")
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
# MAGIC ######################  sat_store  ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_store"
# MAGIC dest_table_name = "main.dv2_0.sat_store"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("store_id")))).alias("sha1_hub_store"),
# MAGIC             col("business_key").alias("business_key"),
# MAGIC             col("name").alias("name"),
# MAGIC             col("email").alias("email"),
# MAGIC             col("city").alias("city"),
# MAGIC             col("address").alias("address"),
# MAGIC             col("phone_number").alias("phone_number"),
# MAGIC             col("start_at").alias("start_at"),
# MAGIC             sha1(concat(upper(trim(col("business_key"))),upper(trim(col("name"))),upper(trim(col("email")))
# MAGIC                   ,upper(trim(col("address"))),upper(trim(col("phone_number"))))).alias("hash_diff"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Store").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )
