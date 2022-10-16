# Databricks notebook source
# DBTITLE 1,Bronze to Raw Data Vault
# MAGIC %python
# MAGIC ######################  hub_Customer  ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_customer"
# MAGIC dest_table_name = "main.dv2_0.hub_customer"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("customer_id")))).alias("sha1_hub_customer"),
# MAGIC             col("customer_id").alias("customer_id"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Customer").alias("source")
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
# MAGIC ######################  sat_Customer ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_dim_customer"
# MAGIC dest_table_name = "main.dv2_0.sat_customer"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("customer_id")))).alias("sha1_hub_customer"),
# MAGIC             col("name").alias("name"),
# MAGIC             col("email").alias("email"),
# MAGIC             col("address").alias("address"),
# MAGIC             col("start_at").alias("start_at"),
# MAGIC             sha1(concat(upper(trim(col("name"))),upper(trim(col("email"))),upper(trim(col("address"))))).alias("hash_diff"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Customer").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true") 
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )
