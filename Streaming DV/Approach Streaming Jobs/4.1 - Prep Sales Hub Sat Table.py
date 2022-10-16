# Databricks notebook source
# DBTITLE 1,Bronze to Raw Data Vault
# MAGIC %python
# MAGIC ######################  hub_Sales  ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_fact_sales"
# MAGIC dest_table_name = "main.dv2_0.hub_sales"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("transaction_id")))).alias("sha1_hub_sales_id"),
# MAGIC             col("transaction_id").alias("transaction_id"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Sales").alias("source")
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
# MAGIC ######################  sat_Sales ########################
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_fact_sales"
# MAGIC dest_table_name = "main.dv2_0.sat_sales"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(upper(trim(col("transaction_id")))).alias("sha1_hub_sales_id"),
# MAGIC             col("date_id").alias("sales_date"),
# MAGIC             col("sales_amount").alias("sales_amount"),
# MAGIC             col("store_business_key").alias("store_business_key"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Sales").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )
