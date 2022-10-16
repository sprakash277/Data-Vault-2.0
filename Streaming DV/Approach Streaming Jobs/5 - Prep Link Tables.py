# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC ######################  LNK PRODUCT  SALES ########################
# MAGIC 
# MAGIC            
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_fact_sales"
# MAGIC dest_table_name = "main.dv2_0.lnk_product_sales"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(concat(upper(trim(col("transaction_id"))),upper(trim(col("product_id"))))).alias("sha1_lnk_product_sales"),
# MAGIC             sha1(upper(trim(col("transaction_id")))).alias("sha1_hub_sales_id"),
# MAGIC             sha1(upper(trim(col("product_id")))).alias("sha1_hub_product"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Sales & Product").alias("source")
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
# MAGIC ######################  LNK CUSTOMER SALES ########################
# MAGIC 
# MAGIC            
# MAGIC from pyspark.sql.functions  import *
# MAGIC 
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC src_table_name = "main.dv2_0.raw_fact_sales"
# MAGIC dest_table_name = "main.dv2_0.lnk_customer_sales"
# MAGIC checkpoint_path = parent_path + src_table_name + "/e2-demo/CDF/checkpoint/" + dest_table_name 
# MAGIC 
# MAGIC (spark.readStream.format("delta") 
# MAGIC         .option("readChangeFeed", "true") 
# MAGIC         .option("startingVersion", 0) 
# MAGIC         .table(src_table_name) 
# MAGIC         .select(
# MAGIC             sha1(concat(upper(trim(col("transaction_id"))),upper(trim(col("customer_id"))))).alias("sha1_lnk_customer_sales"),
# MAGIC             sha1(upper(trim(col("transaction_id")))).alias("sha1_hub_sales_id"),
# MAGIC             sha1(upper(trim(col("customer_id")))).alias("sha1_hub_customer"),
# MAGIC             current_timestamp().alias("load_ts"),
# MAGIC             lit("Customer & Sales").alias("source")
# MAGIC         )
# MAGIC     .writeStream.format("delta") 
# MAGIC         .option("checkpointLocation", checkpoint_path) 
# MAGIC         .option("mergeSchema", "true") 
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(dest_table_name) 
# MAGIC )
