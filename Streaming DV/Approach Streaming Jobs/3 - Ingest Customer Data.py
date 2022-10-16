# Databricks notebook source
# DBTITLE 1,Source to Bronze Layer
# MAGIC %python
# MAGIC 
# MAGIC ############################## RAW CUSTOMER FEED #########################
# MAGIC 
# MAGIC schema_hint = """
# MAGIC               customer_id BIGINT,
# MAGIC               name STRING,
# MAGIC               email STRING,
# MAGIC               address STRING,
# MAGIC               created_date TIMESTAMP,
# MAGIC               updated_date TIMESTAMP,
# MAGIC               start_at TIMESTAMP,
# MAGIC               end_at TIMESTAMP
# MAGIC 
# MAGIC """
# MAGIC src_table_name = "dim_customer"
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC schema_checkpoint_file_path = parent_path + src_table_name + "/e2-demo/ingestion/" + src_table_name 
# MAGIC dest_table_name = "main.dv2_0.raw_dim_customer"
# MAGIC (
# MAGIC    spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.schemaLocation", schema_checkpoint_file_path+"/schema")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.schemaHints", schema_hint)
# MAGIC         .load(parent_path+"/Tables/"+src_table_name)
# MAGIC         .writeStream.format("delta")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .option("checkpointLocation",schema_checkpoint_file_path+"/checkpoint")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable("main.dv2_0.raw_dim_customer")        
# MAGIC )
