# Databricks notebook source
# DBTITLE 1,Source to Bronze Layer
# MAGIC %python
# MAGIC 
# MAGIC ############################## RAW SALES FEED #########################
# MAGIC 
# MAGIC schema_hint = """
# MAGIC               transaction_id BIGINT,
# MAGIC               date_id BIGINT ,
# MAGIC               customer_id BIGINT ,
# MAGIC               product_id BIGINT ,
# MAGIC               store_id BIGINT ,
# MAGIC               store_business_key STRING,
# MAGIC               sales_amount DOUBLE
# MAGIC 
# MAGIC """
# MAGIC src_table_name = "fact_sales"
# MAGIC parent_path = "abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/"
# MAGIC schema_checkpoint_file_path = parent_path + src_table_name + "/e2-demo/ingestion/" + src_table_name 
# MAGIC dest_table_name = "main.dv2_0.raw_fact_sales"
# MAGIC (
# MAGIC    spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.schemaLocation", schema_checkpoint_file_path+"/schema")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("header", "false")
# MAGIC         .schema(schema_hint)
# MAGIC         .load(parent_path+"/Tables/"+src_table_name)
# MAGIC         .writeStream.format("delta")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .option("checkpointLocation",schema_checkpoint_file_path+"/checkpoint")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable("main.dv2_0.raw_fact_sales")        
# MAGIC )
