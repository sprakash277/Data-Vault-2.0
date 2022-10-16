-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC spark.readStream.format("delta") \
-- MAGIC   .option("ignoreDeletes", "true") \
-- MAGIC   .option("startingVersion", 0) \
-- MAGIC   .table("main.data_vault_model.dim_store")\
-- MAGIC   .writeStream.format("json")\
-- MAGIC     .option("checkpointLocation","abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/e2-demo/checkpoint/dim_store/")\
-- MAGIC     .start("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_store/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.format("delta") \
-- MAGIC   .option("ignoreDeletes", "true") \
-- MAGIC   .option("startingVersion", 0) \
-- MAGIC   .table("main.data_vault_model.dim_product")\
-- MAGIC   .writeStream.format("json")\
-- MAGIC     .option("checkpointLocation","abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/e2-demo/checkpoint/dim_product")\
-- MAGIC     .start("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_product/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.format("delta") \
-- MAGIC   .option("ignoreDeletes", "true") \
-- MAGIC   .option("startingVersion", 0) \
-- MAGIC   .table("main.data_vault_model.dim_customer")\
-- MAGIC   .writeStream.format("json")\
-- MAGIC     .option("checkpointLocation","abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/e2-demo/checkpoint/dim_customer/")\
-- MAGIC     .start("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_customer/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.format("delta") \
-- MAGIC   .option("ignoreDeletes", "true") \
-- MAGIC   .option("startingVersion", 0) \
-- MAGIC   .table("main.data_vault_model.dim_date")\
-- MAGIC   .writeStream.format("csv")\
-- MAGIC     .option("checkpointLocation","abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/e2-demo/checkpoint/dim_date/")\
-- MAGIC     .start("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_date/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.format("delta") \
-- MAGIC   .option("ignoreDeletes", "true") \
-- MAGIC   .option("startingVersion", 0) \
-- MAGIC   .table("main.data_vault_model.fact_sales")\
-- MAGIC   .writeStream.format("csv")\
-- MAGIC     .option("checkpointLocation","abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/e2-demo/checkpoint/fact_sales")\
-- MAGIC     .start("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/fact_sales/")
