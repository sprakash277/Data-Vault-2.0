-- Databricks notebook source

CREATE OR REFRESH STREAMING LIVE TABLE raw_store (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "RAW Store data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS  SELECT 
      * 
    FROM 
      cloud_files("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_store", "json",
        map("cloudFiles.schemaHints", "store_id BIGINT ,business_key STRING,name STRING,email STRING,city STRING,address STRING,phone_number STRING,created_date TIMESTAMP,updated_date TIMESTAMP, start_at TIMESTAMP,end_at TIMESTAMP",
            "mergeSchema", "true"
           )
                 ) 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_product (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "RAW Product data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS  SELECT 
      * 
    FROM 
      cloud_files("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_product", "json",
        map("cloudFiles.schemaHints", "product_id BIGINT ,type STRING,SKU STRING,name STRING,description STRING,sale_price DOUBLE,regular_price DOUBLE,created_date TIMESTAMP,updated_date TIMESTAMP,start_at TIMESTAMP,end_at TIMESTAMP",
           "mergeSchema", "true"
           )
                 ) 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_customer (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "RAW Customer data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS  SELECT 
      * 
    FROM 
      cloud_files("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_customer", "json",
        map("cloudFiles.schemaHints", "customer_id BIGINT,name STRING,email STRING,address STRING,created_date TIMESTAMP,updated_date TIMESTAMP,start_at TIMESTAMP,end_at TIMESTAMP",
            "mergeSchema", "true"
           )
                 ) 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_sales (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "RAW Sales data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS  SELECT 
      * 
    FROM 
      cloud_files("abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/fact_sales", "csv",
        map("schema", "transaction_id BIGINT,date_id BIGINT ,customer_id BIGINT ,product_id BIGINT ,store_id BIGINT ,store_business_key STRING,sales_amount DOUBLE,_rescued_data STRING",
            "header", "false",
            "mergeSchema", "true"
           )
                 ) 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_store(
  sha1_hub_store          STRING     NOT NULL,
  store_id                BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
)
COMMENT " HUb STORE TABLE"
AS SELECT
      sha1(UPPER(TRIM(store_id))) as sha1_hub_store ,
      store_id,
      current_timestamp as load_ts,
      "Store" as source
   FROM
      STREAM(live.raw_store)
      

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_store(
  sha1_hub_store          STRING    NOT NULL,
  business_key            STRING,
  name                    STRING,
  email                   STRING,
  city                    STRING,
  address                 STRING,
  phone_number            STRING,
  start_at                TIMESTAMP, 
  hash_diff               STRING    NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL
)
COMMENT " SAT STORE TABLE"
AS SELECT
      sha1(UPPER(TRIM(store_id))) as sha1_hub_store,
      business_key,
      name,
      email,
      city,
      address,
      phone_number,
      start_at,
      sha1(concat(UPPER(TRIM(business_key)),UPPER(TRIM(name)),UPPER(TRIM(email)),UPPER(TRIM(address)),UPPER(TRIM(phone_number)))) as hash_diff,
      current_timestamp as load_ts,
      "Store" as source
   FROM
      STREAM(live.raw_store)
  

  

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_product(
  sha1_hub_product          STRING     NOT NULL,
  product_id                BIGINT     NOT NULL,
  load_ts                   TIMESTAMP,
  source                    STRING
)
COMMENT " HUb PRODUTC TABLE"
AS SELECT
      sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
      product_id,
      current_timestamp as load_ts,
      "Product" as source
   FROM 
       STREAM(live.raw_product)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_product(
  sha1_hub_product          STRING    NOT NULL,
  type                      STRING,
  SKU                       STRING,
  name                      STRING,
  description               STRING,
  sale_price                DOUBLE,
  regular_price             DOUBLE,
  start_at                  TIMESTAMP, 
  hash_diff                 STRING    NOT NULL,
  load_ts                   TIMESTAMP,
  source                    STRING    NOT NULL
  
)
COMMENT " SAT PRODUCT TABLE"
AS SELECT
      sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
      type,
      sku,
      name,
      description,
      sale_price,
      regular_price,
      start_at,
      sha1(concat(UPPER(TRIM(type)),UPPER(TRIM(SKU)),UPPER(TRIM(name)),UPPER(TRIM(sale_price)),UPPER(TRIM(regular_price)))) as hash_diff,
      current_timestamp as load_ts,
      "Product" as source
   FROM
       STREAM(live.raw_product)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_customer(
  sha1_hub_customer       STRING     NOT NULL,
  customer_id             BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
      customer_id,
      current_timestamp as load_ts ,
      "Customer" as source
   FROM
       STREAM(live.raw_customer)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_customer(
  sha1_hub_customer       STRING    NOT NULL,
  name                    STRING,
  email                   STRING,
  address                 STRING,
  start_at                TIMESTAMP, 
  hash_diff               STRING    NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL
)
COMMENT " SAT CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
      name,
      email,
      address,
      start_at,
      sha1(concat(UPPER(TRIM(name)),UPPER(TRIM(email)),UPPER(TRIM(address)))) as hash_diff,
      current_timestamp as load_ts,
      "Customer" as source
  FROM
      STREAM(live.raw_customer)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_sales(
  sha1_hub_sales_id       STRING     NOT NULL,
  transaction_id          BIGINT     NOT NULL,
  load_ts                 TIMESTAMP  NOT NULL,
  source                  STRING     NOT NULL
)
COMMENT " HUb SALES TABLE"
AS SELECT
      sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
      transaction_id,
      current_timestamp as load_ts,
      "Sales" as source
   FROM
       STREAM(live.raw_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_sales(
  sha1_hub_sales_id          STRING     NOT NULL,
  sales_date              BIGINT,
  sales_amount            DOUBLE,
  store_business_key      STRING,
  load_ts                 TIMESTAMP,
  source                  STRING     NOT NULL
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT " SAT SALES TABLE"
AS SELECT
      sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
      date_id as sales_date ,
      sales_amount,
      store_business_key,
      current_timestamp as load_ts,
       "Sales"  as source
   FROM
       STREAM(live.raw_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lnk_customer_sales
(
  sha1_lnk_customer_sales        STRING     NOT NULL ,  
  sha1_hub_sales_id              STRING ,
  sha1_hub_customer              STRING ,
  load_ts                        TIMESTAMP  NOT NULL,
  source                         STRING     NOT NULL 
)
COMMENT " LNK CUSTOMER SALES TABLE "
AS SELECT
      sha1(concat(UPPER(TRIM(transaction_id)),UPPER(TRIM(customer_id)))) as sha1_lnk_customer_sales,
      sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
      sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
      current_timestamp as load_ts,
      "Sales and Customer" as source
   FROM
       STREAM(live.raw_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lnk_product_sales
(
  sha1_lnk_product_sales         STRING     NOT NULL ,  
  sha1_hub_sales_id              STRING ,
  sha1_hub_product               STRING ,
  load_ts                        TIMESTAMP  NOT NULL,
  source                         STRING     NOT NULL
)
COMMENT " LNK PRODUCT SALES TABLE "
AS SELECT
      sha1(concat(UPPER(TRIM(transaction_id)),UPPER(TRIM(product_id)))) as sha1_lnk_product_sales,
      sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
      sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
      current_timestamp as load_ts,
      "Sales and Product" as source
   FROM
       STREAM(live.raw_sales)

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE sat_sales_bv
(
  sha1_hub_sales_id         STRING     NOT NULL ,  
  sales_date                BIGINT ,
  sales_amount              DOUBLE ,
  store_business_key        STRING  NOT NULL,
  custom_business_rule      STRING     NOT NULL
)
COMMENT " SAT SALES Business Vault TABLE "
AS SELECT
          sha1_hub_sales_id AS sha1_hub_sales_id,
          sales_date AS sales_date,
		  sales_amount AS sales_amount,
		  store_business_key AS store_business_key,
		  CASE WHEN store_business_key IN ('BNE02') THEN 'Tier-1'
               WHEN store_business_key IN ('PER01') THEN 'Tier-2'  
				 ELSE 'Tier-3'
		  END custom_business_rule 
   FROM
       live.sat_sales

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE sat_sales_bv_agg
(
  sales_amount_sum          DOUBLE ,
  store_business_key        STRING  NOT NULL
)
COMMENT " SAT  SALES Business Vault Agg TABLE "
AS SELECT
		  sum(sales_amount) AS sales_amount_sum,
		  store_business_key AS store_business_key
		  
   FROM
       live.sat_sales
   GROUP BY
       store_business_key
       
