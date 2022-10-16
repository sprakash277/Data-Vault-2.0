-- Databricks notebook source
-- DBTITLE 1,DDL Syntax Landing Zone on Bronze Layer
create database if not exists main.dv2_0;
use main.dv2_0;

CREATE OR REPLACE TABLE main.dv2_0.raw_dim_store(
  store_id BIGINT ,
  business_key STRING,
  name STRING,
  email STRING,
  city STRING,
  address STRING,
  phone_number STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Product dimension
CREATE OR REPLACE TABLE main.dv2_0.raw_dim_product(
  product_id BIGINT ,
  type STRING,
  SKU STRING,
  name STRING,
  description STRING,
  sale_price DOUBLE,
  regular_price DOUBLE,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Customer dimension
CREATE OR REPLACE TABLE main.dv2_0.raw_dim_customer(
  customer_id BIGINT ,
  name STRING,
  email STRING,
  address STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Date dimension
CREATE OR REPLACE TABLE main.dv2_0.raw_dim_date(
  date_id BIGINT ,
  date_num INT,
  date STRING,
  year_month_number INT,
  calendar_quarter STRING,
  month_num INT,
  month_name STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);



-- Fact Sales
CREATE OR REPLACE TABLE main.dv2_0.raw_fact_sales(  
  transaction_id BIGINT ,
  date_id BIGINT ,
  customer_id BIGINT ,
  product_id BIGINT ,
  store_id BIGINT ,
  store_business_key STRING,
  sales_amount DOUBLE
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);




-- COMMAND ----------

-- DBTITLE 1,DDL Syntax to Create Raw Data vault

----- Store
CREATE OR REPLACE TABLE main.dv2_0.hub_store(
  sha1_hub_store          STRING     NOT NULL,
  store_id                BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING,
  CONSTRAINT pk_hub_store   PRIMARY KEY(sha1_hub_store)
);

CREATE OR REPLACE TABLE main.dv2_0.sat_store(
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
  source                  STRING    NOT NULL,
  CONSTRAINT pk_sat_store          PRIMARY KEY(sha1_hub_store, load_ts),
  CONSTRAINT fk_sat_store          FOREIGN KEY(sha1_hub_store) REFERENCES main.dv2_0.hub_store
);                       

----- Product
CREATE OR REPLACE TABLE main.dv2_0.hub_product(
  sha1_hub_product          STRING     NOT NULL,
  product_id                BIGINT     NOT NULL,
  load_ts                   TIMESTAMP,
  source                    STRING,
  CONSTRAINT pk_hub_product   PRIMARY KEY(sha1_hub_product)
);

CREATE OR REPLACE TABLE main.dv2_0.sat_product(
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
  source                    STRING    NOT NULL,
  CONSTRAINT pk_sat_product          PRIMARY KEY(sha1_hub_product, load_ts),
  CONSTRAINT fk_sat_product          FOREIGN KEY(sha1_hub_product) REFERENCES main.dv2_0.hub_product
);



---- Customers 

CREATE OR REPLACE TABLE main.dv2_0.hub_customer(
  sha1_hub_customer       STRING     NOT NULL,
  customer_id             BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING,
  CONSTRAINT pk_hub_customer   PRIMARY KEY(sha1_hub_customer)
);

CREATE OR REPLACE TABLE main.dv2_0.sat_customer(
  sha1_hub_customer       STRING    NOT NULL,
  name                    STRING,
  email                   STRING,
  address                 STRING,
  start_at                TIMESTAMP, 
  hash_diff               STRING    NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL,
  CONSTRAINT pk_sat_customer       PRIMARY KEY(sha1_hub_customer, load_ts),
  CONSTRAINT fk_sat_customer       FOREIGN KEY(sha1_hub_customer) REFERENCES main.dv2_0.hub_customer
);             

---- Sales

CREATE OR REPLACE TABLE main.dv2_0.hub_sales(
  sha1_hub_sales_id       STRING     NOT NULL,
  transaction_id          BIGINT     NOT NULL,
  load_ts                 TIMESTAMP  NOT NULL,
  source                  STRING     NOT NULL,
  CONSTRAINT pk_hub_sales_id   PRIMARY KEY(sha1_hub_sales_id)
);

CREATE OR REPLACE TABLE main.dv2_0.sat_sales(
  sha1_hub_sales_id          STRING     NOT NULL,
  sales_date              BIGINT,
  sales_amount            DOUBLE,
  store_business_key      STRING,
  load_ts                 TIMESTAMP,
  source                  STRING     NOT NULL,
  CONSTRAINT pk_sat_sales_id       PRIMARY KEY(sha1_hub_sales_id, load_ts),
  CONSTRAINT fk_sat_sales_id       FOREIGN KEY(sha1_hub_sales_id) REFERENCES main.dv2_0.hub_sales
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);;  

---- DDL for Customer Sales link Table

CREATE OR REPLACE TABLE main.dv2_0.lnk_customer_sales
(
  sha1_lnk_customer_sales        STRING     NOT NULL ,  
  sha1_hub_sales_id              STRING ,
  sha1_hub_customer              STRING ,
  load_ts                        TIMESTAMP  NOT NULL,
  source                         STRING     NOT NULL ,
  CONSTRAINT pk_lnk_customer_sales  PRIMARY KEY(sha1_lnk_customer_sales),
  CONSTRAINT fk1_lnk_customer_sales FOREIGN KEY(sha1_hub_sales_id)    REFERENCES main.dv2_0.hub_sales,
  CONSTRAINT fk2_lnk_customer_sales FOREIGN KEY(sha1_hub_customer)    REFERENCES main.dv2_0.hub_customer
);

---- DDL for Product Sales link Table

CREATE OR REPLACE TABLE main.dv2_0.lnk_product_sales
(
  sha1_lnk_product_sales         STRING     NOT NULL ,  
  sha1_hub_sales_id              STRING ,
  sha1_hub_product               STRING ,
  load_ts                        TIMESTAMP  NOT NULL,
  source                         STRING     NOT NULL,
  CONSTRAINT pk_lnk_product_sales  PRIMARY KEY(sha1_lnk_product_sales),
  CONSTRAINT fk1_lnk_product_sales FOREIGN KEY(sha1_hub_sales_id)    REFERENCES main.dv2_0.hub_sales,
  CONSTRAINT fk2_lnk_product_sales FOREIGN KEY(sha1_hub_product)    REFERENCES main.dv2_0.hub_product
);
