-- Databricks notebook source
-- DBTITLE 1,DDL Syntax Landing Zone on Bronze Layer
create database if not exists main.copy_into_approach;
use main.copy_into_approach;

CREATE OR REPLACE TABLE raw_store(
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
CREATE OR REPLACE TABLE raw_product(
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
CREATE OR REPLACE TABLE raw_customer(
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
CREATE OR REPLACE TABLE raw_date(
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
CREATE OR REPLACE TABLE raw_sales(  
  transaction_id BIGINT ,
  date_id BIGINT ,
  customer_id BIGINT ,
  product_id BIGINT ,
  store_id BIGINT ,
  store_business_key STRING,
  sales_amount DOUBLE
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);



