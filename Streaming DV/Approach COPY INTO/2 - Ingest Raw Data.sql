-- Databricks notebook source
use main.copy_into_approach;
COPY INTO raw_store
  FROM(
      SELECT
        store_id::BIGINT,
        business_key,
        name,
        email,
        city,
        address,
        phone_number,
        created_date::TIMESTAMP,
        updated_date::TIMESTAMP,
        start_at::TIMESTAMP
      FROM 'abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_store'  
      )
  FILEFORMAT = JSON   ;
  
 
 
 COPY INTO raw_product
   FROM(
       SELECT
          product_id::BIGINT ,
          type::STRING,
          SKU::STRING,
          name::STRING,
          description::STRING,
          sale_price::DOUBLE,
          regular_price::DOUBLE,
          created_date::TIMESTAMP,
          updated_date::TIMESTAMP,
          start_at::TIMESTAMP
        FROM 'abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_product'  
        )
  FILEFORMAT = JSON   ; 
  
  
  
  COPY INTO raw_customer
   FROM(
       SELECT
          customer_id::BIGINT ,
          name::STRING,
          email::STRING,
          address::STRING,
          created_date::TIMESTAMP,
          updated_date::TIMESTAMP,
          start_at::TIMESTAMP
        FROM 'abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/dim_customer'  
        )
  FILEFORMAT = JSON   ;    
  
  
  COPY INTO raw_sales
  FROM(
      SELECT
        _c0::BIGINT  transaction_id ,
        _c1::BIGINT  date_id ,
        _c2::BIGINT  customer_id ,
        _c3::BIGINT  product_id ,
        _c4::BIGINT  store_id ,
        _c5::STRING  store_business_key,
        _c6::DOUBLE  sales_amount
      FROM 'abfss://datavault@sumitsalesdata.dfs.core.windows.net/Demo/Tables/fact_sales'  
      )
  FILEFORMAT = CSV 
  COPY_OPTIONS   ('header' = 'false','mergeSchema' = 'true')
  ;
