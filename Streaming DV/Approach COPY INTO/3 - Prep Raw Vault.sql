-- Databricks notebook source

CREATE MATERIALIZED VIEW main.copy_into_approach.hub_store SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(store_id))) as sha1_hub_store ,
            store_id as store_id ,
            current_timestamp as load_ts,
            "Store" as source
		 FROM 
			raw_store ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.sat_store SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(store_id))) as sha1_hub_store,
            business_key as business_key ,
            name as name ,
            email as email,
            city as city,
            address as address,
            phone_number as phone_number,
            start_at as start_at,
            sha1(concat(UPPER(TRIM(business_key)),UPPER(TRIM(name)),UPPER(TRIM(email)),UPPER(TRIM(address)),UPPER(TRIM(phone_number)))) as hash_diff,
            current_timestamp as load_ts,
            "Store" as source
		 FROM 
			raw_store ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.hub_product SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
            product_id as product_id,
            current_timestamp as load_ts,
            "Product" as source
		 FROM 
			raw_product ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.sat_product SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
            type as type ,
            sku as sku,
            name as name ,
            description as description,
            sale_price as sale_price,
            regular_price as regular_price,
            start_at as start_at,
            sha1(concat(UPPER(TRIM(type)),UPPER(TRIM(SKU)),UPPER(TRIM(name)),UPPER(TRIM(sale_price)),UPPER(TRIM(regular_price)))) as hash_diff,
            current_timestamp as load_ts,
            "Product" as source
		 FROM 
			raw_product ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.hub_customer SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
            customer_id as customer_id,
            current_timestamp as load_ts ,
            "Customer" as source
		 FROM 
			raw_customer ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.sat_customer SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
            name as name,
            email as email,
            address as address,
            start_at as start_at,
            sha1(concat(UPPER(TRIM(name)),UPPER(TRIM(email)),UPPER(TRIM(address)))) as hash_diff,
            current_timestamp as load_ts,
            "Customer" as source
		 FROM 
			raw_customer ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.hub_sales SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
            transaction_id as transaction_id,
            current_timestamp as load_ts,
            "Sales" as source
		 FROM 
			raw_sales ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.sat_sales SNAPSHOT AS
		SELECT 
			sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
            date_id as sales_date ,
            sales_amount as sales_amount,
            store_business_key as store_business_key,
            current_timestamp as load_ts,
             "Sales"  as source
		 FROM 
			raw_sales ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.lnk_customer_sales SNAPSHOT AS
		SELECT 
			sha1(concat(UPPER(TRIM(transaction_id)),UPPER(TRIM(customer_id)))) as sha1_lnk_customer_sales,
            sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
            sha1(UPPER(TRIM(customer_id))) as sha1_hub_customer,
            current_timestamp as load_ts,
            "Sales and Customer" as source
		 FROM 
			raw_sales ;

-- COMMAND ----------

CREATE MATERIALIZED VIEW main.copy_into_approach.lnk_product_sales SNAPSHOT AS
		SELECT 
			sha1(concat(UPPER(TRIM(transaction_id)),UPPER(TRIM(product_id)))) as sha1_lnk_product_sales,
            sha1(UPPER(TRIM(transaction_id))) as sha1_hub_sales_id,
            sha1(UPPER(TRIM(product_id))) as sha1_hub_product,
            current_timestamp as load_ts,
            "Sales and Product" as source
		 FROM 
			raw_sales ;
