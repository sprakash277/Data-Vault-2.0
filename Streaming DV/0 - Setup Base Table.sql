-- Databricks notebook source
create database if not exists main.dv2_0;
use main.dv2_0;

CREATE OR REPLACE TABLE main.dv2_0.dim_store(
  store_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
);

-- Product dimension
CREATE OR REPLACE TABLE main.dv2_0.dim_product(
  product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
);

-- Customer dimension
CREATE OR REPLACE TABLE main.dv2_0.dim_customer(
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  name STRING,
  email STRING,
  address STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
);

-- Date dimension
CREATE OR REPLACE TABLE main.dv2_0.dim_date(
  date_id BIGINT PRIMARY KEY,
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
);



-- Fact Sales
CREATE OR REPLACE TABLE main.dv2_0.fact_sales(  
  transaction_id BIGINT PRIMARY KEY,
  date_id BIGINT NOT NULL CONSTRAINT dim_date_fk FOREIGN KEY REFERENCES dim_date,
  customer_id BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
  product_id BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
  store_id BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
  store_business_key STRING,
  sales_amount DOUBLE
);




-- COMMAND ----------

INSERT INTO
  main.dv2_0.dim_store (business_key, name, email, city, address, phone_number, created_date, updated_date, start_at, end_at)
VALUES
  ("PER01", "Perth CBD", "yhernandez@example.com", "Perth", "Level 2 95 Jorge Vale St. Gary, NT, 2705", "08-9854-6006", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("BNE02", "Brisbane Airport" , "castillojoseph@example.net", "Brisbane", "6 Ware Copse Doughertystad, NSW, 2687", "0425.061.371", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  main.dv2_0.dim_product (type, SKU, name, description, sale_price, regular_price, created_date, updated_date, start_at, end_at)
VALUES 
  ("variable", "vneck-tee", "V-Neck T-Shirt", "This is a variable product of type vneck-tee", "60.00", "50.00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("simple", "hoodie", "Hoodie", "This is a simple product of type hoodie", "90.00", "79.00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  main.dv2_0.dim_customer (name, email, address, created_date, updated_date, start_at, end_at)
VALUES 
  ("Stephanie Brown", "howardalejandra@example.net", "8273 Jerry Pine East Angela, ID 50196", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("Christopher Cooper", "campbelljohn@example.net", "8273 Jerry Pine East Angela, ID 50196", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("Daniel White", "colonricardo@example.net", "945 Goodwin Plain Suite 312 Dylanmouth, NY 14319", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  main.dv2_0.dim_date (date_id, date_num, date, year_month_number, calendar_quarter, month_num, month_name, created_date, updated_date, start_at, end_at)
VALUES 
  (20211001, 20211001, "2021-10-01", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  (20211002, 20211002, "2021-10-02", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  (20211003, 20211003, "2021-10-03", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
-- Insert Sample Data to Fact Table

INSERT INTO
    main.dv2_0.fact_sales (transaction_id, date_id, customer_id, product_id, store_id, store_business_key, sales_amount)
  VALUES
    (10001, 20211001, 1, 1, 1, "PER01", 50.00),
    (10002, 20211002, 2, 1, 2, "BNE02", 79.00),
    (10003, 20211002, 1, 2, 2, "BNE02", 79.00),
    (10004, 20211003, 2, 1, 2, "BNE02", 60.00),
    (10005, 20211003, 3, 2, 1, "PER01", 79.00);
