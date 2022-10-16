-- Databricks notebook source
------------------------- REFRESH MATERIALIZED VIEWS ------------------------------------------------
REFRESH MATERIALIZED VIEW main.copy_into_approach.hub_store;    
REFRESH MATERIALIZED VIEW main.copy_into_approach.sat_store;  
REFRESH MATERIALIZED VIEW main.copy_into_approach.hub_product;
REFRESH MATERIALIZED VIEW main.copy_into_approach.sat_product;    
REFRESH MATERIALIZED VIEW main.copy_into_approach.hub_customer;  
REFRESH MATERIALIZED VIEW main.copy_into_approach.sat_customer;  
REFRESH MATERIALIZED VIEW main.copy_into_approach.sat_sales;    
REFRESH MATERIALIZED VIEW main.copy_into_approach.lnk_customer_sales;  
REFRESH MATERIALIZED VIEW main.copy_into_approach.lnk_product_sales;  
