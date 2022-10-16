-- Databricks notebook source
--------------------------       MV CODE STARTS HERE     ----------------------------------------
------------------------- MV can Only Run on SQL Warehouse , No Serverless ----------------------


CREATE MATERIALIZED VIEW main.dv2_0.sat_sales_bv SNAPSHOT AS
		SELECT 
			sha1_hub_sales_id AS sha1_hub_sales_id,
			sales_date AS sales_date,
			sales_amount AS sales_amount,
			store_business_key AS store_business_key,
			CASE WHEN store_business_key IN ('BNE02') THEN 'Tier-1'
				 WHEN store_business_key IN ('PER01') THEN 'Tier-2'  
				 ELSE 'Tier-3'
			END custom_business_rule 
		 FROM 
			main.dv2_0.sat_sales ;

------------------------- REFRESH MATERIALIZED VIEWS ------------------------------------------------
REFRESH MATERIALIZED VIEW main.dv2_0.sat_sales_bv;            
		
