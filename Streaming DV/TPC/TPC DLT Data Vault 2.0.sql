-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE raw_customer 
COMMENT "RAW Customer Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/customer", "parquet",
      map("schema", 
          " 
          c_custkey     bigint,
          c_name        string,
          c_address     string,
          c_nationkey   bigint,
          c_phone       string,
          c_acctbal     decimal(18,2),
          c_mktsegment  string,
          c_comment     string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_customer(
  sha1_hub_custkey        STRING    NOT NULL,
  c_name                    STRING,
  c_address                 STRING,
  c_nationkey               BIGINT,
  c_phone                   STRING,
  c_acctbal                 DECIMAL(18,2),
  c_mktsegment              STRING,
  hash_diff               STRING    NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL
)
COMMENT " SAT CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(c_custkey))) as sha1_hub_custkey,
      c_name,
      c_address,
      c_nationkey,
      c_phone,
      c_acctbal,
      c_mktsegment,
      sha1(concat(UPPER(TRIM(c_name)),UPPER(TRIM(c_address)),UPPER(TRIM(c_phone)),UPPER(TRIM(c_mktsegment)))) as hash_diff,
      current_timestamp as load_ts,
      "Customer" as source
   FROM
      STREAM(live.raw_customer)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_customer(
  sha1_hub_custkey        STRING     NOT NULL,
  c_custkey                 BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(c_custkey))) as sha1_hub_custkey ,
      c_custkey,
      current_timestamp as load_ts,
      "Customer" as source
   FROM
      STREAM(live.raw_customer)
      

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_orders 
COMMENT "RAW Orders Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/orders", "parquet",
      map("schema", 
          " 
          o_orderkey       bigint,
          o_custkey        bigint,
          o_orderstatus    string,
          o_totalprice     decimal(18,2),
          o_orderdate      date,
          o_orderpriority  string,
          o_clerk          string,
          o_shippriority   int,
          o_comment        string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_orders(
  sha1_hub_orderkey       STRING    NOT NULL,
  o_orderstatus             STRING,
  o_totalprice              decimal(18,2),
  o_orderdate               DATE,
  o_orderpriority           STRING,
  o_clerk                   STRING,
  o_shippriority            INT,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL
)
COMMENT " SAT CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(o_orderkey))) as sha1_hub_orderkey,
      o_orderstatus,
      o_totalprice,
      o_orderdate,
      o_orderpriority,
      o_clerk,
      o_shippriority,
      current_timestamp as load_ts,
      "Order" as source
   FROM
      STREAM(live.raw_orders)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_orders(
  sha1_hub_orderkey       STRING     NOT NULL,
  o_orderkey              BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1(UPPER(TRIM(o_orderkey))) as sha1_hub_orderkey ,
      o_orderkey,
      current_timestamp as load_ts,
      "Order" as source
   FROM
      STREAM(live.raw_orders)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lnk_customer_orders
(
  sha1_lnk_customer_order_key     STRING     NOT NULL ,  
  sha1_hub_orderkey               STRING ,
  sha1_hub_custkey                STRING ,
  load_ts                         TIMESTAMP  NOT NULL,
  source                          STRING     NOT NULL 
)
COMMENT " LNK CUSTOMER ORDERS TABLE "
AS SELECT
      sha1(concat(UPPER(TRIM(o_orderkey)),UPPER(TRIM(o_custkey)))) as sha1_lnk_customer_order_key,
      sha1(UPPER(TRIM(o_orderkey))) as sha1_hub_orderkey,
      sha1(UPPER(TRIM(o_custkey)))  as sha1_hub_custkey,
      current_timestamp             as load_ts,
      "Orders and Customer"         as source
   FROM
       STREAM(live.raw_orders)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_lineitem 
COMMENT "RAW LineItem Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/lineitem", "parquet",
      map("schema", 
          " 
          l_orderkey      bigint,
          l_partkey       bigint,
          l_suppkey       bigint,
          l_linenumber    int,
          l_quantity      decimal(18,2),
          l_extendedprice decimal(18,2),
          l_discount      decimal(18,2),
          l_tax           decimal(18,2),
          l_returnflag    string,
          l_linestatus    string,
          l_shipdate      date,
          l_commitdate    date,
          l_receiptdate   date,
          l_shipinstructs string,
          l_shipmode      string,
          l_comment       string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_part 
COMMENT "RAW Parts Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/part", "parquet",
      map("schema", 
          " 
          p_partkey      bigint,
          p_name         string,
          p_mfgr         string,
          p_brand        string,
          p_type         string,
          p_size         int,
          p_container    string,
          p_retailprice  decimal(18,2),
          p_comment      string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_supplier
COMMENT "RAW Supplier Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/supplier", "parquet",
      map("schema", 
          " 
          s_suppkey      bigint,
          s_name         string,
          s_address      string,
          s_nationkey    bigint,
          s_phone        string,
          s_acctbal      decimal(18,2),
          s_comment      string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_region 
COMMENT "RAW Region Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/region", "parquet",
      map("schema", 
          " 
          r_regionkey     bigint,
          r_name          string,
          r_comment       string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE ref_region(
  r_regionkey        bigint     NOT NULL,
  r_name             STRING ,
  load_ts            TIMESTAMP,
  source             STRING
)
COMMENT " Ref Region Table"
AS SELECT
        r_regionkey,
        r_name,
        current_timestamp as load_ts,
        "Region" as source
   FROM
        live.raw_region

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_nation
COMMENT "RAW Nation Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/nation", "parquet",
      map("schema", 
          " 
          n_nationkey     bigint,
          n_name          string,
          n_regionkey     bigint,
          n_comment       string
          "
           )
      )

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE ref_nation(
  n_nationkey        bigint     NOT NULL,
  n_name             STRING ,
  n_regionkey        bigint,
  load_ts            TIMESTAMP,
  source             STRING
)
COMMENT " Ref Nation Table"
AS SELECT
        n_nationkey,
        n_name,
        n_regionkey,
        current_timestamp as load_ts,
        "Nation" as source
   FROM
        live.raw_nation

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE hub_lineitem(
  sha1_hub_lineitem        STRING     NOT NULL,
  sha1_hub_orderkey        STRING     NOT NULL,
  l_linenumber             int,
  load_ts                  TIMESTAMP,
  source                   STRING
)
COMMENT " HUb LINEITEM TABLE"
AS SELECT
      sha1(concat(UPPER(TRIM(l_orderkey)),UPPER(TRIM(l_linenumber)))) as sha1_hub_lineitem,
      sha1(UPPER(TRIM(l_orderkey))) as sha1_hub_orderkey,
      l_linenumber,
      current_timestamp as load_ts,
      "LineItem" as source
   FROM
      STREAM(live.raw_lineitem)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sat_lineitem(
          sha1_hub_lineitem        STRING     NOT NULL,
          l_quantity               decimal(18,2),
          l_extendedprice          decimal(18,2),
          l_discount               decimal(18,2),
          l_tax                    decimal(18,2),
          l_returnflag             string,
          l_linestatus             string,
          l_shipdate               date,
          l_commitdate             date,
          l_receiptdate            date,
          l_shipinstructs          string,
          l_shipmode               string,
          load_ts                  TIMESTAMP,
          source                   STRING
)
COMMENT " SAT LINEITEM TABLE"
AS SELECT
          sha1(concat(UPPER(TRIM(l_orderkey)),UPPER(TRIM(l_linenumber)))) as sha1_hub_lineitem,
          l_quantity,
          l_extendedprice,
          l_discount,
          l_tax,
          l_returnflag,
          l_linestatus,
          l_shipdate,
          l_commitdate,
          l_receiptdate,
          l_shipinstructs,
          l_shipmode,
          current_timestamp as load_ts,
          "LineItem" as source
   FROM
      STREAM(live.raw_lineitem)
      
