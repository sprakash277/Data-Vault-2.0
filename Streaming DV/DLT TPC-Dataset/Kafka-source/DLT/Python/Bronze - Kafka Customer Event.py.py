# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

customer_events_parsed_schema = StructType([ \
    StructField("c_custkey", IntegerType(), True),\
    StructField("c_name", StringType(),True),\
    StructField("c_address", StringType(),True),\
    StructField("c_nationkey", IntegerType(),True),\
    StructField("c_phone", StringType(),True),\
    StructField("c_acctbal", DecimalType(),True),\
    StructField("c_mktsegment", StringType(),True),\
    StructField("c_comment", StringType(),True)                                       
  ])

@dlt.table(
  name="customer_events_parsed_bronze",
  comment="The Customer info dataset, Parsed",
  table_properties={
    "quality": "bronze"
  }
)
@dlt.expect("c_custkey", "c_custkey IS NOT NULL")
def customer_events_parsed_bronze():
  return (
    dlt.read_stream("kafka_customer_events_raw")  
    .select(col("timestamp"),
            col("key").cast("string"),
            from_json(col("value").cast("string"), customer_events_parsed_schema).alias("parsed_value")
           )
    .select("timestamp",
            "key",
            "parsed_value.*")
  )
