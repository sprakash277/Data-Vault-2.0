# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample Customer Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Producer
# MAGIC 
# MAGIC The producer will generate Customer Info  event data as a real-time stream in Event Hubs (Kafka protocol)

# COMMAND ----------

from pyspark.sql.functions import lit, col
from pyspark.sql.types import LongType, StringType
from pyspark.sql.functions import udf
import random
import json
import string
import decimal



# COMMAND ----------

# KAFKA CONFIGS (EVENT HUB)


EH_KAFKA_TOPIC = "customer_info"

# Get Databricks secret value 
EH_NAMESPACE = dbutils.secrets.get(scope = "Sumit_Key_Vault", key = "event-hub-nscp")
connSharedAccessKey = dbutils.secrets.get(scope = "Sumit_Key_Vault", key = "sumit-eventhub-nspc1")
connSharedAccessKeyName = dbutils.secrets.get(scope = "Sumit_Key_Vault", key = "accesskey")
EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"

EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={connSharedAccessKeyName};SharedAccessKey={connSharedAccessKey};EntityPath={EH_KAFKA_TOPIC}\";"


# COMMAND ----------

def randomNationKey():
  validNationKeys= [2,3,5,6,8,9,10,12,14,16,22]
  return validNationKeys[random.randint(0,len(validNationKeys)-1)]

def randomMarketSegment():
  validMarketSegments= ['MACHINERY','AUTOMOBILE','BUILDING','HOUSEHOLD','FURNITURE']
  return validMarketSegments[random.randint(0,len(validMarketSegments)-1)]
  
def randomArtistSong():
  return [random.randint(30001,30084)]

def randomCustomerKey():
  return random.randint(990000,9900000)

def randomCustomerName():
  return "customer_"+''.join(random.choices(string.digits, k=10))

def randomCustomerAddress():
  return ''.join(random.choices(string.ascii_letters, k=10))

def randomAccountBalance():
  return float(random.randrange(150000, 3000000))/100

def randomPhoneNumber():
  return ''.join(random.choices(string.digits, k=10))


# COMMAND ----------

def genCustomerInfo():
    
    event = {
      "c_custkey": randomCustomerKey(),
      "c_name": randomCustomerName(),
      "c_address": randomCustomerAddress(),
      "c_nationkey": randomNationKey(),
      "c_phone":  randomPhoneNumber(), 
      "c_acctbal": randomAccountBalance(), 
      "c_mktsegment": randomMarketSegment(),
      "c_comment": randomCustomerAddress() 
    }

    event_string = json.dumps(event)
    return event_string
#     return event
  
#print(genCustomerInfo())

# COMMAND ----------

gen_event_schema_udf = udf(genCustomerInfo, StringType())

source_schema = (
  spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .load()
    .withColumn("key", lit("event"))
    .withColumn("value", lit(gen_event_schema_udf()))
    .select("key", col("value").cast("string"))
)
#display(source_schema)

# COMMAND ----------

from datetime import datetime
# helps avoiding loading and writing all historical data. 
datetime_checkpoint = datetime.now().strftime('%Y%m%d%H%M%S')

# Write df to EventHubs using Spark's Kafka connector
write = (source_schema.writeStream
    .format("kafka")
    .outputMode("append")
    .option("topic", EH_KAFKA_TOPIC)
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("checkpointLocation", f"/tmp/{EH_NAMESPACE}/{EH_KAFKA_TOPIC}/{datetime_checkpoint}/_checkpoint")
    .trigger(processingTime='10 seconds')
    .start())
