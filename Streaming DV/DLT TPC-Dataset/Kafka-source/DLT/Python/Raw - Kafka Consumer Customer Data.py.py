# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

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

stream_customer_info_kafka_raw = (spark.readStream
    .format("kafka")
    .option("subscribe", EH_KAFKA_TOPIC)
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load())

# COMMAND ----------


@dlt.table(
  name="kafka_customer_events_raw",
  comment="The Customer info dataset, ingested from Event Hubs kafka topic.",
  table_properties={
    "quality": "raw",
    "pipelines.reset.allowed": "false"
  }
)
@dlt.expect("valid_kafka_key_not_null", "key IS NOT NULL")
def kafka_customer_events_raw():
  return stream_customer_info_kafka_raw
