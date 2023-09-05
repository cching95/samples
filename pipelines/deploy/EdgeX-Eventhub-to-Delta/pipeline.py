from rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from rtdip_sdk.pipelines.transformers.spark.binary_to_string import BinaryToStringTransformer
from rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from rtdip_sdk.pipelines.transformers.spark.edgex_opcua_json_to_pcdm import EdgeXOPCUAJsonToPCDMTransformer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json


def edgeX_eventhub_to_delta():

  # Spark session setup not required if running in Databricks
  spark = (SparkSession.builder.appName("MySparkSession")
           .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .getOrCreate())
  
  ehConf = {
    "eventhubs.connectionString": "{EventhubConnectionString}",
    "eventhubs.consumerGroup": "{EventhubConsumerGroup}",
    "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})}

  source = SparkEventhubSource(spark, ehConf).read_batch()
  string_data = BinaryToStringTransformer(source,"body", "body").transform()
  PCDM_data = EdgeXOPCUAJsonToPCDMTransformer(string_data,"body").transform()
  SparkDeltaDestination(data= PCDM_data, options= {}, destination="{/path/to/destination}").write_batch()

if __name__ == "__main__":
  edgeX_eventhub_to_delta()