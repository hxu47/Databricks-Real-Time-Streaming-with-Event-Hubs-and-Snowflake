# Databricks notebook source
# MAGIC %md
# MAGIC ### READ Stream using Event Hubs

# COMMAND ----------

event_hub_connection_string = "Endpoint=sb://aish-eventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8n0hSJM13/UT2GsF6j5HGagH4S+cT5XpktBju5cAFMo=" 

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC event_hub_name = "src-streaming"
# MAGIC connection_string = event_hub_connection_string + ";EntityPath=" + event_hub_name
# MAGIC
# MAGIC print("Consumer Connection String: {}".format(connection_string))

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import json
# MAGIC
# MAGIC # Create the starting position Dictionary
# MAGIC startingEventPosition = {
# MAGIC   "offset": "-1",
# MAGIC   "seqNo": -1,            # not in use
# MAGIC   "enqueuedTime": None,   # not in use
# MAGIC   "isInclusive": True
# MAGIC }
# MAGIC
# MAGIC eventHubsConf = {
# MAGIC   "eventhubs.connectionString" : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
# MAGIC   "eventhubs.startingPosition" : json.dumps(startingEventPosition),
# MAGIC   "setMaxEventsPerTrigger": 100
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
# MAGIC
# MAGIC eventStreamDF = (spark.readStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**eventHubsConf)
# MAGIC   .load()
# MAGIC )
# MAGIC
# MAGIC eventStreamDF.printSchema()
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC bodyDF = eventStreamDF.select(col("body").cast("STRING"))
# MAGIC display(bodyDF, streamName= "bodyDF")

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   if s.name == "bodyDF":
# MAGIC     s.stop()

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
# MAGIC
# MAGIC schema = StructType([
# MAGIC   StructField("Arrival_Time", LongType(), True),
# MAGIC   StructField("Creation_Time", LongType(), True),
# MAGIC   StructField("Device", StringType(), True),
# MAGIC   StructField("Index", LongType(), True),
# MAGIC   StructField("Model", StringType(), True),
# MAGIC   StructField("User", StringType(), True),
# MAGIC   StructField("gt", StringType(), True),
# MAGIC   StructField("x", DoubleType(), True),
# MAGIC   StructField("y", DoubleType(), True),
# MAGIC   StructField("z", DoubleType(), True),
# MAGIC   StructField("geolocation", StructType([
# MAGIC     StructField("PostalCode", StringType(), True),
# MAGIC     StructField("StateProvince", StringType(), True),
# MAGIC     StructField("city", StringType(), True),
# MAGIC     StructField("country", StringType(), True)
# MAGIC   ]), True),
# MAGIC   StructField("id", StringType(), True)
# MAGIC ])

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col, from_json
# MAGIC
# MAGIC parsedEventsDF = bodyDF.select(
# MAGIC   from_json(col("body"), schema).alias("json"))
# MAGIC
# MAGIC parsedEventsDF.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import from_unixtime
# MAGIC
# MAGIC flatSchemaDF = (parsedEventsDF
# MAGIC   .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
# MAGIC           (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
# MAGIC           col("json.Device").alias("Device"),
# MAGIC           col("json.Index").alias("Index"),
# MAGIC           col("json.Model").alias("Model"),
# MAGIC           col("json.User").alias("User"),
# MAGIC           col("json.gt").alias("gt"),
# MAGIC           col("json.x").alias("x"),
# MAGIC           col("json.y").alias("y"),
# MAGIC           col("json.z").alias("z"),
# MAGIC           col("json.id").alias("id"),
# MAGIC           col("json.geolocation.country").alias("country"),
# MAGIC           col("json.geolocation.city").alias("city"),
# MAGIC           col("json.geolocation.PostalCode").alias("PostalCode"),
# MAGIC           col("json.geolocation.StateProvince").alias("StateProvince"))
# MAGIC )
# MAGIC display(flatSchemaDF)

# COMMAND ----------

flatSchemaDF.writeStream\
   .format("delta")\
   .outputMode("append")\
   .option("checkpointLocation", "/iot/delta/checkpoints/")\
   .start("/user/aishu/sf_data")

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   s.stop()

# COMMAND ----------


sf_user=dbutils.secrets.get(scope="mykey",key="snowflakes-user")
sf_pwd=dbutils.secrets.get(scope="mykey",key="snowflakes-pwd")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

options={"sfUrl":"https://mzzrker-fu56496.snowflakecomputing.com/",
        "sfUser":sf_user,
        "sfPassword":sf_pwd,
        "sfDatabase":"test",
        "sfSchema":"src_output",
        "sfWarehouse":"COMPUTE_WH"}
def write_to_snowflake(src_df,tgt_tbl):
    src_df.write.format("snowflake").options(** options).option("dbtable",tgt_tbl).mode("append").save()

# COMMAND ----------

read_df=spark.read.format("delta").load("/user/aishu/sf_data")
write_to_snowflake(read_df,"iot_with_geo")
