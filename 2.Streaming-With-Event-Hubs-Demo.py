# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %run ./Streaming-DF

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

# MAGIC %md
# MAGIC ### Write Stream to Event Hub to Produce Stream

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
# MAGIC ehWriteConf = {
# MAGIC   'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
# MAGIC }
# MAGIC
# MAGIC checkpointPath = userhome + "/event-hub/write-checkpoint"
# MAGIC dbutils.fs.rm(checkpointPath,True)
# MAGIC
# MAGIC (activityStreamDF
# MAGIC   .writeStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**ehWriteConf)
# MAGIC   .option("checkpointLocation", checkpointPath)
# MAGIC   .start())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Stop all active streams

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   s.stop()
