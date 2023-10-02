# Databricks notebook source
# MAGIC %run "./Dataset-Mounts1"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Reading a Stream</h2>
# MAGIC

# COMMAND ----------

# Here we define the schema using a DDL-formatted string (the SQL Data Definition Language).
dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

# COMMAND ----------

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
initialDF = (spark
  .readStream                            # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger
  .schema(dataSchema)                    # Required for all streaming DataFrames
  .json(dataPath)                        # The stream's source directory and file type
)

# COMMAND ----------

streamingDF = (initialDF
  .withColumnRenamed("Index", "User_ID")  # Pick a "better" column name
  .drop("_corrupt_record")                # Remove an unnecessary column
)

# COMMAND ----------

# Static vs Streaming?
streamingDF.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsupported Operations
# MAGIC
# MAGIC For more details, refer
# MAGIC "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" 

# COMMAND ----------

from pyspark.sql.functions import col

try:
  sortedDF = streamingDF.orderBy(col("Recorded_At").desc())
  display(sortedDF)
except:
  print("Sorting is not supported on an unaggregated stream")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Streaming the output</h2>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Triggers
# MAGIC
# MAGIC The trigger specifies when the system should process the next set of data.
# MAGIC
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | _DEFAULT_- The query will be executed as soon as the system has completed processing the previous query |
# MAGIC | Fixed interval micro-batches           | `.trigger(Trigger.ProcessingTime("2 hours"))` | The query will be executed in micro-batches at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch and then stop|
# MAGIC | Continuous with fixed checkpoint interval | `.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Output Sinks
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Let's Do Some Streaming</h2>
# MAGIC
# MAGIC In the cell below, we write data from a streaming query to `outputPathDir`.
# MAGIC
# MAGIC There are a couple of things to note below:
# MAGIC 0. We are giving the query a name via the call to `.queryName` 
# MAGIC 0. Spark begins running jobs once we call `.start`
# MAGIC 0. The call to `.start` returns a `StreamingQuery` object

# COMMAND ----------

userhome = "dbfs:/user/hxu"
basePath = userhome + "/structured-streaming/python" # A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                   # Make sure that our working directory exists
outputPathDir = basePath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = basePath + "/checkpoint"                     # A subdirectory for our checkpoint & W-A logs

streamingQuery = (streamingDF                                 # Start with our "streaming" DataFrame
  .writeStream                                                # Get the DataStreamWriter
  .queryName("iot_streaming")                                     # Name the query
  .trigger(processingTime="3 seconds")                        # Configure for a 3-second micro-batch
  .format("parquet")                                          # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)               # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                       # Write only new data to the "file"
  .start(outputPathDir)                                       # Start the job, writing to the specified directory
)

# COMMAND ----------

streamingQuery.recentProgress

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
  print("{}: {}".format(s.id, s.name)) # Print the stream's id and name

# COMMAND ----------

streamingQuery.awaitTermination(300)                      

# COMMAND ----------

streamingQuery.stop() 

# COMMAND ----------

myStream = "iot_streaming_p2"
display(streamingDF, streamName = myStream)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the `streamName` get's registered as a temporary table pointing to the memory sink, we can use SQL to query the sink.

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.sql("select * from iot_streaming_p2").show()

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>Clean up our directories</h2>

# COMMAND ----------

dbutils.fs.rm(basePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2> Additional Resources</h2>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a>
# MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#id2" target="_blank">Failed Streaming Query Recovery</a>
# MAGIC
# MAGIC
