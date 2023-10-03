# Databricks Real-Time Streaming with Event Hubs and Snowflake

This project demonstrates real-time data ingestion and processing using Azure Event Hub, Databricks, and Snowflake. Leveraging Spark's structured streaming capabilities, the project showcases how to ingest, transform, and save streaming data, providing a blueprint for real-time analytics solutions.

## Overview

1. **Structured Streaming**: An introduction to Spark's structured streaming concepts is provided, showcasing how Spark handles real-time data processing.
2. **Azure Event Hub as Sink**: Streaming data is sent into Azure Event Hub, turning it into a data ingestion hub.
3. **Azure Event Hub as Source**: Data is continuously read from the Azure Event Hub, transformed, and processed in real-time using Spark on Databricks.
4. **Data Storage & Processing**: The transformed data is then stored in a Delta Lake format and further ingested into Snowflake for persistent storage and analytics.


## Steps

1. **Structured Streaming**:
   Use the `Structured-Streaming-Concepts.py` to understand the basic operations like defining a streaming DataFrame, performing transformations, and writing the transformed data.
  
2. **Data Ingestion to Event Hub**:
   Use `Streaming-With-Event-Hubs-Demo.py` to initialize the streaming DataFrame and write it to the Event Hub.
  
3. **Reading and Processing from Event Hub**:
   Use `Reading-from-Event-Hubs-Demo.py` to:
        1. Establish a connection to the Event Hub.
        2. Read data from the Event Hub into a streaming DataFrame.
        3. Define the schema of the incoming data.
        4. Transform and flatten the data.
        5. Store the data in Delta Lake format.
        6. Persist the data in Snowflake for further analytics.
