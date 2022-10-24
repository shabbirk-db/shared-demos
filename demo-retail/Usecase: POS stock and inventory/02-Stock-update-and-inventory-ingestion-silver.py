# Databricks notebook source
# MAGIC %md 
# MAGIC # Inventory & stock update
# MAGIC 
# MAGIC 
# MAGIC Stock update and inventory reconciliation can be a real challenge for retailers.
# MAGIC 
# MAGIC A typical retail system has 2 main input source to update their stock
# MAGIC 
# MAGIC * The stock inventory, typically updated with a low frequency and containing the snapshot of the absolute stock value
# MAGIC * The stock update, with relative value (ex: a sell will send a -1 stock update, and a resupply +10)
# MAGIC 
# MAGIC We then need to reconciliate these 2 datasources to materialize the most accurate inventory stock. For each item, we get the last recent inventory stock and add/substract all the relative changes:
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos_high_level.png" width="1000px" />
# MAGIC 
# MAGIC Once the final inventory is materialized, it opens to multiple use-cases:
# MAGIC 
# MAGIC * Inventory optimization
# MAGIC * Cross-store stock optimization
# MAGIC * Demand forecasting
# MAGIC * ...
# MAGIC 
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fpos_inventory%2Fnotebook_ingestion_dlt1&dt=RETAIL_POS">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta Live Table (DLT): a simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC 
# MAGIC Ingesting data in streaming and applying ML on top of it can be a real challenge. <br/>
# MAGIC Delta Live Table and the Lakehouse simplify Data Engineering by handling all the complexity for you, while you can focus on your business transformation.
# MAGIC 
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC 
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC 
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC 
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC 
# MAGIC ## Implementing an end to end Inventory pipeline with Delta Live Table
# MAGIC 
# MAGIC In this demo, we'll be implementing a pipeline to handle stock update end to end using Delta Live Table:
# MAGIC 
# MAGIC - 1/ Setup the reference tables (semi static, items/store/change type)
# MAGIC - 2/ Load the inventory snapshots
# MAGIC - 3/ Load the incremental changes
# MAGIC - 4/ Merge the snapshots with the incremental changes to build the final inventory table
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-full-flow-0.png" width="1000px"/></div>
# MAGIC 
# MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c72782ae-982a-4308-8d00-93dcf36a3519/updates/2d250b66-42fc-43c9-98df-d67653b00f12) to see this flow in action
# MAGIC 
# MAGIC *Note: Make sure you run the init cell in the [01-Data-Generation]($./01-Data-Generation) notebook to load the data.*

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import dlt # this is the delta live tables library
import pyspark.sql.functions as f
from pyspark.sql.types import *
from delta.tables import *
import time

# COMMAND ----------

# DBTITLE 1,Data source - Use the generator to import data
#the notebook 01 will generate data under your home folder. We'll use this folder as landing input for our DLT pipeline 
current_user = "quentin.ambard@databricks.com"
cloud_storage_path = f"/Users/{current_user}/field_demos/retail/pos"

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC ## 1/ Loading the Static Reference Data
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-full-flow-1.png" width="800px" style="float: right"/>
# MAGIC 
# MAGIC Our pipeline require access to reference tables. These dataset are relatively stable so that we might update them just once daily. 
# MAGIC 
# MAGIC Let's start by loading the 3 tables using DLT decorators:
# MAGIC 
# MAGIC * The Stores (with store ID and names)
# MAGIC * The Items (with items ID, names, ...)
# MAGIC * The type change (change ID with it's name in a human-readabmle format: "online sell" etc)
# MAGIC 
# MAGIC These tables are coming as `csv` files in our cloud storage.
# MAGIC 
# MAGIC ### DLT and Databricks Autoloader (cloudFiles)
# MAGIC 
# MAGIC To create these reference tables, we'll be using DLT decoratos and simply return a Spark stream with `cloudFiles` 
# MAGIC 
# MAGIC The *name* element associated with the *@dlt.table* decorator identifies the name of the table to be created by the DLT engine.
# MAGIC 
# MAGIC CloudFiles will incrementally load new upcoming data if our referential table are updated, handling schema inference & evolution for us out of the box.
# MAGIC 
# MAGIC *NOTE: For more information on the DLT Python specification, please refer to [this document](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-python-ref).*

# COMMAND ----------

# DBTITLE 1,Store table
#define the dlt table
@dlt.table(
  name='store', # name of the table to create
  comment = 'data associated with individual store locations', # description
  spark_conf = {'pipelines.trigger.interval': '24 hours'}) # this will only be refreshed every 24 hours
def store():
  return (spark.readStream
                 .format("cloudFiles")
                 .option("header", "true")
                 .option("cloudFiles.format", "csv")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .load(f'{cloud_storage_path}/stores'))

# COMMAND ----------

# DBTITLE 1,Item table
@dlt.table(
  name = 'item',
  comment = 'data associated with individual items',
  spark_conf={'pipelines.trigger.interval': '24 hours'})
def item():
  return (spark.readStream
                 .format("cloudFiles")
                 .option("header", "true")
                 .option("cloudFiles.format", "csv")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .load(f'{cloud_storage_path}/items'))

# COMMAND ----------

# MAGIC %md And lastly, we can write a DLT table definition for our inventory change type data:

# COMMAND ----------

# DBTITLE 1,Inventory Change Types table
@dlt.table(
  name = 'change_type_mapping',
  comment = 'data mapping change type id values to descriptive strings',
  spark_conf={'pipelines.trigger.interval': '24 hours'}
)
def inventory_change_type():
  return (spark.readStream
                 .format("cloudFiles")
                 .option("header", "true")
                 .option("cloudFiles.format", "csv")
                 .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaHints", "change_type_id bigint")
                 .load(f'{cloud_storage_path}/inventory_change_type'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Ingest Inventory Snapshots
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-full-flow-2.png" width="800px" style="float: right"/>
# MAGIC 
# MAGIC 
# MAGIC Periodically, we receive absolute counts of items in inventory at a given store location.  
# MAGIC 
# MAGIC Such inventory snapshots are frequently used by retailers to update their understanding of which products are actually on-hand given concerns about the reliability of calculated inventory quantities.  
# MAGIC 
# MAGIC We may wish to preserve both a full history of inventory snapshots received and the latest counts for each product in each store location. <br />To meet this need, two separate tables are built from this:
# MAGIC 
# MAGIC * one with all the history (simple `APPEND` mode).
# MAGIC * one containing the most recent inventory per store (`UPSERT` based on `item_id` and `store_id`)
# MAGIC 
# MAGIC 
# MAGIC These inventory snapshot data arrive in this environment as CSV files on a slightly irregular basis. 
# MAGIC 
# MAGIC But as soon as they land, we will want to process them, making them available to support more accurate estimates of current inventory. To enable this, we will take advantage of the Databricks [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) feature which listens for incoming files to a storage path and processes the data for any newly arriving files as a stream. 

# COMMAND ----------

# DBTITLE 1,Access Incoming Snapshots
#Create a view to incrementally load the raw data (csv files, store-based inventory)
@dlt.view(name='raw_inventory_snapshot')
def raw_inventory_snapshot():
  return (
    spark
      .readStream
      .format('cloudFiles')  # auto loader
      .option('cloudFiles.format', 'csv')
      .option('cloudFiles.includeExistingFiles', 'true') 
      .option("cloudFiles.schemaHints", "date_time timestamp")
      .load(f'{cloud_storage_path}/inventory_snapshots'))
  
#Append the data to our inventory history table
@dlt.table(
  name='history_inventory',
  comment='data representing periodic counts of items in inventory')
def history_inventory():
  return dlt.read_stream("raw_inventory_snapshot")

# COMMAND ----------

# MAGIC %md 
# MAGIC For the purposes of calculating things like current inventory, we only need the latest count of an item in a given location.  We can create a DLT table containing this subset of data in a relatively simple manner using the [*apply_changes()*](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-cdc) method.
# MAGIC 
# MAGIC The `apply_changes()` method is part of Delta Live Table's change data capture functionality. This is will automatically run a UPSERT (MERGE) operation based on `store_id` and `item_id`, handling all technical details for us.

# COMMAND ----------

# DBTITLE 1,Upsert to build the Latest Inventory Snapshot table
# create dlt table to hold latest inventory snapshot (if it doesn't exist)
dlt.create_target_table('latest_inventory_snapshot')

# merge incoming snapshot data with latest
dlt.apply_changes(
  target = 'latest_inventory_snapshot',
  source = 'raw_inventory_snapshot',
  keys = ['store_id', 'item_id'], # match source to target records on these keys
  sequence_by = 'date_time' # determine latest value by comparing date_time field
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 3/ Stream Inventory Change Events
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-full-flow-3.png" width="800px" style="float: right"/>
# MAGIC 
# MAGIC 
# MAGIC Let's now tackle our inventory change event data. These data consist of a JSON document transmitted by a store to summarize an event with inventory relevance. 
# MAGIC 
# MAGIC Everytime a store receive a new stock or sell an item, they send this information to our streaming system with the relative information (ex: -1 for a sell)
# MAGIC 
# MAGIC These events may represent sales, restocks, or reported loss, damage or theft (shrinkage).  A fourth event type, *bopis*, indicates a sales transaction that takes place in the online store but which is fulfilled by a physical store. All these events are transmitted as part of a consolidated stream:
# MAGIC 
# MAGIC As 1 message can contain multiple items (ex: 1 command contain multiple items), we need to add an extra step to explode this data and remove potential duplicates.
# MAGIC 
# MAGIC *Note: To simplify our demo, we'll reading the data from a cloud storage, but this could simply be done with DLT (just set `kafka` as input format with your kafka credential as option)*

# COMMAND ----------

@dlt.view(
  name = 'raw_inventory_change',
  comment = 'data representing item-level inventory changes originating from the POS')
def raw_inventory_change():
  return (spark.readStream
                  .format('cloudFiles')  # auto loader
                  .option('cloudFiles.format', 'json')
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaHints", "date_time timestamp, change_type_id bigint")
                  .load(f'{cloud_storage_path}/inventory_event'))
  
  
@dlt.table(
  name = 'inventory_change',
  comment = 'data representing item-level inventory changes originating from the POS')
def inventory_change():
  return (dlt.read_stream('raw_inventory_change')
               .withColumn('item', f.explode('items'))
               .withColumn('item_id', f.col('item.item_id'))
               .withColumn('quantity', f.col('item.quantity'))
               .drop("items", "item")
               .withWatermark('date_time', '1 hour') # ignore any data more than 1 hour old flowing into deduplication
                 .dropDuplicates(['trans_id','item_id'])  # drop duplicates 
               .join(dlt.read("change_type_mapping").drop('_rescued_data'), "change_type_id")
         )

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## 4/ Next:  Merge the snapshots with the incremental changes to build the final inventory table
# MAGIC 
# MAGIC We have now all the data ready to build our final Inventory table. We can start reviewing the existing [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/896c8de6-6e2d-4028-95b0-f63f0bf7909c).
# MAGIC 
# MAGIC Let's see how DLT can do that using a simple SQL query in the [03-Compute-current-stock-Gold]($./03-Compute-current-stock-Gold) notebook.
