# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("sleep_between_write", "10", "Sleep between each file")
dbutils.widgets.text("max_run_time_sec", "0", "Max generation file duration in sec (0 for instant generation, 3600 for 1h run max)")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Data Generation simulating stocks / Inventory
# MAGIC 
# MAGIC The purpose of this notebook is to generate a stream of inventory-relevant data originating from two simulated stores, one physical and the other online.  These data are transmitted to various data ingest services configured with the cloud provider as indicated in the *POS 01* notebook.
# MAGIC 
# MAGIC **Usage:**
# MAGIC 
# MAGIC The data is being saved under your home folder: **/Users/<firstname.lastname@xxx.com>/field_demos/retail/pos**. Make sure you set your DLT pipeline accordingly.
# MAGIC 
# MAGIC * `reset_all_data`: if true, we'll erase all data and restart from scratch
# MAGIC * `sleep_between_write`: how long we'll wait between each csv file write (to simulate the stream)
# MAGIC * `reset_all_data`: how long the fake stream will be running. Set 0 for instant generation. Max value is 1h.
# MAGIC 
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fpos_inventory%2Fnotebook_generation&dt=RETAIL_POS">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------


reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
sleep_between_write = int(dbutils.widgets.get("max_run_time_sec"))
assert sleep_between_write >= 0
max_run_time_sec = int(dbutils.widgets.get("max_run_time_sec"))
assert max_run_time_sec >= 0 and max_run_time_sec <=3600

from pyspark.sql.types import *
import pyspark.sql.functions as f

import datetime, time

# COMMAND ----------

# MAGIC %md This notebook is typically run to simulate a new stream of POS data. To ensure data from prior runs are not used in downstream calculations, you should reset the database and DLT engine environment between runs of this notebook:
# MAGIC 
# MAGIC **NOTE** These actions are not typical of a real-world workflow but instead are here to ensure the proper calculation of values in a simulation.

# COMMAND ----------

# DBTITLE 1,Step 3: Upload Data Files to DBFS
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile

def download_and_unzip(url, extract_to='.'):
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=extract_to)

if reset_all_data or len(dbutils.fs.ls(cloud_storage_path)) < 4:
  print(f'extracting data and saving content under {cloud_storage_path}')
  
  download_and_unzip('https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/point_of_sale_simulated.zip', '/tmp/pos')
  #dbutils.fs.mkdirs(f'/dbfs{cloud_storage_path}/generator')
  dbutils.fs.mv("file:/tmp/pos/inventory_change_online.txt", f'{cloud_storage_path}/generator/inventory_change_online.txt')  
  dbutils.fs.mv("file:/tmp/pos/inventory_change_store001.txt", f'{cloud_storage_path}/generator/inventory_change_store001.txt')  
  dbutils.fs.mv("file:/tmp/pos/inventory_snapshot_online.txt", f'{cloud_storage_path}/generator/inventory_snapshot_online.txt')   
  dbutils.fs.mv("file:/tmp/pos/inventory_snapshot_store001.txt", f'{cloud_storage_path}/generator/inventory_snapshot_store001.txt') 

  dbutils.fs.mv("file:/tmp/pos/inventory_change_type.txt", f'{cloud_storage_path}/inventory_change_type/inventory_change_type.txt') 
  dbutils.fs.mv("file:/tmp/pos/item.txt", f'{cloud_storage_path}/items/item.txt')  
  dbutils.fs.mv("file:/tmp/pos/store.txt", f'{cloud_storage_path}/stores/store.txt')

# COMMAND ----------

# MAGIC %md ## Step 1: Assemble Inventory Change Records
# MAGIC 
# MAGIC The inventory change records represent events taking place in a store location which impact inventory.  These may be sales transactions, recorded loss, damage or theft, or replenishment events. In addition, Buy-Online, Pickup In-Store (BOPIS) events originating in the online store and fulfilled by the physical store are captured in the data. While each of these events might have some different attributes associated with them, they have been consolidated into a single stream of inventory change event records. Each event type is distinguished through a change type identifier.
# MAGIC 
# MAGIC A given event may involve multiple products (items).  By grouping the data for items associated with an event around the transaction ID that uniquely identifies that event, the multiple items associated with a sales transaction or other event type can be efficiently transmitted:

# COMMAND ----------

# DBTITLE 1,Combine & Reformat Inventory Change Records
# format of inventory change records
inventory_change_schema = StructType([
  StructField('trans_id', StringType()),  # transaction event ID
  StructField('item_id', IntegerType()),  
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType()),
  StructField('change_type_id', IntegerType())
  ])

# inventory change record data files (one from each store)
inventory_change_files = [cloud_storage_path+'/generator/inventory_change_store001.txt',
                          cloud_storage_path+'/generator/inventory_change_online.txt']

# read inventory change records and group items associated with each transaction so that one output record represents one complete transaction
inventory_change = (
  spark
    .read
    .csv(
      inventory_change_files, 
      header=True, 
      schema=inventory_change_schema, 
      timestampFormat='yyyy-MM-dd HH:mm:ss'
      )
    .withColumn('trans_id', f.expr('substring(trans_id, 2, length(trans_id)-2)')) # remove surrounding curly braces from trans id
    .withColumn('item', f.struct('item_id', 'quantity')) # combine items and quantities into structures from which we can build a list
    .groupBy('date_time','trans_id')
      .agg(
        f.first('store_id').alias('store_id'),
        f.first('change_type_id').alias('change_type_id'),
        f.collect_list('item').alias('items')  # organize item info as a list
        )
    .orderBy('date_time','trans_id')
    .toJSON()
    .collect()
  )

# print a single transaction record to illustrate data structure
eval(inventory_change[0])

# COMMAND ----------

# MAGIC %md ##Step 2: Assemble Inventory Snapshots
# MAGIC 
# MAGIC Inventory snapshots represent the physical counts taken of products sitting in inventory.  
# MAGIC Such counts are periodically taken to capture the true state of a store's inventory and are necessary given the challenges most retailers encounter in inventory management.
# MAGIC 
# MAGIC With each snapshot, we capture basic information about the store, item, and quantity on-hand along with the date time and employee associated with the count.  So that the impact of inventory snapshots may be more rapidly reflected in our streams, we simulate a complete recount of products in a store every 5-days.  This is far more aggressive than would occur in the real world but again helps to demonstrate the streaming logic:

# COMMAND ----------

# DBTITLE 1,Access Inventory Snapshots
# format of inventory snapshot records
inventory_snapshot_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('employee_id', IntegerType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType())
  ])

# inventory snapshot files
inventory_snapshot_files = [cloud_storage_path+'/generator/inventory_snapshot_store001.txt',
                          cloud_storage_path+'/generator/inventory_snapshot_online.txt']


# read inventory snapshot data
inventory_snapshots = (
  spark
    .read
    .csv(
      inventory_snapshot_files, 
      header=True, 
      timestampFormat='yyyy-MM-dd HH:mm:ss.SSSXXX', 
      schema=inventory_snapshot_schema
      )
  ).cache()

display(inventory_snapshots)

# COMMAND ----------

# MAGIC %md To coordinate the transmission of change event data with periodic snapshots, the unique dates and times for which snapshots were taken within a given store location are extracted to a list.  This list will be used in the section of code that follows:
# MAGIC 
# MAGIC **NOTE** It is critical for the logic below that this list of dates is sorted in chronological order.

# COMMAND ----------

# DBTITLE 1,Assemble Set of Snapshot DateTimes by Store
# get date_time of each inventory snapshot by store
inventory_snapshot_times = (
  inventory_snapshots
    .select('date_time','store_id')
    .distinct()
    .orderBy('date_time')  # sorting of list is essential for logic below
  ).collect()

inventory_snapshot_times

# COMMAND ----------

# MAGIC %md ## Step 3: Transmit Store Data to the Cloud
# MAGIC 
# MAGIC In this step, we will send the event change JSON documents to our blob storage.  Using the time differences between transactions, we will delay the transmittal of each document by a calculated number of seconds. 
# MAGIC 
# MAGIC 
# MAGIC As events are transmitted, the data is checked to see if any snapshot files should be generated and sent into the cloud storage. In order to ensure the snapshot data is received in-sequence, any old snapshot files in the storage account are deleted before data transmission begins.
# MAGIC 
# MAGIC We'll fake the time and deliver our dataset at a fixed rate to ensure constant message delivery.

# COMMAND ----------

dbutils.fs.mkdirs(cloud_storage_path+ "/inventory_snapshots")
dbutils.fs.mkdirs(cloud_storage_path+ "/inventory_event")

# COMMAND ----------

if max_run_time_sec == 0:
  #Let's generate 50 files as miniumu
  events_per_file = len(inventory_change) / 50
else:
  events_per_file = len(inventory_change) / max_run_time_sec * sleep_between_write

# COMMAND ----------

inventory_snapshot_times_loop = inventory_snapshot_times

def write_events(file, events):
  with open(file, "w") as f:
    f.writelines(events)

events = []
for i, event in enumerate(inventory_change):
  # extract datetime from transaction document
  d = eval(event) # evaluate json as a dictionary
  dt = datetime.datetime.strptime( d['date_time'], '%Y-%m-%dT%H:%M:%S.000Z')
  
  inventory_event_file_name = f'/dbfs{cloud_storage_path}/inventory_event/events-{dt.strftime("%Y-%m-%d-%H-%M-%S")}.json'
  # inventory snapshot transmission
  # -----------------------------------------------------------------------
  for snapshot_dt, store_id in inventory_snapshot_times_loop: # for each snapshot    
    # if event date time is before next snapshot date time
    if dt < snapshot_dt: # (snapshot times are ordered by date)
      break              #   nothing to transmit
    else: # event date time exceeds a snapshot datetime
      if len(events) > 0:
        write_events(inventory_event_file_name, events)
        events = []
      # extract snaphot data for this dt
      snapshot_pd = (
        inventory_snapshots
          .filter(f.expr("store_id={0} AND date_time='{1}'".format(store_id, snapshot_dt)))
          .withColumn('date_time', f.expr("date_format(date_time, 'yyyy-MM-dd HH:mm:ss')")) # force timestamp conversion to include 
          .toPandas())
        
      # transmit to storage blob as csv
      snapshot_pd.to_csv(f'/dbfs{cloud_storage_path}/inventory_snapshots/inventory_snapshot_{store_id}_{snapshot_dt.strftime("%Y-%m-%d-%H-%M-%S")}.csv', index=False)
      # remove snapshot date from inventory_snapshot_times_loop
      inventory_snapshot_times_loop.pop(0)
      print('Loaded inventory snapshot for {0}'.format(snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')))
      
  # -----------------------------------------------------------------------
  # inventory change event transmission
  # -----------------------------------------------------------------------
  events.append(event)

  if len(events) > events_per_file:
    print(f"write {len(events)} events under {inventory_event_file_name} and sleep {sleep_between_write}sec")
    write_events(inventory_event_file_name, events)
    events = []
    time.sleep(sleep_between_write)
    
if len(events) > 0:
  print(f"write {len(events)} events under {inventory_event_file_name} and sleep {sleep_between_write}sec")
  write_events(inventory_event_file_name, events)

# COMMAND ----------

# MAGIC %md ## You're now ready to start your DLT pipeline ingesting this data!
