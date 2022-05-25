# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# sql_workshop_shabbir_khanbhai_databricks_com_db

source_db = spark.conf.get("source_db")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest Airlines data

# COMMAND ----------

def generate_table(id,origin_airport_name):

    @dlt.table(
        name=f"airport_split_{id}",
        table_properties={"quality":"bronze"}
    )
    def BZ_event_split():
      
        step_1 = (spark.readStream.table(f'{source_db}.airline_trips_silver')
                  .filter(col('origin_airport_name') == f"{origin_airport_name}")
                 )
          
        return (
            step_1.withColumn("data_payload",lit("testing123123"))
        )

# COMMAND ----------

airports_list = list(spark.read.table(f'{source_db}.airline_trips_silver').select("origin_airport_name").drop_duplicates().toPandas()['origin_airport_name'])

[generate_table(e,a) for e,a in enumerate(airports_list[0:60])]
