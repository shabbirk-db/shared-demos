# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import dlt
from mosaic import *
from pyspark.sql.functions import *

enable_mosaic(spark, dbutils)

taxi_zones_raw_path = spark.conf.get("mypipeline.taxi_zones_raw_path")
nyc_taxi_trips_table = spark.conf.get("mypipeline.nyc_taxi_trips_table")
H3resolution = spark.conf.get("mypipeline.H3resolution")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ingest Taxi Zone Boundary info
# MAGIC
# MAGIC - These are geoJSON datasets
# MAGIC - H3 Mosaic Indices are derived from the table

# COMMAND ----------

listTest = spark.conf.get("mypipeline.listTest")

@dlt.table(
  comment="Taxi Zone raw data"
  ,table_properties={"quality":"bronze"
                    }
)
def BZ_taxiZone_geojson():
  return (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format","json")
               .option("cloudFiles.inferColumnTypes","true") 
               .option("multiline","true")
               .load(taxi_zones_raw_path)
        )

# COMMAND ----------

@dlt.table(
  comment="Taxi Zone Neighbourhood definitions: H3 Indexed"
  ,table_properties={"quality":"silver"
                    ,"pipelines.autoOptimize.zOrderCols":"mosaic_index"
                    }
)
def SV_neighbourhoods():
  
  geojson_explode = dlt.read_stream("BZ_taxiZone_geojson").select("type",explode("features").alias("feature"))

  H3Index = (geojson_explode
             .withColumn("properties",col("feature.properties"))
             .withColumn("geometry",st_geomfromgeojson(to_json("feature.geometry")))
             .select("type"
                     ,"properties"
                     ,"geometry"
                     ,mosaic_explode("geometry", lit(int(H3resolution))).alias("mosaic_index")
                   )
            )
  
  return (H3Index)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ingest Taxi Trip Data
# MAGIC
# MAGIC - Calculate taxi trip pickups and dropoff locations from longitudes and latitudes

# COMMAND ----------

@dlt.view(
  comment="Raw Taxi Dataset for Geospatial calculations"
)
@dlt.expect_or_drop("valid trip", "dropoff_datetime >= pickup_datetime AND trip_distance >= 0")
@dlt.expect_or_drop("valid fare", "total_amount >= fare_amount AND total_amount >= 0")
@dlt.expect_or_drop("VTS vendor only", "vendor_id = 'VTS'")

def BZ_nycTaxiTrips():
  return spark.readStream.format("delta").load(nyc_taxi_trips_table)

# COMMAND ----------

@dlt.table(
  comment="Geometry enrichment, H3 indexing"
  ,table_properties={"quality":"silver"
                    ,"pipelines.autoOptimize.zOrderCols":"['pickup_h3','dropoff_h3']"
                    }
)
def SV_nycTaxiTrips():
  
  taxi_trip_geometries = (dlt.read_stream("BZ_nycTaxiTrips")
                           .withColumn("pickup_geom",st_point('pickup_longitude', 'pickup_latitude'))
                           .withColumn("dropoff_geom",st_point('dropoff_longitude', 'dropoff_latitude'))
                           .withColumn("pickup_h3", point_index_geom("pickup_geom", lit(int(H3resolution))))
                           .withColumn("dropoff_h3", point_index_geom("dropoff_geom", lit(int(H3resolution))))
                           .withColumn("trip_line", st_makeline(array("pickup_geom", "dropoff_geom")))
                           .withColumn("time_elapsed",expr("timestampdiff(MINUTE,pickup_datetime,dropoff_datetime)"))
                           .withColumn("monthID",date_format(col('pickup_datetime'), 'yyyyMM'))
                           .select(
                               "trip_distance"
                              ,"total_amount"
                              ,"monthID"
                              ,"pickup_datetime"
                              ,"dropoff_datetime"
                              ,"time_elapsed"
                              ,"pickup_geom"
                              ,"dropoff_geom"
                              ,"pickup_h3"
                              ,"dropoff_h3"
                              ,"trip_line"
                            )
                         )
  return(taxi_trip_geometries)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Perform spatial join to match the trips to the zones

# COMMAND ----------

@dlt.table(
  comment="Geospatial Taxi Analytics Ready Dataset"
  ,table_properties={"quality":"silver"
                     ,"pipelines.autoOptimize.zOrderCols":"['pickup_h3','dropoff_h3']"
                    }
)
def SV_spatialJoin(): 
  
  pickupJoinCondition = [col('p.mosaic_index.index_id') == col('pickup_h3')]
  dropoffJoinCondition = [col('d.mosaic_index.index_id') == col('dropoff_h3')]
  
  spatialJoin = (dlt.read_stream("SV_nycTaxiTrips")
             .join(dlt.read("SV_neighbourhoods").alias('p').withColumn("pickup_zone",col("p.properties.zone")),pickupJoinCondition)
             .join(dlt.read("SV_neighbourhoods").alias('d').withColumn("dropoff_zone",col("d.properties.zone")),dropoffJoinCondition)
             .where((col("p.mosaic_index.is_core"))
                    | (st_contains(col("p.mosaic_index.wkb"), col("pickup_geom")))
                    | (st_contains(col("d.mosaic_index.wkb"), col("dropoff_geom")))
                   )
             .select("trip_distance"
                    ,"total_amount"
                    ,"monthID"
                    ,"pickup_datetime"
                    ,"dropoff_datetime"
                    ,"time_elapsed" 
                    ,"pickup_geom"
                    ,"dropoff_geom"
                    ,"pickup_h3"
                    ,"dropoff_h3"
                    ,"pickup_zone"
                    ,"dropoff_zone"
                    ,"trip_line"
                    )
                )
  
  return (spatialJoin)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Gold layer of aggregate views

# COMMAND ----------

@dlt.table(
  comment="Taxi monthly summary table"
  ,table_properties={"quality":"gold"
                     ,"pipelines.autoOptimize.zOrderCols":"monthID"
                    }
)
def GL_pickupZone_monthlySummary():
  
  pickup_aggregates = (dlt.read("SV_spatialJoin")
                          .groupBy("pickup_zone","monthID")
                          .agg(sum("total_amount").alias("total_taxi_revenue")
                               ,avg("total_amount").alias("avg_fare")
                               ,avg("trip_distance").alias("avg_distance")
                               ,avg("time_elapsed").alias("avg_time_elapsed_mins")
                               ,max("trip_distance").alias("max_distance")
                               ,max("time_elapsed").alias("max_time_elapsed_mins")
                              )
                      )
  return(pickup_aggregates)

# COMMAND ----------

@dlt.table(
  comment="Daily summary table"
  ,table_properties={"quality":"gold"}
)
def GL_dailySummary():
  
  daily_aggregates = (dlt.read("SV_spatialJoin")
                          .withColumn("pickup_date",date_format(col("pickup_datetime"),'yyyy-MM-dd'))
                          .groupBy("pickup_date")
                          .agg(sum("total_amount").alias("total_taxi_revenue")
                               ,avg("total_amount").alias("avg_fare")
                               ,avg("trip_distance").alias("avg_distance")
                               ,avg("time_elapsed").alias("avg_time_elapsed_mins")
                               ,max("trip_distance").alias("max_distance")
                               ,max("time_elapsed").alias("max_time_elapsed_mins")
                              )
                      )
  return(daily_aggregates)
