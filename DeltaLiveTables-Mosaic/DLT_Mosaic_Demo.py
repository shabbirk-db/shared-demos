# Databricks notebook source
# MAGIC %pip install /dbfs/FileStore/shabbir/mosaic_demo/databricks_mosaic-0.1.1-py3-none-any.whl

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

@dlt.table(
  comment="Taxi Zone raw data"
  ,table_properties={"quality":"bronze"}
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
  
  return (geojson_explode
             .withColumn("properties",col("feature.properties"))
             .withColumn("geometry",st_astext(st_geomfromgeojson(to_json("feature.geometry"))))
             .select("type"
                     ,"properties"
                     ,"geometry"
                     ,mosaic_explode("geometry", lit(int(H3resolution))).alias("mosaic_index")
                   )
         )

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
  return spark.read.format("delta").load(nyc_taxi_trips_table)

# COMMAND ----------

@dlt.table(
  comment="Geometry enrichment, H3 indexing"
  ,table_properties={"quality":"silver"
                    ,"pipelines.autoOptimize.zOrderCols":"['pickup_h3','dropoff_h3']"
                    }
)
def SV_nycTaxiTrips():
  
  taxi_trip_geometries = (dlt.read("BZ_nycTaxiTrips")
                           .withColumn("pickup_geom",st_astext(st_point('pickup_longitude', 'pickup_latitude')))
                           .withColumn("dropoff_geom",st_astext(st_point('dropoff_longitude', 'dropoff_latitude')))
                           .select("trip_distance"
                                  ,"pickup_datetime"
                                  ,"dropoff_datetime"
                                  ,st_astext(st_point('pickup_longitude', 'pickup_latitude')).alias('pickup_geom')
                                  ,st_astext(st_point('dropoff_longitude', 'dropoff_latitude')).alias('dropoff_geom') 
                                  ,"total_amount" 
                                  )
                         )
  return (
                    taxi_trip_geometries
                        .select("trip_distance"
                                ,"total_amount"
                                ,"pickup_datetime"
                                ,"dropoff_datetime"
                                ,"pickup_geom"
                                ,"dropoff_geom"
                                ,point_index_geom("pickup_geom", lit(int(H3resolution))).alias('pickup_h3')
                                ,point_index_geom("dropoff_geom", lit(int(H3resolution))).alias('dropoff_h3')
                                ,st_makeline(array("pickup_geom", "dropoff_geom")).alias('trip_line')
                                )
         )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Perform spatial join to match the trips to the zones

# COMMAND ----------

@dlt.table(
  comment="Geospatial Taxis Summary Dataset"
  ,table_properties={"quality":"gold"}
)
def GL_spatialJoin(): 
  
  pickupJoinCondition = [col('p.mosaic_index.index_id') == col('pickup_h3')]
  dropoffJoinCondition = [col('d.mosaic_index.index_id') == col('dropoff_h3')]
  
  return (dlt.read("SV_nycTaxiTrips")
             .join(dlt.read("SV_neighbourhoods").alias('p').withColumn("pickup_zone",col("p.properties.zone")),pickupJoinCondition)
             .join(dlt.read("SV_neighbourhoods").alias('d').withColumn("dropoff_zone",col("d.properties.zone")),dropoffJoinCondition)
             .where((col("p.mosaic_index.is_core"))
                    | (st_contains(col("p.mosaic_index.wkb"), col("pickup_geom")))
                    | (st_contains(col("d.mosaic_index.wkb"), col("dropoff_geom")))
                   )
             .select("trip_distance"
                    ,"pickup_geom"
                    ,"dropoff_geom"
                    ,"pickup_h3"
                    ,"dropoff_h3"
                    ,"pickup_zone"
                    ,"dropoff_zone"
                    ,"trip_line"
                    )
         )
