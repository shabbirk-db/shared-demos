-- Databricks notebook source
-- DBTITLE 1,Bronze: Stream raw airlines data with Autoloader
CREATE INCREMENTAL LIVE TABLE airline_trips_bronze
  (CONSTRAINT quarantine_rescued_data EXPECT (_rescued_data IS NULL)) --Provides a count of rescued data from auto loader
COMMENT 'Bronze table for airlines ETL'
AS SELECT * FROM cloud_files('${userhome}/airlines/'
                              ,"json"
                            )

-- COMMAND ----------

-- DBTITLE 1,Import an Airport Names mapping table
CREATE LIVE TABLE airport_codes
COMMENT 'Airport IATA Codes static table - Imported from online source'
AS
SELECT *
FROM json.`${userhome}/iata_data/airport_codes.json`

-- COMMAND ----------

-- DBTITLE 1,Import an Airline Names mapping table
CREATE LIVE TABLE airline_codes
  (CONSTRAINT corrupt_records EXPECT (_corrupt_record IS NULL) ON VIOLATION DROP ROW
  ,CONSTRAINT dupe_checks EXPECT (dupes_check = 1) ON VIOLATION DROP ROW
  )
COMMENT 'Airline IATA Codes static table - Imported from online source'
AS
SELECT *
,ROW_NUMBER() OVER (PARTITION BY iata ORDER BY name) dupes_check
FROM json.`${userhome}/iata_data/airline_codes.json`
WHERE iata IS NOT NULL
  AND iata != ''
  AND active = 'Y'

-- COMMAND ----------

-- DBTITLE 1,Silver: Combine Bronze and Mappings to generate a cleansed, analytics-ready table
CREATE INCREMENTAL LIVE TABLE airline_trips_silver
  (CONSTRAINT valid_month EXPECT (Month BETWEEN 1 AND 12) ON VIOLATION DROP ROW
   ,CONSTRAINT valid_dayOfWeek EXPECT (DayOfWeek BETWEEN 1 AND 7) ON VIOLATION DROP ROW
   ,CONSTRAINT valid_dayOfMonth EXPECT (DayOfMonth BETWEEN 1 AND 31) ON VIOLATION DROP ROW
  ) 
COMMENT 'Silver table for airlines ETL, Z-Ordered and enriched'
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = 'Date')
AS 
SELECT 
   ActualElapsedTime
  ,ArrDelay::INT
  ,ArrTime 
  ,CRSArrTime 
  ,CRSDepTime 
  ,CRSElapsedTime 
  ,Cancelled::INT
  ,DayOfWeek
  ,DayOfMonth
  ,Month
  ,Year
  ,DepDelay::INT
  ,DepTime 
  ,Dest 
  ,Distance 
  ,Diverted::INT
  ,FlightNum 
  ,IsArrDelayed 
  ,IsDepDelayed
  ,Origin 
  ,UniqueCarrier
  ,TO_DATE(STRING(INT(Year*10000+Month*100+DayofMonth)),'yyyyMMdd') AS Date
  ,airlines.name as airline_name
  ,origin.municipality origin_city
  ,origin.name as origin_airport_name
  ,origin.elevation_ft::INT origin_elevation_ft
  ,split(origin.coordinates,',') as origin_coordinates_array
  ,dest.municipality dest_city
  ,dest.name as dest_airport_name
  ,dest.elevation_ft::INT dest_elevation_ft
  ,split(dest.coordinates,',') as dest_coordinates_array
FROM STREAM(LIVE.airline_trips_bronze) raw
LEFT JOIN LIVE.airport_codes origin
  ON raw.Origin = origin.iata_code
LEFT JOIN LIVE.airport_codes dest
  ON raw.Dest = dest.iata_code  
LEFT JOIN LIVE.airline_codes airlines
  ON raw.UniqueCarrier = airlines.iata
  

-- COMMAND ----------

-- DBTITLE 1,Delays summary - SCD Type 1
CREATE INCREMENTAL LIVE TABLE flight_delays_scd1;

APPLY CHANGES INTO LIVE.flight_delays_scd1
FROM STREAM(LIVE.airline_trips_silver)
KEYS (airline_name,origin_city,dest_city)
SEQUENCE BY Date
COLUMNS airline_name,origin_city,dest_city,arrtime,IsArrDelayed
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,Delays summary - SCD Type 2
CREATE INCREMENTAL LIVE TABLE flight_delays_scd2;

APPLY CHANGES INTO LIVE.flight_delays_scd2
FROM STREAM(LIVE.airline_trips_silver)
KEYS (airline_name,origin_city,dest_city)
SEQUENCE BY Date
COLUMNS airline_name,origin_city,dest_city,arrtime,IsArrDelayed
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- DBTITLE 1,Check: No duplicate rows created
CREATE LIVE TABLE check_no_duplicates
  (
  CONSTRAINT no_duplicates EXPECT (bronze_count >= silver_count)
  ) 
  COMMENT 'Check no duplication between bronze and silver'
  AS
  SELECT * FROM
  (SELECT COUNT(*) AS bronze_count FROM LIVE.airline_trips_bronze),
  (SELECT COUNT(*) AS silver_count FROM LIVE.airline_trips_silver)

-- COMMAND ----------

-- DBTITLE 1,Check: No rows unintentionally lost
CREATE LIVE TABLE check_no_rows_dropped
  (
  CONSTRAINT no_rows_dropped EXPECT (bronze_count <= silver_count)
  ) 
  COMMENT 'Check no dropped rows between bronze and silver'
  AS 
  SELECT * FROM
  (SELECT COUNT(*) AS bronze_count FROM LIVE.airline_trips_bronze),
  (SELECT COUNT(*) AS silver_count FROM LIVE.airline_trips_silver)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Prepare your DLT Pipeline
-- MAGIC 
-- MAGIC ### 1. Create a new pipeline with the UI
-- MAGIC 
-- MAGIC - You can use your generated "userhome" and "database" names from the 00_SimulatedStream step
-- MAGIC - Your "user-folder" will your personal workspace loation"
-- MAGIC - Pipeline Mode:
-- MAGIC   - Select "triggered" to control the frequency of the updates. Less frequent runs will help control costs
-- MAGIC     - Frequency can be controlled by adding a configuration, but we can skip this for now
-- MAGIC   - Select "continuous" to simulate a fully real-time environment.
-- MAGIC 
-- MAGIC <img src="https://github.com/shabbirk-db/public-resources/blob/main/DLT_Pipeline_Setup.png?raw=true" width=400 alt="Example pipeline"/>
-- MAGIC 
-- MAGIC ***
-- MAGIC 
-- MAGIC ### 2. Your new DLT Pipeline should look like this:
-- MAGIC 
-- MAGIC - Once triggered, your pipeline will generate the key table for our analytics, your-database-name.airline_trips_silver
-- MAGIC 
-- MAGIC <img src="https://github.com/shabbirk-db/public-resources/blob/main/DLT_Pipeline_DAG.png?raw=true" width=1000 alt="Example pipeline"/>
-- MAGIC 
-- MAGIC ***
-- MAGIC 
-- MAGIC ### 3. Continue the demo in Databricks SQL
-- MAGIC 
-- MAGIC 
-- MAGIC - Some example queries and visualisations have already been packaged up into a Dashboard for you
-- MAGIC - These queries will demonstrate: 
-- MAGIC   - Creating a mix of simple and complex, parameterised queries
-- MAGIC   - Generating charts, maps and more from our data with only a few clicks
-- MAGIC   - Setting up near real-time updating to keep data up-to-date
-- MAGIC     - This can then be tested by setting the Simulated Stream to continuous, letting DLT process the data through our ETL and letting Databricks SQL automatically refresh the Dashboard as the data lands

-- COMMAND ----------


