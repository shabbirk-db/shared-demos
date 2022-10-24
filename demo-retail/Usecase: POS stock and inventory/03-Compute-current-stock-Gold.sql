-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 4/Merge the snapshots with the incremental changes to build the final inventory table
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-full-flow-4.png" width="800px" style="float: right"/>
-- MAGIC 
-- MAGIC We saw in the [previous notebook]($./02-Stock-update-and-inventory-ingestion-silver) how to setup all the tables required (referential, snapshot and incremental stock update). 
-- MAGIC 
-- MAGIC Let's now build the final step an materialize our final inventory.
-- MAGIC 
-- MAGIC The logic is the following: 
-- MAGIC 
-- MAGIC * for all the items and all the stores, get the most recent quantity we have in the Inventory Snapshot table (absolute count, our reference).
-- MAGIC * Add to this value the `SUM` of all the incremental changes we received after the last inventory for all items/store
-- MAGIC 
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fpos_inventory%2Fnotebook_ingestion_dlt2&dt=RETAIL_POS">

-- COMMAND ----------

SET pipelines.trigger.interval = 5 minute;

CREATE LIVE TABLE inventory_current (
  CONSTRAINT incorrect_stock EXPECT (current_inventory > 0) -- let's track incorrect stocks over time (negative values) 
)
COMMENT 'calculate current inventory given the latest inventory snapshots and inventory-relevant events' 
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols' = 'store_id, item_id') 
AS
  SELECT 
      s.store_id,
      i.item_id,
      FIRST(i.name) as name,
      COALESCE(FIRST(s.quantity), 0) as snapshot_quantity,
      COALESCE(SUM(c.quantity), 0) as change_quantity,
      COALESCE(FIRST(s.quantity), 0) + COALESCE(SUM(c.quantity), 0) as current_inventory,
      FIRST(s.date_time) as last_inventory_date,
      GREATEST(FIRST(s.date_time), MAX(c.date_time)) as last_change_quantity_date
  FROM LIVE.item i 
   -- We want stock for all the stores
  CROSS JOIN LIVE.store
  -- Get the stocks from the most recent snapshots (if available)
  LEFT JOIN LIVE.latest_inventory_snapshot s ON s.item_id = i.item_id and s.store_id = store.store_id 
  -- add the incremental changes (only the one after the most recent inventory)
  LEFT JOIN (
    SELECT
        x.store_id,
        x.item_id,
        x.date_time,
        x.quantity
      FROM LIVE.inventory_change x
        INNER JOIN LIVE.store y ON x.store_id = y.store_id
      WHERE NOT(y.name = 'online' AND x.change_type = 'bopis') -- exclude bopis records from online store
      ) c 
    ON s.item_id = c.item_id AND 
       s.store_id = c.store_id AND
       (c.date_time >= s.date_time or s.date_time is null)
  GROUP BY i.item_id, s.store_id

-- COMMAND ----------

-- MAGIC %md It's important to note that the current inventory table is implemented using a 5-minute recalculation. While DLT supports near real-time streaming, the business objectives associated with the calculation of near current state inventories do not require up to the second precision.  Instead, 5-, 10- and 15-minute latencies are often preferred to give the data some stability and to reduce the computational requirements associated with keeping current.  From a business perspective, responses to diminished inventories are often triggered when values fall below a threshold that's well-above the point of full depletion (as lead times for restocking may be measured in hours, days or even weeks). With that in mind, the 5-minute interval used here exceeds the requirements of many retailers.

-- COMMAND ----------

select * from field_demos_retail.pos_inventory_current

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next steps
-- MAGIC 
-- MAGIC That's it! [Our end to end ingestion pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/896c8de6-6e2d-4028-95b0-f63f0bf7909c) is ready and our inventory table will be continuously updated by our DLT pipeline.
-- MAGIC 
-- MAGIC We can now leverage this data to start analyzing our current stock, but also it's evolution leveraging the history tables we keep.
-- MAGIC 
-- MAGIC This will open the door to multiple use-case on top of that and add intelligence / AI leveraging Databricks Lakehouse:
-- MAGIC 
-- MAGIC * Inventory tracking
-- MAGIC * Stock optimization
-- MAGIC * Demand forecasting
-- MAGIC * ...
-- MAGIC 
-- MAGIC Open the [DBSQL Inventory Dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/b30c82c4-d6cd-4533-be7b-9c69ab73352a-pos---stock-inventory-update?o=1444828305810485) and start analysing your existing inventory per store.
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/pos/pos-dashboard.png" width="1000px" />
