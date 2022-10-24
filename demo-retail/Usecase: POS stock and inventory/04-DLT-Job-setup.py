# Databricks notebook source
# MAGIC %md ## Setup the DLT pipeline:
# MAGIC 
# MAGIC Let's setup our DLT pipeline. Open the Workflows menu, click on Delta Live Table and add the following setup:
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "edition": "advanced",
# MAGIC     "photon": false,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/<xxx.xxx@xxx.com>/field-demo/demo-retail/Usecase: POS stock and inventory/02-Stock-update-and-inventory-ingestion-silver"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/<xxx.xxx@xxx.com>/field-demo/demo-retail/Usecase: POS stock and inventory/03-Compute-current-stock-Gold"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "retail_pos_stock",
# MAGIC     "target": "<xxx>"
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fpos_inventory%2Fnotebook_ingestion_dlt3&dt=RETAIL_POS">
