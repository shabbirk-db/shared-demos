# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC # Downloading the data from Kaggle
# MAGIC 
# MAGIC ## Obtain KAGGLE_USERNAME and KAGGLE_KEY for authentication
# MAGIC 
# MAGIC * Instructions on how to obtain this information can be found [here](https://www.kaggle.com/docs/api).
# MAGIC 
# MAGIC * This information will need to be entered below. Please use [secret](https://docs.databricks.com/security/secrets/index.html) to avoid having your key as plain text
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_setup_data&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to setup data.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ./_kaggle_credential

# COMMAND ----------

if "KAGGLE_USERNAME" not in os.environ or os.environ['KAGGLE_USERNAME'] == "" or "KAGGLE_KEY" not in os.environ or os.environ['KAGGLE_KEY'] == "":
  print("You need to specify your KAGGLE USERNAME and KAGGLE KEY to download the data")
  print("Please open notebook under ./_resources/01_download and sepcify your Kaggle credential")
  dbutils.notebook.exit("ERROR: Kaggle credential is required to download the data. Please open notebook under ./_resources/kaggle_credential and specify your Kaggle credential")

# COMMAND ----------

dbutils.widgets.text("cloud_storage_path", "/demos/propensity/", "Cloud Storage Path")
try:
    cloud_storage_path
except NameError:
    cloud_storage_path = dbutils.widgets.get("cloud_storage_path")
if cloud_storage_path.endswith("/"):
  cloud_storage_path = cloud_storage_path[:-1]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dunnhumby the complete journey Dataset
# MAGIC 
# MAGIC * The dataset used in this accelerator is from [Kaggle: Dunnhumby the complete journey Dataset](https://www.kaggle.com/datasets/frtgnn/dunnhumby-the-complete-journey). 
# MAGIC 
# MAGIC   
# MAGIC * Further details about this dataset
# MAGIC   * Dataset title: Dunnhumby the complete journey Dataset
# MAGIC   * Dataset source URL: https://www.kaggle.com/datasets/frtgnn/dunnhumby-the-complete-journey
# MAGIC   * Dataset source description: Kaggle competition
# MAGIC   * Dataset license: please see dataset source URL above

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download the dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/complete_journey
# MAGIC kaggle datasets download -d frtgnn/dunnhumby-the-complete-journey  --force -p /tmp/complete_journey

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unzip jigsaw dataset
# MAGIC Breakdown of the data downloaded: https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge/overview

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/complete_journey
# MAGIC unzip -o dunnhumby-the-complete-journey.zip
# MAGIC mv campaign_table.csv campaigns_households.csv
# MAGIC mv campaign_desc.csv campaigns.csv
# MAGIC mv coupon.csv coupons.csv
# MAGIC mv coupon_redempt.csv coupon_redemptions.csv
# MAGIC mv product.csv products.csv
# MAGIC mv transaction_data.csv transactions.csv
# MAGIC mv hh_demographic.csv households.csv
# MAGIC ls .

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move Data to storage
# MAGIC 
# MAGIC Move the dataset from the driver node to object storage so that it can be be ingested into Delta Lake for our demo.

# COMMAND ----------

for file in ['campaigns_households', 'campaigns', 'causal_data', 'coupons', 'coupon_redemptions', 'households', 'products', 'transactions']:
  src = f"file:/tmp/complete_journey/{file}.csv"
  dst = f"{cloud_storage_path}/incoming/{file}"
  print(f"Moving: {src} to {dst}")
  dbutils.fs.mkdirs(dst)
  dbutils.fs.mv(src, dst)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api|
