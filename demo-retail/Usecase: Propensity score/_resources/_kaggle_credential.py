# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Downloading the data from Kaggle
# MAGIC 
# MAGIC ## Define KAGGLE_USERNAME and KAGGLE_KEY for authentication
# MAGIC 
# MAGIC * Instructions on how to obtain this information can be found [here](https://www.kaggle.com/docs/api).
# MAGIC 
# MAGIC * This information will need to be entered below. Please use [secret](https://docs.databricks.com/security/secrets/index.html) to avoid having your key as plain text
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_kaggle&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to define Kaggle credential and load the dataset.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

import os
os.environ['KAGGLE_USERNAME'] = "" #dbutils.secrets.get(scope="kaggle", key="kaggle_username")
os.environ['KAGGLE_KEY'] = "" #dbutils.secrets.get(scope="kaggle", key="kaggle_key")

#Alternative option is to write your kaggle file in your driver using the cluster terminal option and load it:
#config = json.load(open(f"./config/{env}.conf", "r"))
#os.environ['KAGGLE_USERNAME'] = config["kaggle_username"]
#os.environ['KAGGLE_KEY'] = config["kaggle_key"]
