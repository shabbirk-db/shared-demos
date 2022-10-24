# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %pip install openpyxl==3.0.9
# MAGIC %pip install lifetimes==0.11.3

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

import pandas as pd
import numpy as np

from lifetimes.fitters.gamma_gamma_fitter import GammaGammaFitter
from lifetimes.fitters.pareto_nbd_fitter import ParetoNBDFitter

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from scipy.stats import pearsonr

import pyspark.sql.functions as f
from pyspark.sql.types import *

import mlflow

import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import plotly.graph_objects as go
import lifetimes


# COMMAND ----------

# DBTITLE 1,Wrapper for CLV Model to be saved in MLFlow
from mlflow.models.signature import infer_signature

# Because CLV models are specific, they're not supported out of the box by MLFlow. 
# We need to create a mode wrapper extending the default PythonModel class.
class ClvModelWrapper(mlflow.pyfunc.PythonModel):
  
    def __init__(self, spend_model, retention_model):
      self.spend_model = spend_model
      self.retention_model = retention_model
      
    def predict(self, context, dataframe):
      # access input series
      recency = dataframe.iloc[:,0]
      frequency = dataframe.iloc[:,1]
      monetary_value = dataframe.iloc[:,2]
      T = dataframe.iloc[:,3]
      months = 36 # hard-code a 3-year value
      discount_rate = 0.01 # monthly discount rate
      
      # make CLV prediction
      clv = self.spend_model.customer_lifetime_value(
            self.retention_model, #the model to use to predict the number of future transactions
            frequency,
            recency,
            T,
            monetary_value,
            time=months,
            discount_rate=discount_rate)
      return clv
    
# add lifetimes to conda environment info for the model
lifetime_conda_env = mlflow.pyfunc.get_default_conda_env()
lifetime_conda_env['dependencies'][-1]['pip'] += ['lifetimes=='+lifetimes.__version__]

