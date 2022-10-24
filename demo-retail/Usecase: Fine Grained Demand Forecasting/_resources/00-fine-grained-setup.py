# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

import fbprophet
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics
from mlflow.models.signature import infer_signature
from hyperopt import fmin, hp, tpe, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from hyperopt import SparkTrials
from hyperopt import space_eval
from pyspark.sql.functions import lit
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
#for this demo disable AEQ as we have a small amount of data and want it to be parallelized
spark.conf.set("spark.sql.adaptive.enabled", "false")


# COMMAND ----------

#temp fix with file in repos, see https://databricks.slack.com/archives/CJFK6FVMG/p1633982875363300

# COMMAND ----------

# MAGIC %sh mkdir /tmp/temp_fix

# COMMAND ----------

# MAGIC %cd /tmp/temp_fix
