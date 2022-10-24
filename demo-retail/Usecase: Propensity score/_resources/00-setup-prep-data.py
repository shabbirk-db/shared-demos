# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail $min_dbr_version=11.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downloading the data

# COMMAND ----------

download = False
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
path = "/mnt/field-demos/retail/propensity"
if test_not_empty_folder(path):
  print(f"Using default path {path} as raw data location")
  #raw data is set to the current user location where we just uploaded all the data
  raw_data_location = "/mnt/field-demos/retail/propensity"
else:
  path = cloud_storage_path + "/propensity"
  if not reset_all_data and test_not_empty_folder(path):    
    print(f"Data already available, re-using it.")
  else:
    print(f"Couldn't find data saved in the default mounted bucket. Will download it from Kaggle instead under {cloud_storage_path}.")
    print("Note: you need to specify your Kaggle Key under ./_resources/_kaggle_credential ...")
    result = dbutils.notebook.run("./_resources/01_download", 300, {"cloud_storage_path": cloud_storage_path + "/propensity"})
    #result = dbutils.notebook.run("./01_download", 300, {"cloud_storage_path": cloud_storage_path + "/propensity"})
    if result is not None and "ERROR" in result:
      print("-------------------------------------------------------------")
      print("---------------- !! ERROR DOWNLOADING DATASET !!-------------")
      print("-------------------------------------------------------------")
      print(result)
      print("-------------------------------------------------------------")
      raise RuntimeError(result)
    else:
      print(f"Success. Dataset downloaded from kaggle and saved under {cloud_storage_path}.")
  raw_data_location = cloud_storage_path + "/propensity"

# COMMAND ----------

import pyspark.sql.functions as f
def clean_dates(df):
  for c in df.columns:
    if c.lower().endswith('day'):
      # if column is an integer day column, convert it to a date value
      df = df.withColumn(c, f.expr(f"date_add('2018-01-01', cast({c} as int)-1)"))
  return df

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Define Feature Generation Logic
# MAGIC 
# MAGIC Our first step is to define a function to generate features from a dataframe of transactional data passed to it.In our function, we are deriving a generic set of features from the last 30, 60 and 90 day periods of the transactional data as well as from a 30-day period (aligned with the labels we wish to predict) from 1-year back. 
# MAGIC 
# MAGIC Feature metrics include but not limited to:
# MAGIC * transaction summaries
# MAGIC * discounts activities 
# MAGIC * days from last activity
# MAGIC * ratios
# MAGIC * ...

# COMMAND ----------

# DBTITLE 1,Define Function to Derive Features
def get_features(df, grouping_fields, window=None):
  '''
  This function derives a number of features from our transactional data.
  These data are grouped by either just the household_key or the household_key
  and commodity_desc field and are filtered based on a window prescribed
  with the function call.
  
  df: the dataframe containing household transaction history
  
  grouping_fields: the field we'll be grouping on
  
  window: one of four supported string values:
    '30d': derive metrics from the last 30 days of the dataset
    '60d': derive metrics from the last 60 days of the dataset
    '90d': derive metrics from the last 90 days of the dataset
    '365d': derive metrics from the 30 day period starting 1-year
           prior to the end of the dataset. this alings with the
           period from which our labels are derived.
  '''
  
  grouping_suffix = ''
  if 'commodity_desc' in grouping_fields:
    grouping_suffix = '_cmd'
    
  # get list of distinct grouping items in the original dataframe
  anchor_df = transactions.select(grouping_fields).distinct()
  
  # identify when dataset starts and ends
  min_day, max_day = df.groupBy().agg(F.min('day').alias('min_day'), F.max('day').alias('max_day')).collect()[0]    
  
  ## print info to support validation
  print(f"group on {grouping_fields}, window={window}")
  #print('{0}:\t{1} days in original set between {2} and {3}'.format(window, (max_day - min_day).days + 1, min_day, max_day))
  
  # adjust min and max days based on specified window   
  window_suffix = '_'+window
  window_size = int(window[:-1])
  min_day = max_day - timedelta(days=window_size-1)
  if window_size == 365 :
    max_day = min_day + timedelta(days=30-1)
  # determine the number of days in the window
  days_in_window = (max_day - min_day).days + 1
  
  ## print to help with date math validation
  #print('{0}:\t{1} days in adjusted set between {2} and {3}'.format(window, days_in_window, min_day, max_day))
  
  # convert dates to strings to make remaining steps easier
  max_day = max_day.strftime('%Y-%m-%d')
  min_day = min_day.strftime('%Y-%m-%d')
  
  # derive summary features from set
  summary_df = (
    df
      .filter(f.expr(f"day between '{min_day}' and '{max_day}'")) # constrain to window
      .groupBy(grouping_fields)
        .agg(
          
          # summary metrics
          f.countDistinct('day').alias('days'), 
          f.countDistinct('basket_id').alias('baskets'),
          f.count('product_id').alias('products'), 
          f.count('*').alias('line_items'),
          f.sum('amount_list').alias('amount_list'),
          f.sum('instore_discount').alias('instore_discount'),
          f.sum('campaign_coupon_discount').alias('campaign_coupon_discount'),
          f.sum('manuf_coupon_discount').alias('manuf_coupon_discount'),
          f.sum('total_coupon_discount').alias('total_coupon_discount'),
          f.sum('amount_paid').alias('amount_paid'),
          
          # unique days with activity
          f.countDistinct(f.expr('case when instore_discount >0 then day else null end')).alias('days_with_instore_discount'),
          f.countDistinct(f.expr('case when campaign_coupon_discount >0 then day else null end')).alias('days_with_campaign_coupon_discount'),
          f.countDistinct(f.expr('case when manuf_coupon_discount >0 then day else null end')).alias('days_with_manuf_coupon_discount'),
          f.countDistinct(f.expr('case when total_coupon_discount >0 then day else null end')).alias('days_with_total_coupon_discount'),
          
          # unique baskets with activity
          f.countDistinct(f.expr('case when instore_discount >0 then basket_id else null end')).alias('baskets_with_instore_discount'),
          f.countDistinct(f.expr('case when campaign_coupon_discount >0 then basket_id else null end')).alias('baskets_with_campaign_coupon_discount'),
          f.countDistinct(f.expr('case when manuf_coupon_discount >0 then basket_id else null end')).alias('baskets_with_manuf_coupon_discount'),
          f.countDistinct(f.expr('case when total_coupon_discount >0 then basket_id else null end')).alias('baskets_with_total_coupon_discount'),          
    
          # unique products with activity
          f.countDistinct(f.expr('case when instore_discount >0 then product_id else null end')).alias('products_with_instore_discount'),
          f.countDistinct(f.expr('case when campaign_coupon_discount >0 then product_id else null end')).alias('products_with_campaign_coupon_discount'),
          f.countDistinct(f.expr('case when manuf_coupon_discount >0 then product_id else null end')).alias('products_with_manuf_coupon_discount'),
          f.countDistinct(f.expr('case when total_coupon_discount >0 then product_id else null end')).alias('products_with_total_coupon_discount'),          
    
          # unique line items with activity
          f.sum(f.expr('case when instore_discount >0 then 1 else null end')).alias('line_items_with_instore_discount'),
          f.sum(f.expr('case when campaign_coupon_discount >0 then 1 else null end')).alias('line_items_with_campaign_coupon_discount'),
          f.sum(f.expr('case when manuf_coupon_discount >0 then 1 else null end')).alias('line_items_with_manuf_coupon_discount'),
          f.sum(f.expr('case when total_coupon_discount >0 then 1 else null end')).alias('line_items_with_total_coupon_discount')          
          ))
  
  #compute ratios
  cols_per_days_ratio = ['days', 'baskets', 'products','line_items','amount_list','instore_discount','campaign_coupon_discount',
                         'manuf_coupon_discount', 'total_coupon_discount','amount_paid','days_with_instore_discount',
                         'days_with_campaign_coupon_discount','days_with_manuf_coupon_discount','days_with_total_coupon_discount']
  for col in cols_per_days_ratio:
    summary_df = (summary_df.withColumn(f'{col}_per_day', f.expr(f'{col}/days')) #per-day ratios
                            .withColumn(f'{col}_per_days_in_set', f.expr(f'{col}/{days_in_window}')) #per-day-in-set ratios
                            .withColumn(f'{col}_per_basket', f.expr(f'{col}/baskets')) # per-basket ratios
                            .withColumn(f'{col}_per_product', f.expr(f'{col}/products')) # per-product ratios
                            .withColumn(f'{col}_per_line_item', f.expr(f'{col}/line_items'))) # per-line_item ratios
    
  # amount_list ratios
  summary_df = (summary_df.withColumn('campaign_coupon_discount_to_amount_list', f.expr('campaign_coupon_discount/amount_list'))
                          .withColumn('manuf_coupon_discount_to_amount_list', f.expr('manuf_coupon_discount/amount_list'))
                          .withColumn('total_coupon_discount_to_amount_list', f.expr('total_coupon_discount/amount_list'))
                          .withColumn('amount_paid_to_amount_list', f.expr('amount_paid/amount_list')))
 

  # derive days-since metrics
  dayssince_df = (
    df
      .filter(f.expr(f"day between '{min_day}' and '{max_day}'"))
      .groupBy(grouping_fields)
        .agg(
          f.min(f.expr(f"'{max_day}' - case when instore_discount >0 then day else '{min_day}' end")).alias('days_since_instore_discount'),
          f.min(f.expr(f"'{max_day}' - case when campaign_coupon_discount >0 then day else '{min_day}' end")).alias('days_since_campaign_coupon_discount'),
          f.min(f.expr(f"'{max_day}' - case when manuf_coupon_discount >0 then day else '{min_day}' end")).alias('days_since_manuf_coupon_discount'),
          f.min(f.expr(f"'{max_day}' - case when total_coupon_discount >0 then day else '{min_day}' end")).alias('days_since_total_coupon_discount')
          )
      )
  
  # combine metrics with anchor set to form return set 
  ret_df = (anchor_df.join(summary_df, on=grouping_fields, how='leftouter')
                     .join(dayssince_df, on=grouping_fields, how='leftouter'))
  
  # rename fields based on control parameters
  for c in ret_df.columns:
    if c not in grouping_fields: # don't rename grouping fields
      ret_df = ret_df.withColumn(c, f.col(c).cast(DoubleType())) # cast all metrics as doubles to avoid confusion as categoricals
      ret_df = ret_df.withColumnRenamed(c,f'{c}{grouping_suffix}{window_suffix}')

  return ret_df


# COMMAND ----------

# DBTITLE 1,Featureset generation loop
from databricks import feature_store
from databricks.feature_store import FeatureStoreClient
from datetime import timedelta
import pyspark.sql.functions as F 

#compute the features for ['household_key'] and individual ['household_key', 'commodity_desc'] and for multiple period of time (30 previous days, 60 previous days, 90, 1year etc)
def generate_featureset(transactions_df, household_commodity_df):
  
  # IDENTIFY LAST DAY IN INCOMING DATAFRAME
  # --------------------------------------------
  last_day = transactions_df.groupBy().agg(f.max('day').alias('last_day')).collect()[0]['last_day']
  print(f"Last day in transaction set is {last_day.strftime('%Y-%m-%d')}")
  
  def add_features(grouping_fields):
    # get master set of grouping field combinations
    features = household_commodity_df.select(grouping_fields).distinct()

    # get features and combine with other feature sets to form full feature set
    for window in ['30d', '60d']: # ['30d', '60d', '90d', '365d']:
      new_features = get_features(transactions_df, grouping_fields, window=window)
      features = features.join(new_features, on=grouping_fields, how='leftouter')
    features.fillna(value=0.0, subset=[c for c in features.columns if c not in grouping_fields])
    return features
  
  household_features = add_features(['household_key'])
  household_commodity_features = add_features(['household_key', 'commodity_desc'])

  # COMBINE HOUSEHOLD & HOUSEHOLD-COMMODITY FEATURES
  # --------------------------------------------
  feature_set = (household_features.join(household_commodity_features, on='household_key')
                                   .withColumn('day', f.lit(last_day))) # add last day to each record to identify day associated with features
  # --------------------------------------------
  return feature_set
  

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Define labels

# COMMAND ----------

from datetime import timedelta
import pyspark.sql.functions as F
def get_items_next_30_days(transactions, household_commodity_df):
  #Let's reduce our dataframe with a sum of all transaction per household and commodity per week
  df = transactions.withColumn("day", f.expr("date_add(day, -dayofweek(day))")) \
                   .groupBy("household_key", "commodity_desc", "day").agg(f.sum('units').alias('units'))

  #Build a DF with all the days for all the household and commodity. That's our key for our labels (init with zeros)
  first_day, last_day = df.groupBy().agg(f.min('day'), f.max('day')).collect()[0]
  days = [(first_day+timedelta(days=d),) for d in range(0, (last_day - first_day).days, 7)]
  days_df = sqlContext.createDataFrame(days, schema="day date")

  days_df.crossJoin(household_commodity_df) \
         .join(df, ["household_key", "commodity_desc", "day"], how="left") \
         .withColumn("units", f.coalesce(F.col("units"), f.lit(0))).createOrReplaceTempView("prop_unit_per_week")
  # Finally we apply a window to count the number of transaction over the next 30 days.
  # We'll use that as label and try to predict this value for our propensity score (0 and 1 if > 0 meaning buy at least 1 item)
  buy_next_month = spark.sql("""select household_key, commodity_desc, day, sum(units) OVER(
                                    PARTITION BY household_key, commodity_desc
                                    ORDER BY CAST(day AS timestamp) --day of type date
                                    RANGE BETWEEN INTERVAL 1 DAYS FOLLOWING AND INTERVAL 30 DAYS FOLLOWING) as units_next_30
                                  from prop_unit_per_week""")
  return buy_next_month.withColumn("will_by_in_30_days", (f.col("units_next_30")>0).cast("integer")).drop("units_next_30")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ML Helpers

# COMMAND ----------

# DBTITLE 1,Wrapper to returns the probability instead of the class for our propensity score
# define custom wrapper
class BuyProbaWrapper(mlflow.pyfunc.PythonModel):
  
  def __init__(self, model):
    self.model = model
    
  def predict(self, context, model_input):
    return self.model.predict_proba(model_input)[:,1]
  

# COMMAND ----------

# DBTITLE 1,AutoML helpers
def display_automl_propensity_link(df): 
  display_automl_link("propensity_auto_ml", "field_demos_propensity", df, "will_by_in_30_days", 5)

def get_automl_propensity_run(df): 
  return get_automl_run_or_start("propensity_auto_ml", "field_demos_propensity", df, "will_by_in_30_days", 5)
