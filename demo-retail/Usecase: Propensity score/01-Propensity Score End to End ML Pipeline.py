# Databricks notebook source
# MAGIC %md
# MAGIC # Propensity Score for marketing - End to End ML Pipeline Demo
# MAGIC 
# MAGIC 
# MAGIC ## What is Propensity Score
# MAGIC The propensity scoring process is a method to estimate customers' potential receptiveness to an offer or to content related to a subset of products.
# MAGIC 
# MAGIC Consumers are expecting personalized interactions as part of their shopping experience today. One of the first steps in the personalization journey is propensity scoring. By examining the general patterns of sales data and calculating numerical attributes (features), we can train a model to estimate the probability the behavior of interest will occur. 
# MAGIC 
# MAGIC ## Why computing the Propensity Score?
# MAGIC 
# MAGIC Overall, knowing your customers propensity score will increase your profitability. Here are typical outcomes driven by propensity score:
# MAGIC 
# MAGIC * **Next best offer & personalisation**: determine which offers, advertisements, etc, to put in front of a given customer 
# MAGIC * **Targeting marketing campaigns**: identify subsets of customers to target for various promotional engagements and boost your revenue
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fpropensity%2Fpropensity_intro&dt=RETAIL_PROPENSITY">
# MAGIC <!-- [metadata={"description":"Propensity demo, forecast propability to build a product",
# MAGIC  "authors":["tian.tian@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propensity Score prediction with Databricks Lakehouse
# MAGIC 
# MAGIC The Lakehouse let you build a platform to onboard such use-cases. From data ingestion, traditional BI to ML within 1 single tool.
# MAGIC 
# MAGIC In this demo, we'll calculate the Propensity Score for each household as the probability to buy a specific product family over the following 30 days. 
# MAGIC 
# MAGIC A high propensity score means that this user is likely to buy this product soon. We can use this information to optimize our targeting campains and increase revenue.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-lakehouse.png" width="1000" />
# MAGIC 
# MAGIC 
# MAGIC ## Demo Flow
# MAGIC 
# MAGIC To support our propensity scoring efforts, we will make use of the [*The Complete Journey*](https://www.kaggle.com/frtgnn/dunnhumby-the-complete-journey) dataset, published to Kaggle by Dunnhumby. 
# MAGIC 
# MAGIC The dataset consists of numerous files identifying household purchasing activity in combination with various promotional campaigns for about 2,500 households over a nearly 2 year period.
# MAGIC 
# MAGIC We'll implement the following flow:
# MAGIC 
# MAGIC 1. Ingest data and build our tables to support SQL/DW workloads
# MAGIC 2. Leverage the Feature Store to create our feature tables and manage 1000s of features that are used to train our model
# MAGIC 3. Build a model with AutoML to predict propensity score
# MAGIC 4. Lastly, we'll generate the scoring for each customer. 
# MAGIC 
# MAGIC The workflow is illustrated as following:
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-flow-1.png' width='1000'>

# COMMAND ----------

# DBTITLE 1,Reset and Retrieve Configuration Values
# MAGIC %run ./_resources/00-setup-prep-data $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ##Step 1: Ingest Data
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-flow-2.png' width='600' style="float: right">
# MAGIC 
# MAGIC Our first step is to ingest the incoming file and build our Delta tables, ready to be queried using traditional BI tools.
# MAGIC 
# MAGIC We'll leverage Databricks Autoloader (`CloudFiles`) to simplify that and loop over all the incoming folders in python to create our tables.
# MAGIC 
# MAGIC Autoloader accelerates data engineers with:
# MAGIC 
# MAGIC * Schema inference & schema evolution
# MAGIC * Simplicity: incrementally load new files, in streaming or batch
# MAGIC * Scalability: easily handle millions of incoming files
# MAGIC 
# MAGIC 
# MAGIC *Note: It's important to note the dates used in this data set are not proper dates.  Instead, they are integer values ranging from 1 to 711. We added a function to generate dates based on the value in the companion notebook*

# COMMAND ----------

#Incrementally load the given path and save the new data to the given table 
def ingest_bronze(raw_files_path, table_name):
  print(f'Incrementally loading folder {raw_files_path} as table {table_name}...')
  df = (spark.readStream 
            .format("cloudFiles") 
            .option("cloudFiles.format", "csv") 
            .option("cloudFiles.schemaLocation", f"{raw_data_location}/bronze/schema/{table_name}")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(raw_files_path))
  #create real dates
  df = clean_dates(df)
  return (df.writeStream 
      .queryName(table_name)
      .option("checkpointLocation", f"{raw_data_location}/bronze/checkpoint/{table_name}") 
      .trigger(availableNow=True).table(table_name))

# COMMAND ----------

# DBTITLE 1,Loop over the incoming folders (1 per table we have to load)
for folder in dbutils.fs.ls(raw_data_location+"/incoming"):
  ingest_bronze(folder.path, f'prop_{folder.name[:-1]}')
wait_for_all_stream("prop_")

# COMMAND ----------

# DBTITLE 1,Data is ready to be requested in SQL
# MAGIC %sql select * from prop_campaigns

# COMMAND ----------

# MAGIC %md ### Compute Transactional Data
# MAGIC 
# MAGIC The transactional data will be the focal point of our analysis. It contains information about what was purchased by each household and when along with various discounts applied at the time of purchase. Some of this information is presented in a manner that is not easily consumable.
# MAGIC 
# MAGIC As such, we will implement some simple logic to sum discounts and combine these with amounts paid to recreate list pricing and make other simply adjustments that make the transactional data a bit easier to consume:  

# COMMAND ----------

# DBTITLE 1,Create Adjusted Transactions Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS prop_transactions_adj AS SELECT
# MAGIC     household_key, basket_id, week_no, day, trans_time, store_id, product_id, amount_list, campaign_coupon_discount, 
# MAGIC     manuf_coupon_discount, manuf_coupon_match_discount, total_coupon_discount, instore_discount, amount_paid, units
# MAGIC   FROM (
# MAGIC     SELECT household_key, basket_id, week_no, day, trans_time, store_id, product_id,
# MAGIC       CASE WHEN COALESCE(coupon_match_disc,0.0) = 0.0 THEN -1 * COALESCE(coupon_disc,0.0) ELSE 0.0 END as campaign_coupon_discount,
# MAGIC       CASE WHEN COALESCE(coupon_match_disc,0.0) != 0.0 THEN -1 * COALESCE(coupon_disc,0.0) ELSE 0.0 END as manuf_coupon_discount,
# MAGIC       -1 * COALESCE(coupon_match_disc,0.0) as manuf_coupon_match_discount,
# MAGIC       -1 * COALESCE(coupon_disc - coupon_match_disc,0.0) as total_coupon_discount,
# MAGIC       COALESCE(-1 * retail_disc,0.0) as instore_discount,
# MAGIC       COALESCE(sales_value,0.0) as amount_paid,
# MAGIC       COALESCE(sales_value - retail_disc - coupon_disc - coupon_match_disc,0.0) as amount_list,
# MAGIC       quantity as units
# MAGIC     FROM prop_transactions);
# MAGIC SELECT * FROM prop_transactions_adj;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Prepare the training set
# MAGIC 
# MAGIC We're now ready to prepare the training set we'll need to build our model.
# MAGIC 
# MAGIC This model will use the past event from each household (number of transactions, frequency etc) to try to predict if they're likely to buy a given commodity product over the next 30 days.
# MAGIC 
# MAGIC We'll compute these features and labels for every week of the dataset, in a moving-window fashion:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-feature.png" width="1000" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 2.1: Prepare the labels: will this household buy this commondity over the next 30 days?
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-flow-3.png' width='600' style="float: right">
# MAGIC 
# MAGIC 
# MAGIC Our goal is to compute for each week (each Monday) how many iteam of each commodity are bough over the following 30 days. 
# MAGIC 
# MAGIC We'll use then use this information as label: 
# MAGIC 
# MAGIC * 1 if the household bought at least one item over the next 30 days, 
# MAGIC * 0 otherwise (household didn't buy this commodity)
# MAGIC 
# MAGIC Because the label computation is a bit expensive (we have a big dataset), we'll save the result in a table.
# MAGIC 
# MAGIC 
# MAGIC *Note: This computation is a bit advanced with a SQL window function, grouping per first day of the current week (Monday). You can open the details in the companion notebook.*

# COMMAND ----------

# join to products to get commodity assignment
products = spark.table('prop_products')
transactions = spark.table('prop_transactions_adj').join(products, on='product_id', how='inner')

# get unique commodities
commodities = products.select('commodity_desc').distinct()

# get unique households
households = transactions.select('household_key').distinct()

# cross join all commodities and households
household_commodity_df = households.crossJoin(commodities)

# COMMAND ----------

# DBTITLE 1,Build labels and save them as a table (can take a couple of minutes to run...)
#For more details on the label creation with window function open the companion notebook
buy_next_month = get_items_next_30_days(transactions, household_commodity_df)
buy_next_month.write.mode("overwrite").saveAsTable("prop_buy_next_month")

# COMMAND ----------

# MAGIC %sql select * from prop_buy_next_month

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ###Step 2.2: Generate Features with Feature Store 
# MAGIC 
# MAGIC #### Feature Store
# MAGIC 
# MAGIC The [Databricks Feature Store](https://docs.databricks.com/applications/machine-learning/feature-store/index.html) is a centralized repository that enables the persistence, discovery and sharing of features across various model training exercises. With feature store, a data scientist can:
# MAGIC * Search for feature tables by feature table name, feature, or data source 
# MAGIC * Connect your data to your model deployment, including but not limited to the following:
# MAGIC   * Identify the data sources used to create a feature table
# MAGIC   * Identify all of the consumers of a particular features, including Models, Endpoints, Notebooks and Jobs
# MAGIC * Control access to feature table’s metadata
# MAGIC 
# MAGIC #### Crafting our features
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-flow-4.png' width='600' style="float: right">
# MAGIC 
# MAGIC We typically requires to train our propensity model using hundreds of features. They are sums, ratio, frequency based on transactional information over multiple period of past time (ex: 30,60,80,365 days).
# MAGIC 
# MAGIC A couple of example of these features would be:
# MAGIC 
# MAGIC * Transaction summaries
# MAGIC * Discounts activities 
# MAGIC * Days from last activity
# MAGIC * Ratios...
# MAGIC 
# MAGIC Like the labels, we'll recompute all these features for every week for every household & commodity.
# MAGIC 
# MAGIC Computing these feature can be a fairly advanced task. It's also compute-intensive. For more detail you can see the companion notebook

# COMMAND ----------

# DBTITLE 1,Generate Features for each week and households
# instantiate feature store client
fs = FeatureStoreClient()

# move last day 30 back from true last day (as we compute the labels on 30 days). 
#date_add(day, -dayofweek(day)) will remove the dayofweek to the current day to get the monday
last_day = spark.read.table("prop_buy_next_month").withColumn("day", f.expr("date_add(day, -dayofweek(day))")).groupBy().agg(f.max('day').alias('last_day')).collect()[0]['last_day']
last_day = last_day - timedelta(days=7*4)
weeks = []
# We'll only compute the features for 2 weeks to speedup the demos, but we could typically take > 1 full year
for d in range(0, 2): #range(0, 54):
  today = last_day - timedelta(days=d*7)
  today = today.strftime('%Y-%m-%d')
  weeks.append(today)
  print(f"Calculating features for {today}")
  
  # generate the features 
  feature_set = generate_featureset(transactions.filter(f.expr(f"day <= '{today}'")), household_commodity_df)

  #We create the feature store table if it doesn't exist 
  #Note: if you have an error here you might have to delete the feature store from the FS UI
  if not spark._jsparkSession.catalog().tableExists(dbName, 'propensity_features'):
    fs.create_table(
          name=f'{dbName}.propensity_features', # name of feature store table
          primary_keys=['household_key', 'commodity_desc', 'day'], # name of keys that will be used to locate records
          schema=feature_set.schema, # schema of feature set as derived from our feature_set dataframe
          description='features used for product propensity scoring')

  # merge feature set data into feature store based on the primary keys
  fs.write_table(name=f'{dbName}.propensity_features', df=feature_set, mode='merge')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-feature-store.png" style="float: right" width="500px" />
# MAGIC 
# MAGIC That's it, our features have been written to a feature store table named *propensity_features*.  You can open the feature store menu to review it.
# MAGIC 
# MAGIC While we can access this feature table within the feature store UI, we can also request the underlying table as a standard SQL table to verify its contents as follows:

# COMMAND ----------

# DBTITLE 1,Review Features
# MAGIC %sql select * from propensity_features

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Step 3: Use the best Auto-ML generated notebook to bootstrap our ML Project
# MAGIC 
# MAGIC [AutoML](https://docs.databricks.com/applications/machine-learning/automl.html) is a fully-automated model development solution seeking to “democratize” and accelerate machine learning project. 
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/propensity/propensity-flow-5.png' width='600' style="float: right">
# MAGIC 
# MAGIC 
# MAGIC It allows you to quickly generate models leveraging industry best practices. As a glass box solution, AutoML first generates a collection of notebooks representing different model variations aligned with your scenario. 
# MAGIC 
# MAGIC In this step, we'll start an AutoML run and just pick the best run from the Auto ML experiment and acclerate ML deployment process by incorporating feature store.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### 3.1: Retrieve Features and create training set
# MAGIC 
# MAGIC To retrieve our features from the Feature Store, we need to use the Primary Key defined during the table creation. You can think about it as a join we'll perform to fetch the feature of a specific row.
# MAGIC 
# MAGIC In our case we'll use: *household_key*, *commodity_desc* and *day*.
# MAGIC 
# MAGIC We'll create a training dataset for the `SOFT DRINKS` commodity. All we have to do is get the list of keys and the label we previously computed and define a Feature Lookup on the `propensity_features` Feature Store.

# COMMAND ----------

# DBTITLE 1,Identify Keys with which to Lookup Features
# primary key matching columns
lookup_keys = ['household_key','commodity_desc', 'day']

# COMMAND ----------

# DBTITLE 1,Create Training Set
# instantiate feature store client
from databricks import feature_store
from databricks.feature_store import FeatureStoreClient, FeatureLookup

# instantiate feature store client
fs = FeatureStoreClient()

# define features to lookup
feature_lookups = [
  FeatureLookup(
    table_name = f'{dbName}.propensity_features', # get data from this feature store table
    lookup_key = lookup_keys # features looked up based on these keys (any other fields are treated as features)
    )
  ]

#We'll limit the training df to SOFT drinks and only for the mondays we have features (to simplify the demo)
training_df = spark.read.table("prop_buy_next_month").where(f"""commodity_desc='SOFT DRINKS'""").filter(F.col('day').isin(weeks))

# combine features with labels to form a training set
training_set = fs.create_training_set(
  df = training_df,
  feature_lookups = feature_lookups,
  label = 'will_by_in_30_days',
  exclude_columns = lookup_keys # dont train on feature lookup keys
  )

# COMMAND ----------

# MAGIC %md ### 3.2: Perform AutoML Model Training
# MAGIC 
# MAGIC With our training set ready, we can now turn AutoML loose on the data to train a classification model.  
# MAGIC 
# MAGIC AutoML supports the use of various algorithms (sklearn, lightgbm, xgboost...). It leverages hyperopt to distribute the hyperparameter tuning training runs used to optimize each of these and captures model training results to an mlflow experiment. 
# MAGIC 
# MAGIC While you can use the UI directly, the [API](https://docs.databricks.com/applications/machine-learning/automl.html#classification) allow us to programatically trigger a different Auto ML run for every single commodity.
# MAGIC 
# MAGIC *Note: we'll call the `display_automl_propensity_link` function to run it once in the background for DRINKS and then simply display the AutoML result to avoid multiple long model-training. See the companion notebooks for more details.*

# COMMAND ----------

#Use the automl api directly:
#automl_run = databricks.automl.classify(dataset = training_set.load_df(), target_col = "will_by_in_30_days", ...)
display_automl_propensity_link(training_set.load_df())
#Get back the best run from API
best_autml_run = get_automl_propensity_run(training_set.load_df())["best_trial_run_id"]

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3.3: Register the Best Model
# MAGIC 
# MAGIC Once we have the AutoML result, we get the best model run and its URI. 
# MAGIC 
# MAGIC Based on that, we can register the model in MLFlow model registry along with information about the feature store assets from which it consumes features. This will make data scoring in our next step much easier.
# MAGIC 
# MAGIC Note that we use the Feature Store client `fs` to log the model.

# COMMAND ----------

# DBTITLE 1,Get the best model from the run
model_uri = mlflow.get_run(best_autml_run).info.artifact_uri+"/model"
model = mlflow.pyfunc.load_model(model_uri)
# wrap original model in custom wrapper because we waht the proba (not the class). Open the companion notebook for more details
wrapped_model = BuyProbaWrapper(model._model_impl)  # _model_impl provides access to the underlying model and its full API

# COMMAND ----------

# DBTITLE 1,Register Model with Link to Feature Store and flag it as Production ready
#log the model in MLFlow registry, keeping a link with our Feature Store (defined in the training_set object)
fs.log_model(
  model=wrapped_model,
  artifact_path='model', 
  flavor=mlflow.pyfunc, 
  training_set=training_set, # log with information on how to lookup features in feature store
  registered_model_name="field_demos_propensity")

#Let's move the model as production ready stage:
client = mlflow.tracking.MlflowClient()
last_version = client.get_registered_model("field_demos_propensity").latest_versions[0].version
print(f"registering model version {last_version} as production model")
client.transition_model_version_stage(name = "field_demos_propensity", version = last_version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Computing the propensity score
# MAGIC 
# MAGIC With our features generated and our model trained, we can now turn our attention to the scoring of individual households for their propensity to buy from a commodity category.  
# MAGIC 
# MAGIC Because we stored our model with metadata on how to lookup features in the feature store, batch scoring is pretty simple.  All we need to do is identify our model and the dataframe against the features we wish to score and let Databricks do the work (the lookup keys defined in the training set).  

# COMMAND ----------

# DBTITLE 1,Assemble dataset to Score
scoring_df = (households
              .withColumn('commodity_desc', f.lit('SOFT DRINKS'))
              .withColumn('day', f.lit(last_day)))
display(scoring_df)

# COMMAND ----------

# DBTITLE 1,Score the Dataset
# connect to feature store
fs = FeatureStoreClient()

#Call the model and compute score
#TODO: why is it only working with None here ?
scores = fs.score_batch(model_uri = f'models:/field_demos_propensity/None', df = scoring_df)
   
#save the result as table for downstream usage
scores.withColumnRenamed('prediction', 'propensity_score').select('household_key', 'commodity_desc', 'day', 'propensity_score') \
      .write.mode("overwrite").saveAsTable("household_profiles")

# COMMAND ----------

# MAGIC %sql select * from household_profiles order by propensity_score desc 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Step 5: downstream usage
# MAGIC 
# MAGIC We now have a table with propensity score for all households and commodities. 
# MAGIC 
# MAGIC As simple as it looks, this process with feature store, automl, and mlflow working together, data scientists can easily build a robost and reproducible end-to-end solutions now.
# MAGIC 
# MAGIC We can publish this information to downstream systems for use by marketing in the orchestration of various campaigns.
# MAGIC 
# MAGIC Typical use-cases are:
# MAGIC 
# MAGIC * Marketing campaigns personalization
# MAGIC * Next best offer 
# MAGIC * Recommender system
# MAGIC ... 

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note** This demo mainly focuses on how to streamline the propensity score process with feature store, autoML, and MLflow. If you require a more in-depth presentation that illustrates a full end-to-end process, which includes detailed data modeling and feature engineering steps, please consider using the notebook associated with [the solution accelerator]()
