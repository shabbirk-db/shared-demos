# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md This demo is intended for business-aligned personas who are more interested in our ability to enable business outcomes than the exact technical details of how we do it.  For that reason, much of the code in this notebook has been hidden and the logic itself has been streamlined to get right to the points they are concerned with.  If you require a more technically-oriented presentation, please consider using the notebook associated with [the solution accelerator](https://databricks.com/blog/2021/04/06/fine-grained-time-series-forecasting-at-scale-with-facebook-prophet-and-apache-spark-updated-for-spark-3.html).
# MAGIC 
# MAGIC Finally, be sure to select *View: Results Only* before presenting this notebook to customers.

# COMMAND ----------

# MAGIC %md # Customer Lifetime Value with Databricks Lakehouse
# MAGIC 
# MAGIC ## What is CLV
# MAGIC Our goal with this demo is to calculate the Customer Lifetime Value (CLV) for each customer with which we engage. The CLV a measure of the customer's revenue generated over their entire relationship with a company.
# MAGIC 
# MAGIC In a scenario where we don't maintain contractual relationships, customers are free to come and go as they please. This makes it very difficult for us to estimate how much a customer will spend with us over the three to five years we typically assign to a customer lifetime.  
# MAGIC 
# MAGIC However, we can examine the general patterns of purchase frequencies and magnitude (monetary value) relative to the time a customer has been engaged with us from across the entire population of our customers to arrive at some predictions about how a particular customer's story will playout over the long-term. In other words, not every customer behaves in the same way, but the range of expected customer behaviors can be found when we look across the entire population of our customers.
# MAGIC 
# MAGIC ## Why computing the CLV?
# MAGIC 
# MAGIC Overall, knowing your customers lifetime value will increase your profitability. Here are typical outcome driven by CLV:
# MAGIC 
# MAGIC * **Optimize Customer Acquisition**: when you know what you earn from a typical customer, you can decide to invest more in the more profitable channel.
# MAGIC * **Personalisation & targeting**: you will be able to invest more on your most important customers and boost your revenue
# MAGIC * **Reduce customer attrition**: churn can be reduced and retention increased, driving more revenue
# MAGIC 
# MAGIC ## Implementing CLV forecast with Databricks Lakehouse
# MAGIC 
# MAGIC In this demo, we'll see how we can optimize revenue in 2 ways leveraging the Lakehouse:
# MAGIC 
# MAGIC 1. We'll use RFM metrics to segment our customer base to be able to take specific actions (the top line in the diagram)
# MAGIC 2. We'll then take it further and build a model to forecast CLV over the next 3 years (the bottom line in the diagram)
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow.png' width=1100>
# MAGIC 
# MAGIC The lakehouse will accelerate this use-case with one single platform:
# MAGIC 
# MAGIC * Simplifying data ingestion and RFM metrics computation at scale (with near-realtime capabilities)
# MAGIC * Building customers segmentations or CLV models for Marketing teams to take strategic actions and increase retention
# MAGIC * Track customers churn and revenue evolution using Databricks SQL Dashboards, measuring CLV estimation impact and ROI
# MAGIC * Bringing a unified governance layer, from data-access permission to audit logs
# MAGIC 
# MAGIC *NOTE: The exact details of how the CLV model works and why certain steps are performed are quite complex.  If you'd like to examine more of those details, please be sure to read the two part blog series on CLV found [here](https://databricks.com/blog/2020/06/03/customer-lifetime-value-part-1-estimating-customer-lifetimes.html) and [here](https://databricks.com/blog/2020/06/17/customer-lifetime-value-part-2-estimating-future-spend.html).  Be sure to review the notebooks associated with each to learn more about the details of the technical implementation demonstrated here.*
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fnotebook_CLV&dt=RETAIL_USE_CASE_CLV">
# MAGIC <!-- [metadata={"description":"Present how Databricks can grow customer revenue by forecasting Customer Lifetime Value and increase Marketing team efficiency",
# MAGIC  "authors":["bryan.smith@databricks.com"],
# MAGIC   "db_resources":{"Dashboards": ["Customer Satisfaction by Segment"]}}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup-clv $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md ## 1: Building the lakehouse: Data ingestion
# MAGIC 
# MAGIC The first step is to ingest the data and make it available in the lakehouse for all: Data engineers processing the data, Data scientist to build CLV model and Data Analyst for BI/reporting workload.
# MAGIC 
# MAGIC The data required to perform a customer lifetime value estimate is typically the core transactional data at the center of every retail business.  While these datasets often have many attributes, the only things we need in such a data set are:</p>
# MAGIC 
# MAGIC 1. the **transaction date**,
# MAGIC 2. a unique identifier for each **customer** that is consistent across transactions, and
# MAGIC 3. a measure of **monetary value** such as revenue or margin

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Ingesting the raw data
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-1.png' style="float:right" width="600">
# MAGIC 
# MAGIC Our data is being delivered in a blob storage as json files. The first thing to do is to ingest this data and load it in a Delta Table.
# MAGIC 
# MAGIC Incrementally ingesting data can be tricky at scale. To simplify schema evolution, inference and ensure good performances, we will be using Databricks Autoloader.
# MAGIC 
# MAGIC Dataloader (`cloudFiles`) will create a Delta table `order_bronze` ready to be requested at scale that we will use later to compte customer RFM metrics

# COMMAND ----------

# DBTITLE 1,Data is made available in our blob storage
# MAGIC %fs ls /mnt/field-demos/retail/clv/orders/

# COMMAND ----------

# DBTITLE 1,Use Autoloader to incrementally load the "order_bronze" table
(spark.readStream 
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schemas/orders")
        .load("/mnt/field-demos/retail/clv/orders/")
      .writeStream
        .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/orders_bronze")
        .trigger(once=True)
        .table("orders_bronze").awaitTermination())

orders = spark.table("orders_bronze")
display(orders)

# COMMAND ----------

# MAGIC %md ## 2: Customer segmentation using RFM metrics

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Compute Term, Recency, Frequency and Monetary value
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-2.png' style="float:right" width="600">
# MAGIC 
# MAGIC We need to derive four key metrics from our `order_bronze` table containing all the transactions:
# MAGIC 
# MAGIC * **Term** (Age) - the number of periods (typically days) from the end of the dataset until the start of the customer relationship
# MAGIC * **Recency** - the term (age) of the customer a the time of the last transaction
# MAGIC * **Frequency** - the number of repeated transactions within which the customer has engaged
# MAGIC * **Monetary Value** - an average spend or margin amount associated with each transaction
# MAGIC 
# MAGIC These metrics, typically referred to as the RFM-metrics, are used throughout various marketing exercises. 
# MAGIC 
# MAGIC We will use them both for customer segmentation and CLV forecast.
# MAGIC 
# MAGIC Computing RFM metrics for millions if not billions of individual line items are boiled down to one set of metrics for each customer requires computational horsepower. Databricks can easily handle these transformation.
# MAGIC 
# MAGIC *Note: For more details on how to compute these metrics, please refere to our Solution Accelerator*

# COMMAND ----------

# DBTITLE 1,Compute per-Customer RFM Metrics
# valid customer orders
#RFM metrics are calculated in a slightly less straightforward manner than we'd typically expect.  
#The exact details aren't terribly important here.  
orders_per_customer_date = (
    orders
      .where(orders.CustomerID.isNotNull())
      .withColumn('transaction_at', orders.InvoiceDate)
      .groupBy(orders.CustomerID, 'transaction_at')
      .agg(f.sum(orders.SalesAmount).alias('salesamount')))

# calculate last date in dataset 
last_transaction_date = orders.select(f.max(col("InvoiceDate")).alias("current_dt"))

# calculate first transaction date by customer
first_transaction_per_customer = (orders.groupBy(orders.CustomerID)
                                        .agg( f.min(col("InvoiceDate")).alias('first_at')))

# combine customer history with date info 
customer_history = (orders_per_customer_date
                      .crossJoin(last_transaction_date)
                      .join(first_transaction_per_customer,['CustomerID'], how='inner')
                      .select(
                        col('CustomerID').alias('customer_id'), 'first_at', 'transaction_at', 'salesamount', 'current_dt'))
      
# calculate relevant metrics by customer
rfm = (customer_history
         .groupBy(col('customer_id'), col('current_dt'), col('first_at'))
         .agg(
            (f.countDistinct(col('transaction_at'))-1).cast('float').alias('frequency'),
            f.datediff( f.max(col('transaction_at')), col('first_at')).cast('float').alias('recency'),
            f.datediff(col('current_dt'), col('first_at')).cast('float').alias('T'),
            f.when( f.countDistinct(col('transaction_at'))==1,0)                           # MONETARY VALUE
                .otherwise(
                f.sum(
                    f.when(col('first_at')==col('transaction_at'),0)
                        .otherwise(col('salesamount'))
                )/( f.countDistinct(col('transaction_at'))-1)
            ).alias('monetary_value')
        )
         .withColumn('monetary_value', f.expr('case when monetary_value < 0 then 0 else round(monetary_value, 2) end'))
         .select('customer_id','frequency','recency','T','monetary_value')
         .orderBy('customer_id')
         )
rfm.write.mode('overwrite').saveAsTable('rfm_silver')

# COMMAND ----------

# DBTITLE 1,Per customer metrics are now saved as "rfm_silver" table
# MAGIC %sql SELECT * FROM rfm_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Explore and analyse RFM Metrics
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-3.png' style="float:right" width="600">
# MAGIC 
# MAGIC This is where the Data Scientist work really begins.
# MAGIC 
# MAGIC The mathematics behind the CLV models are quite complex.  However, we might examine our metrics to develop an intuition about what these models are doing.  
# MAGIC 
# MAGIC First, our data should contain information from customers who've engaged with us over a broad range of time.  Newer customers are likely to have a certain set of patterns that evolve as they *age* within the customer population:

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ####Examine Recency Distribution
# MAGIC 
# MAGIC In general, we should expect customers to gravitate towards a certain frequency of engagement.  Some customers will engage less often and some customers will engage much more often. While not illustrated in this chart, there is expected to be a relationship between frequency and the *age* of the customer so that as a customer approaches the end of their relationship with our organization, the frequency tapers off in a relatively predictable manner:

# COMMAND ----------

df = spark.table("rfm_silver").selectExpr("T as Term", "frequency", "monetary_value").toPandas()
px.histogram(df, x="Term", nbins=20, histnorm='percent')

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Examine Frequency Distribution
# MAGIC 
# MAGIC Spending should follow a very similar pattern with most customers spending a certain amount with each transaction and occassionally spending a bit more. This average amount of spend evolves over the customer's lifetime just like frequency does until the customer is no longer engaged.

# COMMAND ----------

# DBTITLE 1,Examine Frequency Distribution
px.histogram(df[df["frequency"]<50], x="frequency", nbins=40, histnorm='percent')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Verify No Relationship Between Frequency & Per-Transaction Spend
# MAGIC 
# MAGIC A key assumuption of our models is that the frequency of a customer's engagement does not dictate the amount they spend with each transaction. In otherwords, a customer is not expected to spend less with each transaction just because he or she engages in transactions more often. The independence of our frequency and monetary values is something we can formally test, but a quick visualization can often help us see if there is a problematic relationship between the two that might necessitate a different approach to CLV:
# MAGIC 
# MAGIC Look for an inverse relationship between frequency and monetary value.  If monetary value decreases as frequency increases, you have a problem.  In this dataset, there is a very slight relationship, but it is low enough that we can ignore it.

# COMMAND ----------

# DBTITLE 1,Verify No Relationship Between Frequency & Per-Transaction Spend
sns.relplot(x="monetary_value", y="frequency", data=df);

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### RFM Segmentation
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-4.png' style="float:right" width="600">
# MAGIC 
# MAGIC CLV estimation is a popular technique for quantifying the value we expect to receive from a customer relationship over an extended period, but the complexity of the technique makes it a bit excessive for scenarios where we simply need to divide customers into broad groups based on their value potential.
# MAGIC 
# MAGIC A much simpler technique, popularly known as *RFM Segmentation*, leverages the same RFM metrics calculated above to identify clusters of customers with similar demonstrated patterns of engagement.  This technique has been employed since the 1960s to inform marketing strategies and have been modernized in recent decades to take advantage of Machine Learning techniques to uncover the most meaningful patterns within the data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert RFM metrics to scores
# MAGIC 
# MAGIC RFM Segmentation typically starts with the conversion of the raw recency, frequency and monetary value metrics into ordinal values between 1 and 5 (or 1 and 10) where the higher ordinal values represent better metric values:

# COMMAND ----------

# DBTITLE 1,Convert Raw Metrics to RFM Scores
# extract the quantile cutoffs for each metric
bin_count = 5 
quantiles = np.linspace(0.0, 1.0, num=bin_count+2).tolist()[1:-1]

def convert_metrics_to_scores(metric, df, asc=True):
  quantile = df.approxQuantile(metric, quantiles, 0)
  #>= for asc order only
  def compare(i):
    return df[metric] >= quantile[i] if asc else df[metric] > quantile[i]
  i = 0 if asc else 6
  return df.withColumn(metric+'_score',
              f.when(compare(3), abs(1-i)).
              when(compare(2), abs(2-i)).
              when(compare(1), abs(3-i)).
              when(compare(0), abs(4-i)).
              otherwise(abs(5-i)))


# convert metrics to scores
scores = convert_metrics_to_scores('recency', rfm)
scores = convert_metrics_to_scores('frequency', scores, False)
scores = convert_metrics_to_scores('monetary_value', scores, False)


display(scores.orderBy('customer_id'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Implement clustering model
# MAGIC 
# MAGIC Leveraging clustering techniques, we can then explore our data for meaningful groupings of data. 
# MAGIC 
# MAGIC Here we will uncover 4-groups in our data, each of which tells a different story:
# MAGIC 
# MAGIC * **(Cluster 0) Anchor Customers** - High value customers with moderate patterns of re-engagement.  Keep these folks happy and moving as-is.
# MAGIC * **(Cluster 1) Lost Opportunities** - High value, high frequency customers that we haven't seen in a while.  We need to figure out why these customers are leaving us and try to win them back.
# MAGIC * **(Cluster 2) Newcomers** - High recency but low frequency and low value customers.  We need to understand how to expand their use of the store to increase frequency and cart size.
# MAGIC * **(Cluster 3) Slipping Opportunities** - Above average spending and high frequency but moderate recency.  These customers are not lost but we need to understand why they may be enagaging less recently. 

# COMMAND ----------

# DBTITLE 1,Cluster on RFM Scores with KMeans
scores_pd = scores.toPandas()
training = scores_pd[['recency_score', 'frequency_score', 'monetary_value_score']]
# Number of clusters determined through elbow technique (not shown)
# For a more detailed segmentation MLflow demo, visit the "../04-DataScience-ML-use-cases/04.2-customer-segmentation" notebook
n_clusters = 4 

with mlflow.start_run(run_name='CLV model') as run:  
  # train clustering model
  km = KMeans(n_clusters=n_clusters, init='random', random_state=42)
  km.fit(training)
  cluster_names = {"0": "Anchor Customers", "1": "Lost Opportunities", "2": "Newcomers", "3": "Slipping Opportunities"}

  # assign each customer to a cluster
  scores_pd['cluster'] = km.predict(training)
  # extract cluster centroids
  df = pd.DataFrame([], columns = ['cluster','R','F','M'])

  for i, center in enumerate(km.cluster_centers_):
    center_score = [str(i)+": "+cluster_names[str(i)]]
    for score in center:
      center_score += [score]
    df.loc[len(df)] = center_score

  fig = px.histogram(df, x="cluster", y=['R', 'F', 'M'], barmode='group')
  mlflow.log_figure(fig, "cluster.html")
  mlflow.log_dict(cluster_names, "cluster_names.json")
display(fig)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### What's next?
# MAGIC 
# MAGIC <img width=500 src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/customer-segmentation-dashboard-satisfaction.png" style="float:right" />
# MAGIC 
# MAGIC We now have our customer segmentation ready, based on RFM scores! 
# MAGIC 
# MAGIC A typical next step is to send this information downstream to the Marketing team and start building Dashboard to measure the customer churn/retention evolution based on the marketing actions. 
# MAGIC 
# MAGIC Optional: Open [Databricks SQL Dasboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/dbb7f120-9c58-471f-9817-a7c0e1b9cbe6-customer-segmentation---satisfaction?o=1444828305810485) to analyse customer satisfaction based on our segments.
# MAGIC 
# MAGIC 
# MAGIC The power of Databricks is found not only in its ability to leverage the cloud to perform complex calculations in a timely manner but in its flexibility to deliver a wide range of outputs from this data.  
# MAGIC 
# MAGIC The introduction of RFM segmentation in this demo is intended to highlight that the same customer data can be used in a variety of ways to achieve our business objectives. 
# MAGIC 
# MAGIC Next, we'll see a more advanced use-case: forecasting CLV.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3: Compute and Forecast CLV for the next 3 years

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Compute RFM value with holdout to train our model forecasting CLV
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-5.png' style="float:right" width="600">
# MAGIC 
# MAGIC For us to predict CLV, we need a metric to evaluate our model. A strategy is to compute the RFM as it was 90 days ago and train our model using this data.
# MAGIC 
# MAGIC Because we know the current CLV value, we'll be able to measure our model performance by measuring the error between the prediction and the actual value.
# MAGIC 
# MAGIC This will be designed as *holdout* data, similar to the usual train/test dataset split: 
# MAGIC 
# MAGIC * Train a model on CLV as it was 90 days ago
# MAGIC * Use the model to predict CLV in 90 days
# MAGIC * Test the model comparing the predictions with the actual CLV value
# MAGIC 
# MAGIC Again, the work of condensing large numbers of transactions to the now expanded set of per-customer metrics demands quite a bit of computational horsepower:

# COMMAND ----------

# DBTITLE 1,Per-Customer Metrics with Holdout
HOLDOUT_DAYS = 90

# combine customer history with date info (CUSTOMER HISTORY)
# note: date_sub requires a single integer value unless employed within an expr() call
customer_history_holdout = customer_history.withColumn('duration_holdout', f.lit(HOLDOUT_DAYS)) \
                                           .withColumn('current_dt_minus_holdout', f.expr('date_sub(current_dt, duration_holdout)'))

# calculate relevant metrics by customer
a = (customer_history_holdout
       .where(col('transaction_at') < col('current_dt_minus_holdout'))
       .groupBy(col('customer_id'), col('current_dt'), col('duration_holdout'), col('current_dt_minus_holdout'), col('first_at'))
       .agg(
          (f.countDistinct(col('transaction_at'))-1).cast('float').alias('frequency_cal'),
          f.datediff( f.max(col('transaction_at')), col('first_at')).cast('float').alias('recency_cal'),
          f.datediff(col('current_dt_minus_holdout'), col('first_at')).cast('float').alias('T_cal'),
          f.when( f.countDistinct(col('transaction_at'))==1,0)                           # MONETARY VALUE
              .otherwise(
              f.sum(
                  f.when(col('first_at')==col('transaction_at'),0)
                      .otherwise(col('salesamount'))
              )/( f.countDistinct(col('transaction_at'))-1)
          ).alias('monetary_value_cal'))
       .withColumn('monetary_value_cal', f.expr('case when monetary_value_cal < 0 then 0 else round(monetary_value_cal, 2) end')))


b = (customer_history_holdout
      .where((col('transaction_at') >= col('current_dt_minus_holdout')) & (col('transaction_at') <= col('current_dt')))
      .groupBy(col('customer_id'))
      .agg(
        f.countDistinct(col('transaction_at')).cast('float').alias('frequency_holdout'),
        f.avg(col('salesamount')).alias('monetary_value_holdout')))

rfm_train_test = (a.join(b, ['customer_id'], how='left')
                   .select(
                       col('customer_id').alias('CustomerID'),
                       'frequency_cal',
                       'recency_cal',
                       'T_cal',
                       'monetary_value_cal',
                       f.coalesce(col('frequency_holdout'), f.lit(0.0)).alias('frequency_holdout'),
                       f.coalesce(col('monetary_value_holdout'), f.lit(0.0)).alias('monetary_value_holdout'),
                       col('duration_holdout'))
                  .withColumn('monetary_value_holdout', f.expr('case when monetary_value_holdout < 0 then 0 else round(monetary_value_holdout, 2) end'))
                 .orderBy('customer_id')
              )
rfm_train_test.write.mode('overwrite').saveAsTable("rfm_holdout_silver")

# COMMAND ----------

# MAGIC %sql select * from rfm_holdout_silver

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ### Train CLV model
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-6.png' style="float:right" width="600">
# MAGIC 
# MAGIC We now have the required data to build the CLV model.
# MAGIC 
# MAGIC CLV models are fairly complex and involve advance mathematics. To simplify our implementation, we'll use the *lifetime* library.
# MAGIC 
# MAGIC To estimate CLV for a given customer, we need to train two models:  
# MAGIC  
# MAGIC * One model estimates the amount we should expect a customer to spend with us in each period that constitutes a customer lifetime.  
# MAGIC * The other model estimates the probability the custoemr will remain engaged through the end of that period given the patterns of engagement drop-off we see in the customer population.  
# MAGIC 
# MAGIC *NOTE: These models need to have customers with at least one repeat transaction within the dataset, and we should remove any extreme outliers that might skew our results.*

# COMMAND ----------

# DBTITLE 1,Train & Evaluate Value Model
#Remove Non-Repeating Customers & Outliers
rfm_train_test_pd = spark.table("rfm_holdout_silver").filter(f.expr('frequency_cal > 0 AND monetary_value_cal > 0')).toPandas()

# train the model
spend_model = GammaGammaFitter(penalizer_coef=0.003)
spend_model.fit(rfm_train_test_pd['frequency_cal'], rfm_train_test_pd['monetary_value_cal'])

# score the model
def rmse(actuals, predicted):
  return np.sqrt(np.sum(np.square(actuals-predicted))/actuals.shape[0])

spend_prediction = spend_model.conditional_expected_average_profit(rfm_train_test_pd['frequency_holdout'], rfm_train_test_pd['monetary_value_holdout'])
spend_prediction[spend_prediction < 0] = 0 # lifelines will occcassionally predict a negative spend value due to the distribution of inputs

spend_rmse = rmse(rfm_train_test_pd['monetary_value_holdout'], spend_prediction)

print(f'Value Model RMSE: {spend_rmse}')

# COMMAND ----------

# DBTITLE 1,Train and evaluate frequency model
# train the model
retention_model = ParetoNBDFitter(penalizer_coef=1.0)
retention_model.fit(rfm_train_test_pd['frequency_cal'], rfm_train_test_pd['recency_cal'], rfm_train_test_pd['T_cal'])

# score the model
retention_prediction = retention_model.predict(rfm_train_test_pd['duration_holdout'], rfm_train_test_pd['frequency_cal'], rfm_train_test_pd['recency_cal'], rfm_train_test_pd['T_cal'])
retention_rmse = rmse(rfm_train_test_pd['frequency_holdout'], retention_prediction)

print('Retention Model RMSE: {0}'.format(retention_rmse))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Model Analysis
# MAGIC 
# MAGIC The error scores associated with our value and retention models are good but not perfect.  We can visually examine this by exploring predicted and actual purchases for our holdout data.  From the results, we can see that our predicted purchases follow the mean expectations for purchase frequency as the time since the last purchase increases: 

# COMMAND ----------

# DBTITLE 1,Predicted vs. Actual Purchases
from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases

plot_calibration_purchases_vs_holdout_purchases(
  retention_model, 
  rfm_train_test_pd, 
  kind='time_since_last_purchase', 
  n=90, 
  **{'figsize':(8,8)}
  )

# COMMAND ----------

# MAGIC %md Similarly we can examine the actual vs. predicted spend in the holdout period for each customer. Projected as a histogram, we can see we are slightly overestimating spending for this period but not by too large an amount:

# COMMAND ----------

# DBTITLE 1,Predicted vs. Actual Spend
monetary_actual = rfm_train_test_pd['monetary_value_holdout']

spend_fig_eval = go.Figure()
spend_fig_eval.add_trace(go.Histogram(x=spend_prediction[spend_prediction < 3000], name='Predictions', marker_line_width=0.5))
spend_fig_eval.add_trace(go.Histogram(x=monetary_actual[monetary_actual< 3000], name='Actual spend', opacity=0.6, marker_line_width=0.5))
spend_fig_eval.update_layout(barmode='overlay')

# COMMAND ----------

# MAGIC %md While not perfect, these models allow us to make reasonable estimations of CLV given simple R, M, F & T inputs.  To productionize this, we might envision a scenario where our model is converted into a function and per-customer recency, frequency, monetary value and term metrics are periodically recalculated and passed to the models, allowing us to generate estimates which can be passed to downstream systems to enable more profitable customer engagement:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Deploy CLV model in production with MLFlow 
# MAGIC 
# MAGIC We now have our model ready. The next step is to take this model and save it to MLFlow, adding move it to the Model registry, and flag it as Production ready.

# COMMAND ----------

# DBTITLE 1,Save Lifetime models to MLFlow
# save model run to mlflow
with mlflow.start_run(run_name='CLV model') as run:  
  #ClvModelWrapper is defined in the _resources notebook
  model = ClvModelWrapper(spend_model, retention_model)
  #save the figure as model artifact
  mlflow.log_figure(spend_fig_eval, 'predicted_vs_actual_spend.html')
  sample = rfm[['recency','frequency','monetary_value','T']].limit(10).toPandas()
  # log our spend model to mlflow
  mlflow.pyfunc.log_model(
    'model', 
    python_model = ClvModelWrapper(spend_model, retention_model), 
    conda_env = lifetime_conda_env,
    signature = infer_signature(sample, model.predict(None, sample)),
    input_example = sample.iloc[1].to_dict())

# COMMAND ----------

# DBTITLE 1,Flag model as Production-ready in MLFlow Registry
model_registered = mlflow.register_model("runs:/"+run.info.run_id+"/model", "field_demos_clv_model")
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_clv_model", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Run inference and forecast CLV for 3 years
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-clv-flow-7.png' style="float:right" width="600">
# MAGIC 
# MAGIC We now have our model fully packaged and deployed in MLFlow registry.
# MAGIC 
# MAGIC An other team can easily pickup the model generated without knowing the details and use it to predict the CLV metrics.
# MAGIC 
# MAGIC These metrics can then be saved in another table and use to take actions and drive revenue growth.

# COMMAND ----------

# define function based on mlflow recorded model
clv_3yr_udf = mlflow.pyfunc.spark_udf(spark, 'models:/field_demos_clv_model/Production')
spark.udf.register('clv_3yr', clv_3yr_udf)

# COMMAND ----------

# DBTITLE 1,Estimate CLV for the entire dataset
# MAGIC %sql 
# MAGIC SELECT *, round(clv_3yr(recency, frequency, monetary_value, T), 2) as clv_prediction_3_years FROM rfm_silver WHERE frequency > 0

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC Based on some simple metrics derived from transactional information associated with each customer, we are now able to estimate a 3-year customer lifetime value for each customer.  We know from our analysis that our estimates are not perfect but they do highlight differences between each customer's potential that may be used by Marketing to target individuals with specific offerings and promotions which maximize the profit potential of the customer relationship over the long term.
# MAGIC 
# MAGIC In addition, we have seen how Databricks lakehouse is uniquely positioned to help growing revenues:
# MAGIC 
# MAGIC 
# MAGIC * Simplifying data ingestion and RFM metrics computation at scale
# MAGIC * Building customers segmentations or CLV models for Marketing teams to take strategic actions and increase retention
# MAGIC * Track customers churn and revenue evolution using Databricks SQL Dashboards
