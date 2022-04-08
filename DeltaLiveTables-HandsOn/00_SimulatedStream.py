# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Reset Folder structure

# COMMAND ----------

# MAGIC %run ./Includes/autoloader-setup $mode = "reset"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Simulate single new file

# COMMAND ----------

simulate.arrival('once')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Simulate continuous new files
# MAGIC 
# MAGIC - Will run up to 200 file parts
# MAGIC - Cancel the query to pause the simulated streaming

# COMMAND ----------

simulate.arrival('continuous')

# COMMAND ----------

# MAGIC %md #### Inspect location for debugging

# COMMAND ----------

userhome

# COMMAND ----------

#Are the files actually available to stream?
dbutils.fs.ls(f'{userhome}/raw/')

# COMMAND ----------

#Are the files actually landing in their destination?
dbutils.fs.ls(f'{userhome}/airlines/')

# COMMAND ----------

# Do the json files have named column names?
dbutils.fs.head(f'{userhome}/airlines/part-0')
