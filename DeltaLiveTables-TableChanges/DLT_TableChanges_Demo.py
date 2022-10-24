# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test 1: Ignore Changes

# COMMAND ----------

@dlt.table(
  comment="Can we ignore changes?"
  ,table_properties={"quality":"bronze"
                    }
)
def ignore_changes():
  
  ignore_changes = (spark.readStream
                    .option("ignoreChanges", "true")
                    .table("shabbir_khanbhai_golden_demo.gl_total_loan_balances_1")
                    )
  
  return (ignore_changes)

# COMMAND ----------

@dlt.table(
  comment="Can we use the ignore changes table?"
  ,table_properties={"quality":"silver"
                    }
)
def ignore_changes_step2():
  
  ignore_changes = (dlt.read_stream("ignore_changes").filter(col("bal") > 1000000)
                    )
  
  return (ignore_changes)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test 2: Read change data feed

# COMMAND ----------

@dlt.table(
  comment="Random table changes from an example"
  ,table_properties={"quality":"bronze"
                    }
)
def read_table_changes():
  
  tblchanges = (spark.readStream.option("readChangeFeed", "true") 
  .option("startingVersion", 37)
  .table("shabbir_khanbhai_golden_demo.gl_total_loan_balances_2")
                    )
  
  return (tblchanges)

# COMMAND ----------

@dlt.table(
  comment="Can we ingest the output of this as another table?"
  ,table_properties={"quality":"silver"
                    }
)
def filtered_table_changes():
  
  test = (dlt.read_stream("read_table_changes").filter(col("bal") == 1))
  
  return (test)

# COMMAND ----------

dlt.create_target_table(
  name = "target_SCD1",
  comment = "Can we ingest the output as CDC feed?"
)
  
dlt.apply_changes(
  target = "target_SCD1",
  source = "read_table_changes",
  keys = ["location_code"],
  sequence_by = "_commit_version",
  ignore_null_updates = False,
  apply_as_deletes = expr("_change_type = 'DELETE'"),
)

# COMMAND ----------

dlt.create_target_table(
  name = "target_SCD2",
  comment = "What about SCD Type 2?"
)
  
dlt.apply_changes(
  target = "target_SCD2",
  source = "read_table_changes",
  keys = ["location_code"],
  sequence_by = "_commit_version",
  ignore_null_updates = False,
  apply_as_deletes = expr("_change_type = 'DELETE'"),
  stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test 3: CDC an arbitrary Delta Table

# COMMAND ----------

@dlt.view()
def source():
  return(spark.readStream.table("shabbir_khanbhai_golden_demo.gl_total_loan_balances_2"))

dlt.create_target_table(
  name = "target_test",
  comment = "What about streaming any Delta table?"
)
  
dlt.apply_changes(
  target = "target_test",
  source = "source",
  keys = ["location_code"],
  sequence_by = "bal"
)
