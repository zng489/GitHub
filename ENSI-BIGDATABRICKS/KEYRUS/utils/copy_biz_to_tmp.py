# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import pyspark.sql.functions as f

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Even if you've got a KPI table, only one is passed as parameter, since we do it in two steps in ADF

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### This one is type string, no need to unpack dict

# COMMAND ----------

var_adf_job_run_id = dbutils.widgets.get("adf_job_run_id")

# COMMAND ----------

source = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
source

# COMMAND ----------

df = spark.read.parquet(source)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SQL DW doesn't allow Struct Types

# COMMAND ----------

if 'kv_process_control' in df.columns:
  df = df.drop('kv_process_control')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Polybase doesn't allow Date Type

# COMMAND ----------

date_columns = [column for column, _type in df.dtypes if _type.lower() == 'date']
for date_column in date_columns:
  df = df.withColumn(date_column, f.col(date_column).cast('Timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Way to go through it now will be to write it back to ADLS Gen1 as parquet in /tmp dir.

# COMMAND ----------

var_tmp_data_path = "{}{}/{}{}".format(var_adls_uri,
                                       var_dls["folders"]["staging"], 
                                       var_adf_job_run_id,
                                       re.sub("/", "_", var_tables["destination"]))
var_tmp_data_path

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Polybase with partition discovery enabled causes Thread Exception, to work around it must save the dataframe without partitions

# COMMAND ----------

df.write.save(path=var_tmp_data_path, format="parquet", mode="overwrite")
