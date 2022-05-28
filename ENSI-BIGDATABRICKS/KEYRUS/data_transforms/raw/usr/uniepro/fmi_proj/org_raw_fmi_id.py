# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # IMPORTANT NOTES FOR ADF IMPLEMENTATION
# MAGIC 
# MAGIC This file after, processing, must go to /ach folder, into which we recplicate the complete structure of the landing folder.
# MAGIC </br>In the lowest level, we'll have a folder with the integer that represents dh_insercao_raw, for keeping the version for this file.
# MAGIC 
# MAGIC <b>Example:</b>
# MAGIC </br>source: /uld/uniepro/fmi/proj 
# MAGIC </br>sink: /ach/uniepro/fmi/proj 
# MAGIC </br>*20200212134522* refers to the integer representation of dh_insercao_raw
# MAGIC 
# MAGIC For now this notebook serves the purpose of mocking and testing ADF pipeline execution. 

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index

import crawler.functions as cf
import json
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

# MAGIC %md
# MAGIC This cell is for implementing widgets.get and json convertion
# MAGIC Provided that it is still not implemented, i'll mock it up by setting the necessary stuff for the table I'm working with.
# MAGIC 
# MAGIC Remember that when parsing any json, we must handle any possibility of strange char, escapes ans whatever comes dirt from Data Factory!

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']
sys = var_dls['systems']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_sink = "{adl_path}{raw}/usr/{namespace}/{file_folder}".format(adl_path=var_adls_uri, raw=raw, namespace=var_file['namespace'], file_folder=var_file['file_folder'])

# COMMAND ----------

import crawler.functions as cf

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % var_source)

# COMMAND ----------

df = spark.read.csv(path=var_adls_uri + var_source, sep=';', encoding='UTF-8', header=True,
                    nullValue='', mode="PERMISSIVE", ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True,
                   inferSchema=True)


# COMMAND ----------

dt_insertion_raw = var_adf["adf_trigger_time"].split(".")[0]

# COMMAND ----------

df = cf.append_control_columns(df, dt_insertion_raw)

# COMMAND ----------

adl_file_time = cf.list_adl_files(spark, dbutils, var_source)
df = df.join(adl_file_time, on='nm_arq_in', how='inner')

# COMMAND ----------

df.write.save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

from datetime import datetime
var_dh_insercao_raw = datetime.strptime(dt_insertion_raw, '%Y-%m-%dT%H:%M:%S').strftime('%Y%m%d%H%M%S')

dbutils.notebook.exit('{"var_dh_insercao_raw": "%s"}' % var_dh_insercao_raw)

# COMMAND ----------


