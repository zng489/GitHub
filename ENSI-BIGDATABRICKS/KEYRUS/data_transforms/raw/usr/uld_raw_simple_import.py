# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### When can I use this notebook?
# MAGIC 
# MAGIC Only if you don't need any data transformation when importing the data from /uld to /raw

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

# var_file = {
#   'namespace': 'inep_enem',
#   'file_folder': 'parametros_rampa',
#   'extension': 'csv',
#   'column_delimiter': ';',
#   'encoding': 'UTF-8',
#   'null_value': ''
# }

# var_adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_base_escolas",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# var_dls = {"folders":{"landing":"/uld","error":"/err","staging":"/stg","log":"/log","raw":"/raw","archive":"/ach"}}

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_source

# COMMAND ----------

var_sink = ("{adl_path}{raw}/usr/{namespace}/{file_folder}"
            .format(adl_path=var_adls_uri, raw=raw, namespace=var_file['namespace'], file_folder=var_file['file_folder']))
var_sink

# COMMAND ----------

import crawler.functions as cf

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % var_source)

# COMMAND ----------

df = spark.read.csv(path=var_adls_uri + var_source, 
                    sep=var_file['column_delimiter'], 
                    encoding=var_file['encoding'], 
                    nullValue=var_file['null_value'],
                    mode='FAILFAST', 
                    header=True,
                    inferSchema=True,
                    ignoreLeadingWhiteSpace=True, 
                    ignoreTrailingWhiteSpace=True)

# COMMAND ----------

# Files are tricky considering columns. Let's lower them all.
for c in df.columns:
  df = df.withColumnRenamed(c, c.lower())

# COMMAND ----------

dt_insertion_raw = var_adf["adf_trigger_time"].split(".")[0]
dt_insertion_raw

# COMMAND ----------

df = cf.append_control_columns(df, dt_insertion_raw)

# COMMAND ----------

adl_file_time = cf.list_adl_files(spark, dbutils, var_source)
df = df.join(adl_file_time, on='nm_arq_in', how='inner')

# COMMAND ----------

df.write.save(path=var_sink, format="parquet", mode="overwrite")