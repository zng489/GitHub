# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

import crawler.functions as cf
import pyspark.sql.functions as f
import re
import json

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

var_table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# # USE THIS FOR DEVELOPMENT PURPOSES ONLY

# var_dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw", "user_landing":"/uld"}}
# var_table = {"schema": "ibge", "table": "relatorio_dtb_brasil_municipio"}
# var_adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_estrutura_territorial",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# COMMAND ----------

lnd = var_dls["folders"]["landing"]
raw = var_dls["folders"]["raw"]

# COMMAND ----------

adl_dir = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=var_table["schema"], table=var_table["table"])
df_adl_files = cf.list_adl_files(spark, dbutils, adl_dir)

# COMMAND ----------

df = spark.read.parquet(var_adls_uri + adl_dir)

# COMMAND ----------

df = cf.append_control_columns(df, dh_insercao_raw=var_adf["adf_trigger_time"].split(".")[0])

# COMMAND ----------

df = df.join(df_adl_files, on='nm_arq_in', how='inner')

# COMMAND ----------

sink = '{raw}/crw/{schema}/{table}'.format(raw=raw, schema=var_table["schema"], table=var_table["table"])
sink_exists = cf.directory_exists(dbutils, sink)

var_sink = var_adls_uri + sink
if sink_exists:
  df_sink = spark.read.parquet(var_sink)
  df = df.join(df_sink, on='CODIGO_MUNICIPIO_COMPLETO', how='left_anti')

# COMMAND ----------

mode = 'append' if sink_exists else 'overwrite'
mode

# COMMAND ----------

df.coalesce(1).write.parquet(var_sink, mode=mode)