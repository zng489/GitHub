# Databricks notebook source
#from cni_connectors_fornecedor import adls_gen1_connector as adls_conn
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.window import Window

import crawler.functions as cf
import datetime
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

try:
  table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
#tmp/dev/lnd/crw/ibge__deflatores
except:
  table = {
   "schema": "ibge",
   "table": "deflatores",
   "table_dict": "pnad_dicionario",
   "table_dest": "pnadc"
 }
  adf = {
   "adf_factory_name": "cnibigdatafactory",
   "adf_pipeline_name": "org_raw_caged",
   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
   "adf_trigger_type": "PipelineActivity"
 }

  dls = {"folders":{"landing":"/tmp/dev/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/tmp/dev/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
lnd_path

# COMMAND ----------

adl_sink = "{adl_path}{raw}/crw/{schema}/{table}".format(adl_path=var_adls_uri, raw=raw, schema=table["schema"], table=table["table"])
adl_sink

# COMMAND ----------

lnd_uri = '{adl}/{lnd}/'.format(adl=var_adls_uri, lnd=lnd_path)
df = spark.read.parquet(lnd_uri)
df_columns = df.columns

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

sqlContext.registerDataFrameAsTable(df, 'table');

# COMMAND ----------

df = df.withColumn('Ano', f.col('ano').cast('Int'))
df = df.withColumn('UF', f.col('UF').cast('Int'))

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, lnd_path)
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------

ordered_columns = df_columns[:] + ['nr_reg', 'nm_arq_in', 'dh_arq_in', 'dh_insercao_raw']
df = df.select(*ordered_columns)

# COMMAND ----------

df.write.parquet(adl_sink, mode='overwrite')