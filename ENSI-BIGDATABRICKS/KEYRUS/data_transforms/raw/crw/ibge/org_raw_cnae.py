# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from pyspark.sql.window import Window

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'schema': 'ibge',
#   'table': 'cnae_subclasses'
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_caged",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders":{"landing":"/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

origin_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
origin_path

# COMMAND ----------

adl_sink = "{adl_path}{raw}/crw/{schema}/{table}".format(adl_path=var_adls_uri, raw=raw, schema=table["schema"], table=table["table"])
adl_sink

# COMMAND ----------

df = spark.read.parquet(var_adls_uri + origin_path)
df_columns = df.columns

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

# COMMAND ----------

partitions = []

for column in df_columns[:-3]:
  w = Window.orderBy('ID')
  df = df.withColumn('GRP_ID', f.sum((f.col(column) != f.lit('nan')).cast('Int')).over(w))

  w = Window.partitionBy(*partitions, 'GRP_ID')
  df = df.withColumn(column, f.first(column).over(w))
  
  partitions.append(column)

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, origin_path)
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------

ordered_columns = df_columns[:-1] + ['nr_reg', 'nm_arq_in', 'dh_arq_in', 'dh_insercao_raw']
df = df.select(*ordered_columns)

# COMMAND ----------

for na in df_columns[:-1]:
  df = df.withColumn(na, f.when(f.col(na) == f.lit('nan'), f.lit(None)).otherwise(f.col(na)))

# COMMAND ----------

df.write.parquet(adl_sink, mode='overwrite')