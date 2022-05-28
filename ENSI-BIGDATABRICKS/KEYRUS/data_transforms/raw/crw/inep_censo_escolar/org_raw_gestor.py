# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	org_raw_microdados_gestor				
# MAGIC Tabela/Arquivo Origem	/lnd/crw/inep_censo_escolar__gestor
# MAGIC Tabela/Arquivo Destino	/raw/crw/inep_censo_escolar_gestor							
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

try:
  table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
except:
  from datetime import datetime
  
  table = {
    'schema': 'inep_censo_escolar',
    'table': 'gestor'
  }

  adf = {
    "adf_factory_name": "cnibigdatafactory",
    "adf_pipeline_name": "org_raw_microdados_gestor",
    "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
    "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
    "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
    "adf_trigger_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    "adf_trigger_type": "PipelineActivity"
  }

  dls = {"folders":{"landing":"/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/raw"}}

# COMMAND ----------

lnd = dls["folders"]["landing"]
raw = dls["folders"]["raw"]

# COMMAND ----------

origin_path = '{lnd}/crw/{schema}__{table}'.format(lnd=lnd, schema=table['schema'], table=table['table'])
origin_path

# COMMAND ----------

adl_origin = '{adl_path}/{lnd}/*/'.format(adl_path=var_adls_uri, lnd=origin_path)
adl_origin

# COMMAND ----------

adl_sink = "{adl_path}{raw}/crw/{schema}/{table}".format(adl_path=var_adls_uri, raw=raw, schema=table["schema"], table=table["table"])
adl_sink

# COMMAND ----------

df = spark.read.csv(adl_origin, header=True, sep='|')

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, origin_path)
df_adl_files = (df_adl_files
                .orderBy(f.col('dh_arq_in').desc())
                .drop_duplicates(subset=['nm_arq_in']))

# COMMAND ----------

df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------

df.coalesce(1).write.save(path=adl_sink, partitionBy='NU_ANO_CENSO', format="parquet", mode="overwrite")