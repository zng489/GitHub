# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize

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
#   'schema': 'inep_afd',
#   'table': 'brasil_regioes_e_ufs',
#   'prm_path': '/prm/usr/inep_afd/KC2332_Painel_Indicadores_educacionais_AFD_brasil_regiao_mapeamento_uni_raw.xlsx'
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

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
lnd_path

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)
adl_sink

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[year]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

for lnd_year_path in sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: int(p.split('/')[-1])):
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)
  
  year = lnd_year_path.split('/')[-1]
  for i, file in enumerate(cf.list_subdirectory(dbutils, lnd_year_path)):
    path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)
  
    df = spark.read.parquet(path_origin)
    cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=year)
    
    df = df.select(*__transform_columns())
    df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
    df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')
    
    (df
     .coalesce(1)
     .write
     .partitionBy('NU_ANO_CENSO')
     .parquet(path=adl_sink, mode='overwrite'))