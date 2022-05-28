# Databricks notebook source
# from cni_connectors_fornecedor import adls_gen1_connector as adls_conn
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

territorio = 'MUNICIPIO'

# COMMAND ----------

# table = {
#   'schema': 'inep_saeb',
#   'table': 'resultado',
#   'prm_path': '/tmp/dev/prm/usr/inep_saeb/FIEC_INEP_saeb_resultado_municipio_mapeamento_unificado_raw.xlsx'
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_saeb_rendimento",
#   "adf_pipeline_run_id": "",
#   "adf_trigger_id": "",
#   "adf_trigger_name": "",
#   "adf_trigger_time": "teste",
#   "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/tmp/dev/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
lnd_path

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/saeb_{table}_{territorio}_unificado".format(raw=raw, schema=table["schema"], table=table["table"], territorio=territorio.lower())

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

adl_sink


# COMMAND ----------

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .upper())
  

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

for sub_plan in var_prm_dict.values():
  for row in sub_plan:
    row[0] = __normalize_str(row[0])
  

# COMMAND ----------

#var_prm_dict

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

_df = None
for lnd_year_path in sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: int(p.split('/')[-1])):
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)
  
  year = lnd_year_path.split('/')[-1]

  files_folder = cf.list_subdirectory(dbutils, lnd_year_path)
  for i, file in enumerate(files_folder):
    path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)
  
    if 'TS_' + territorio in path_origin.upper():
    
      df = spark.read.parquet(path_origin)
      cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=year)

      df = df.select(*__transform_columns())
      df = df.withColumn("ANO", f.lit(year))
      
      if _df:
        _df = _df.union(df)
      else:
        _df = df

  if _df:
    df = _df
  

  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

  (df
   .write
   .partitionBy('ANO')
   .parquet(path=adl_sink, mode='overwrite'))
  
  

# COMMAND ----------

# df.display()


# COMMAND ----------

