# Databricks notebook source
# from cni_connectors_fornecedor import adls_gen1_connector as adls_conn
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize
from datetime import date
  
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
#   'schema': 'catho',
#   'table': 'vagas',
#   'prm_path': '/tmp/dev/prm/usr/inep_rmd/FIEC_catho_vagas_mapeamento_unificado_raw.xlsx'
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_catho_vagas",
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

# lnd_path = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=lnd_path)

lnd_path

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

adl_sink


# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

columns_df = [
  'nm_arq_in', 
  'id', 
  'titulo', 
  'data', 
  'dataAtualizacao', 
  'salario', 
  'faixaSalarial', 
  'faixaSalarialId', 
  'salarioACombinar', 
  'jobQuantity', 
  'empId', 
  'companySize', 
  'tipoEmp', 
  'localPreenchimento', 
  'descricao', 
  'perfilId', 
  'ppdPerfilId', 
  'hrenova', 
  'horario', 
  'infoAdicional', 
  'regimeContrato', 
  'benef', 
  'entradaEmp', 
  'empContratou', 
  'grupoMidia', 
  'habilidades', 
  'ppdInfo', 
  'ppdFiltro', 
  'anunciante', 
  'contratante', 
  'vagas', 
  'url_vaga', 
  'nr_reg', 
  'dh_insercao_raw', 
  'dh_arq_in', 
  'dataRemovido'
]

# COMMAND ----------

lnd_date_path = sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: p.split('/')[-1])
lnd_date_path_max = max(lnd_date_path)

_df = None
df_adl_files = cf.list_adl_files(spark, dbutils, lnd_date_path_max)

files_folder = cf.list_subdirectory(dbutils, lnd_date_path_max)
for i, file in enumerate(files_folder):
  path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)

  df = spark.read.parquet(path_origin)
  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

  if _df:
    _df = _df.union(df)

  else:
    _df = df

if _df:
  df = _df

df = df.dropDuplicates(['id'])
df = df.withColumn('dataRemovido', f.lit(None).cast('string'))

df = df.select(columns_df)
  

# COMMAND ----------

def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------


first_load = file_exists(adl_sink)

if not first_load or len(dbutils.fs.ls(adl_sink))==0:
  (df
   .coalesce(1)
   .write
   .parquet(path=adl_sink, mode='overwrite'))
  
else:
  df_raw = spark.read.parquet(adl_sink)

  # novos registros
  df_raw_id = df_raw.select(f.col('id').alias('raw_id'))
  df_new = df.join(df_raw_id, df_raw_id.raw_id == df.id, 'leftanti')

  # contatena
  df_full = df_raw.union(df_new)

  # verifica ausentes
  df_lnd_id = df.select(f.col('id').alias('lnd_id'))
  df_full = df_full.join(df_lnd_id, df_lnd_id.lnd_id == df_full.id, 'left')
  
  df_full = df_full.withColumn('dataRemovido', f.when(f.col('lnd_id').isNull(), str(date.today())))
  df_full = df_full.drop(df_full.lnd_id)
  
  #carrega
  df = df_full
  (df
   .coalesce(1)
   .write
   .parquet(path=adl_sink, mode='overwrite'))
  

# COMMAND ----------


