# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

import json
import re
import os
import crawler.functions as cf
import pyspark.sql.functions as f

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'schema': 'inep_prova_brasil',
#   'table': 'aluno',
#   'prm_path': '/prm/usr/inep_prova_brasil/KC2332_Prova_Brasil_mapeamento_unificado_raw_V1.xlsx',
#   'sheets': ['PB_TS_RESPOSTA_ALUNO', 'PB_TS_QUEST_ALUNO', 'PB_TS_RESULTADO_ALUNO']
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_estrutura_territorial",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw"}}

# COMMAND ----------

def remove_ba_layer(folder):
  val = 2 if folder.startswith('/') else 1
  return '/'.join(folder.split('/')[val:])

# COMMAND ----------

headers = {'name_header':'Tabela Origem','pos_header':'B','pos_org':'C','pos_dst':'E','pos_type':'F',}
metadata = {'name_header':'Processo','pos_header':'B'}

# COMMAND ----------

parse, metadata = cf.parse_ba_doc(dbutils, table['prm_path'], headers, metadata, sheet_names=table['sheets'])

# COMMAND ----------

dt_insertion_raw = adf["adf_trigger_time"].split(".")[0]

# COMMAND ----------

for tbl_sheet in parse:
  mtd = metadata[tbl_sheet]
  lnd_path = '{adl_path}{lnd}/{dst}'.format(adl_path=var_adls_uri, lnd=dls['folders']['landing'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_ORIGEM']))
  
  df = spark.read.csv(path=lnd_path, sep=';', header=True)
  if 'PROFICIENCIA_LP_SAEB' in df.columns:
    df = df.withColumn('PROFICIENCIA_LP_SAEB', f.regexp_replace(f.col('PROFICIENCIA_LP_SAEB'), ',', '.'))
  
  if 'PROFICIENCIA_MT_SAEB' in df.columns:
    df = df.withColumn('PROFICIENCIA_MT_SAEB', f.regexp_replace(f.col('PROFICIENCIA_MT_SAEB'), ',', '.'))
  
  for org, dst, _type in parse[tbl_sheet]:
    df = df.withColumn(org, f.col(org).cast(_type))
    if org != dst:
      df = df.withColumnRenamed(org, dst)
  
  df = cf.append_control_columns(df, dt_insertion_raw)
  df_adl_files = cf.list_adl_files(spark, dbutils, os.path.dirname(mtd['TABELA_ARQUIVO_ORIGEM']))
  df = df.join(df_adl_files, on='nm_arq_in', how='inner')
  
  adl_raw_dir = '{adl_path}{raw}/{dst}'.format(adl_path=var_adls_uri, raw=dls['folders']['raw'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_DESTINO']))
  df.repartition(10).write.parquet(adl_raw_dir, mode='overwrite')