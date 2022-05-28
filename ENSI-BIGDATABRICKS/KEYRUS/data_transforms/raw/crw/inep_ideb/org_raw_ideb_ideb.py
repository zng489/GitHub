# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

import crawler.functions as cf
import pyspark.sql.functions as f

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   "schema": "inep_ideb",
#   "table": "brasil_af",
#   "table_name": "ideb",
#   "merge_tables": [
#     "brasil",
#     "estados_regioes",
#     "municipios",
#     "escolas"
#   ],
#   "category_tables": [
#     "ai",
#     "af",
#     "em"
#   ],
#   "prm_path": "/prm/usr/inep_ideb/KC2332_IDEB_BRASIL_mapeamento_unificado_raw_V1.xlsx"
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

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table_name"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)
adl_sink

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

def iterate_landing_paths():
  for merge_table in table['merge_tables']:
    for category in table['category_tables']:
      table_name = "{table}_{category}".format(table=merge_table, category=category)
      yield "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table_name), category.upper()

# COMMAND ----------

def __transform_columns(sheet, category):
  for org, dst, _type in var_prm_dict[sheet]:
    if org == 'N/A':
      dft = category if dst == 'TIPO_ARQUIVO' else None
      yield f.lit(dft).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

def __execute_transformation(lnd_path, category, sheet):
  df = spark.read.parquet(var_adls_uri + lnd_path)
  cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet)
  
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_path)
  
  df = df.select(*__transform_columns(sheet, category))
  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')
  
  return df

# COMMAND ----------

cf.delete_files(dbutils, raw_path)

# COMMAND ----------

iterator = iter(iterate_landing_paths())

lnd_path, category = next(iterator)
df_brasil_ai = __execute_transformation(lnd_path, category, sheet='IDEB_BRASIL_AI')
df_brasil_ai.write.parquet(adl_sink, mode='overwrite')

# COMMAND ----------

lnd_path, category = next(iterator)
df_brasil_af = __execute_transformation(lnd_path, category, sheet='IDEB_BRASIL_AF')
df_brasil_af.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_brasil_em = __execute_transformation(lnd_path, category, sheet='IDEB_BRASIL_EM')
df_brasil_em.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_estado_regiao_ai = __execute_transformation(lnd_path, category, sheet='IDEB_ESTADO_REGIAO_AI')
df_estado_regiao_ai.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_estado_regiao_af = __execute_transformation(lnd_path, category, sheet='IDEB_ESTADO_REGIAO_AF')
df_estado_regiao_af.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_estado_regiao_em = __execute_transformation(lnd_path, category, sheet='IDEB_ESTADO_REGIAO_EM')
df_estado_regiao_em.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_municipio_ai = __execute_transformation(lnd_path, category, sheet='IDEB_MUNICIPIO_AI')
df_municipio_ai.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_municipio_af = __execute_transformation(lnd_path, category, sheet='IDEB_MUNICIPIO_AF')
df_municipio_af.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_municipio_em = __execute_transformation(lnd_path, category, sheet='IDEB_MUNICIPIO_EM')
df_municipio_em.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_escola_ai = __execute_transformation(lnd_path, category, sheet='IDEB_ESCOLA_AI')
df_escola_ai.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_escola_af = __execute_transformation(lnd_path, category, sheet='IDEB_ESCOLA_AF')
df_escola_af.write.parquet(adl_sink, mode='append')

# COMMAND ----------

lnd_path, category = next(iterator)
df_escola_em = __execute_transformation(lnd_path, category, sheet='IDEB_ESCOLA_EM')
df_escola_em.write.parquet(adl_sink, mode='append')