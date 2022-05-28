# Databricks notebook source
import datetime

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

# COMMAND ----------

# table = {
#  'schema': 'inep_icg',
#  'table': 'escolas',
#  'prm_path': '/tmp/dev/prm/usr/inep_icg/FIEC_INEP_icg_escolas_mapeamento_unificado_trusted.xlsx'
# }

# adf = {
#  "adf_factory_name": "cnibigdatafactory",
#  "adf_pipeline_name": "org_trs_icg_escolas",
#  "adf_pipeline_run_id": "",
#  "adf_trigger_id": "",
#  "adf_trigger_name": "",
#  "adf_trigger_time": "teste",
#  "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders":{"error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/tmp/dev/raw", "trusted":"/tmp/dev/trs"}}


# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']


# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

# adl_raw


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)

# adl_trs


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


# COMMAND ----------

sheet_name='Escolas'
columns = [name[1] for name in var_prm_dict[sheet_name]]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)
 

# COMMAND ----------

df = spark.read.parquet(adl_raw)


# COMMAND ----------

df = df.select(*__transform_columns())


# COMMAND ----------

NM_UF = [
        {'NM_UF': 'Rondônia', 'CD_UF': 11},
        {'NM_UF': 'RO', 'CD_UF': 11},
        {'NM_UF': 'Acre', 'CD_UF': 12},
        {'NM_UF': 'AC', 'CD_UF': 12},
        {'NM_UF': 'Amazonas', 'CD_UF': 13},
        {'NM_UF': 'AM', 'CD_UF': 13},
        {'NM_UF': 'Roraima', 'CD_UF': 14},
        {'NM_UF': 'RR', 'CD_UF': 14},
        {'NM_UF': 'Pará', 'CD_UF': 15},
        {'NM_UF': 'PA', 'CD_UF': 15},
        {'NM_UF': 'Amapá', 'CD_UF': 16},
        {'NM_UF': 'AP', 'CD_UF': 16},
        {'NM_UF': 'Tocantins', 'CD_UF': 17},
        {'NM_UF': 'TO', 'CD_UF': 17},
        {'NM_UF': 'Maranhão', 'CD_UF': 21},
        {'NM_UF': 'MA', 'CD_UF': 21},
        {'NM_UF': 'Piauí', 'CD_UF': 22},
        {'NM_UF': 'PI', 'CD_UF': 22},
        {'NM_UF': 'Ceará', 'CD_UF': 23},
        {'NM_UF': 'CE', 'CD_UF': 23},
        {'NM_UF': 'Rio Grande do Norte', 'CD_UF': 24},
        {'NM_UF': 'RN', 'CD_UF': 24},
        {'NM_UF': 'Paraíba', 'CD_UF': 25},
        {'NM_UF': 'PB', 'CD_UF': 25},
        {'NM_UF': 'Pernambuco', 'CD_UF': 26},
        {'NM_UF': 'PE', 'CD_UF': 26},
        {'NM_UF': 'Alagoas', 'CD_UF': 27},
        {'NM_UF': 'AL', 'CD_UF': 27},
        {'NM_UF': 'Sergipe', 'CD_UF': 28},
        {'NM_UF': 'SE', 'CD_UF': 28},
        {'NM_UF': 'Bahia', 'CD_UF': 29},
        {'NM_UF': 'BA', 'CD_UF': 29},
        {'NM_UF': 'Minas Gerais', 'CD_UF': 31},
        {'NM_UF': 'MG', 'CD_UF': 31},
        {'NM_UF': 'Espírito Santo', 'CD_UF': 32},
        {'NM_UF': 'ES', 'CD_UF': 32},
        {'NM_UF': 'Rio de Janeiro', 'CD_UF': 33},
        {'NM_UF': 'RJ', 'CD_UF': 33},
        {'NM_UF': 'São Paulo', 'CD_UF': 35},
        {'NM_UF': 'SP', 'CD_UF': 35},
        {'NM_UF': 'Paraná', 'CD_UF': 41},
        {'NM_UF': 'PR', 'CD_UF': 41},
        {'NM_UF': 'Santa Catarina', 'CD_UF': 42},
        {'NM_UF': 'SC', 'CD_UF': 42},
        {'NM_UF': 'Rio Grande do Sul', 'CD_UF': 43},
        {'NM_UF': 'RS', 'CD_UF': 43},
        {'NM_UF': 'Mato Grosso do Sul', 'CD_UF': 50},
        {'NM_UF': 'MS', 'CD_UF': 50},
        {'NM_UF': 'Mato Grosso', 'CD_UF': 51},
        {'NM_UF': 'MT', 'CD_UF': 51},
        {'NM_UF': 'Goiás', 'CD_UF': 52},
        {'NM_UF': 'GO', 'CD_UF': 52},
        {'NM_UF': 'Distrito Federal', 'CD_UF': 53},
        {'NM_UF': 'DF', 'CD_UF': 53}
]

# COMMAND ----------

df_nm_uf = spark.createDataFrame(NM_UF)


# COMMAND ----------

NM_LOCALIZACAO = [
    {'NM_LOCALIZACAO': 'Urbana', 'CD_LOCALIZACAO': 1},
    {'NM_LOCALIZACAO': 'Rural', 'CD_LOCALIZACAO': 2}
]


# COMMAND ----------

df_nm_localizacao = spark.createDataFrame(NM_LOCALIZACAO)


# COMMAND ----------

NM_DEPENDENCIA = [
    {'NM_DEPENDENCIA': 'Federal', 'CD_DEPENDENCIA': 1},
    {'NM_DEPENDENCIA': 'Estadual', 'CD_DEPENDENCIA': 2},
    {'NM_DEPENDENCIA': 'Municipal', 'CD_DEPENDENCIA': 3},
    {'NM_DEPENDENCIA': 'Privada', 'CD_DEPENDENCIA': 4}
]

# COMMAND ----------

df_nm_dependencia = spark.createDataFrame(NM_DEPENDENCIA)

# COMMAND ----------

NM_ICG = [
    {'NM_ICG': 'Nível 1', 'CD_ICG': 1},
    {'NM_ICG': 'Nível 2', 'CD_ICG': 2},
    {'NM_ICG': 'Nível 3', 'CD_ICG': 3},
    {'NM_ICG': 'Nível 4', 'CD_ICG': 4},
    {'NM_ICG': 'Nível 5', 'CD_ICG': 5},
    {'NM_ICG': 'Nível 6', 'CD_ICG': 6}
]

# COMMAND ----------

df_nm_icg = spark.createDataFrame(NM_ICG)

# COMMAND ----------

df = df.withColumn("NM_UF", f.trim(df.NM_UF))

df = df.join(df_nm_uf, f.upper(df.NM_UF)==f.upper(df_nm_uf.NM_UF), how='left') \
       .drop(df.CD_UF) \
       .drop(df_nm_uf.NM_UF)

df = df.join(df_nm_localizacao, f.upper(df.NM_LOCALIZACAO)==f.upper(df_nm_localizacao.NM_LOCALIZACAO), how='left') \
       .drop(df.CD_LOCALIZACAO) \
       .drop(df_nm_localizacao.NM_LOCALIZACAO)

df = df.join(df_nm_dependencia, f.upper(df.NM_DEPENDENCIA)==f.upper(df_nm_dependencia.NM_DEPENDENCIA), how='left') \
       .drop(df.CD_DEPENDENCIA) \
       .drop(df_nm_dependencia.NM_DEPENDENCIA)

df = df.join(df_nm_icg, f.upper(df.NM_ICG)==f.upper(df_nm_icg.NM_ICG), how='left') \
       .drop(df.CD_ICG) \
       .drop(df_nm_icg.NM_ICG)


df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# display(df)


# COMMAND ----------

df.write \
  .partitionBy('DT_ANO_CENSO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------

