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
#  'schema': 'inep_tdi',
#  'table': 'brasil_regioes_e_ufs',
#  'prm_path': '/tmp/dev/prm/usr/inep_tdi/FIEC_inep_tdi_brasil_regioes_uf_mapeamento_unificado_trusted.xlsx'
# }

# adf = {
#  "adf_factory_name": "cnibigdatafactory",
#  "adf_pipeline_name": "org_trs_taxa_rendimento",
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

adl_raw


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)

adl_trs


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


# COMMAND ----------

sheet_name='Regiões_UF'
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

df = df.withColumn('UNIDGEO', 
    f.when((f.col('UNIDGEO').isNull()) & (f.col('nm_arq_in').contains('BRASIL')), 'Brasil').otherwise(f.col('UNIDGEO')))


# COMMAND ----------

df = df.select(*__transform_columns())


# COMMAND ----------

NO_UNIDADE_GEO = [
        {'NO_UNIDADE_GEO': 'Rondônia', 'CD_UF': 11},
        {'NO_UNIDADE_GEO': 'RO', 'CD_UF': 11},
        {'NO_UNIDADE_GEO': 'Acre', 'CD_UF': 12},
        {'NO_UNIDADE_GEO': 'AC', 'CD_UF': 12},
        {'NO_UNIDADE_GEO': 'Amazonas', 'CD_UF': 13},
        {'NO_UNIDADE_GEO': 'AM', 'CD_UF': 13},
        {'NO_UNIDADE_GEO': 'Roraima', 'CD_UF': 14},
        {'NO_UNIDADE_GEO': 'RR', 'CD_UF': 14},
        {'NO_UNIDADE_GEO': 'Pará', 'CD_UF': 15},
        {'NO_UNIDADE_GEO': 'PA', 'CD_UF': 15},
        {'NO_UNIDADE_GEO': 'Amapá', 'CD_UF': 16},
        {'NO_UNIDADE_GEO': 'AP', 'CD_UF': 16},
        {'NO_UNIDADE_GEO': 'Tocantins', 'CD_UF': 17},
        {'NO_UNIDADE_GEO': 'TO', 'CD_UF': 17},
        {'NO_UNIDADE_GEO': 'Maranhão', 'CD_UF': 21},
        {'NO_UNIDADE_GEO': 'MA', 'CD_UF': 21},
        {'NO_UNIDADE_GEO': 'Piauí', 'CD_UF': 22},
        {'NO_UNIDADE_GEO': 'PI', 'CD_UF': 22},
        {'NO_UNIDADE_GEO': 'Ceará', 'CD_UF': 23},
        {'NO_UNIDADE_GEO': 'CE', 'CD_UF': 23},
        {'NO_UNIDADE_GEO': 'Rio Grande do Norte', 'CD_UF': 24},
        {'NO_UNIDADE_GEO': 'RN', 'CD_UF': 24},
        {'NO_UNIDADE_GEO': 'Paraíba', 'CD_UF': 25},
        {'NO_UNIDADE_GEO': 'PB', 'CD_UF': 25},
        {'NO_UNIDADE_GEO': 'Pernambuco', 'CD_UF': 26},
        {'NO_UNIDADE_GEO': 'PE', 'CD_UF': 26},
        {'NO_UNIDADE_GEO': 'Alagoas', 'CD_UF': 27},
        {'NO_UNIDADE_GEO': 'AL', 'CD_UF': 27},
        {'NO_UNIDADE_GEO': 'Sergipe', 'CD_UF': 28},
        {'NO_UNIDADE_GEO': 'SE', 'CD_UF': 28},
        {'NO_UNIDADE_GEO': 'Bahia', 'CD_UF': 29},
        {'NO_UNIDADE_GEO': 'BA', 'CD_UF': 29},
        {'NO_UNIDADE_GEO': 'Minas Gerais', 'CD_UF': 31},
        {'NO_UNIDADE_GEO': 'MG', 'CD_UF': 31},
        {'NO_UNIDADE_GEO': 'Espírito Santo', 'CD_UF': 32},
        {'NO_UNIDADE_GEO': 'ES', 'CD_UF': 32},
        {'NO_UNIDADE_GEO': 'Rio de Janeiro', 'CD_UF': 33},
        {'NO_UNIDADE_GEO': 'RJ', 'CD_UF': 33},
        {'NO_UNIDADE_GEO': 'São Paulo', 'CD_UF': 35},
        {'NO_UNIDADE_GEO': 'SP', 'CD_UF': 35},
        {'NO_UNIDADE_GEO': 'Paraná', 'CD_UF': 41},
        {'NO_UNIDADE_GEO': 'PR', 'CD_UF': 41},
        {'NO_UNIDADE_GEO': 'Santa Catarina', 'CD_UF': 42},
        {'NO_UNIDADE_GEO': 'SC', 'CD_UF': 42},
        {'NO_UNIDADE_GEO': 'Rio Grande do Sul', 'CD_UF': 43},
        {'NO_UNIDADE_GEO': 'RS', 'CD_UF': 43},
        {'NO_UNIDADE_GEO': 'Mato Grosso do Sul', 'CD_UF': 50},
        {'NO_UNIDADE_GEO': 'MS', 'CD_UF': 50},
        {'NO_UNIDADE_GEO': 'Mato Grosso', 'CD_UF': 51},
        {'NO_UNIDADE_GEO': 'MT', 'CD_UF': 51},
        {'NO_UNIDADE_GEO': 'Goiás', 'CD_UF': 52},
        {'NO_UNIDADE_GEO': 'GO', 'CD_UF': 52},
        {'NO_UNIDADE_GEO': 'Distrito Federal', 'CD_UF': 53},
        {'NO_UNIDADE_GEO': 'DF', 'CD_UF': 53}
]


# COMMAND ----------

df_unidade_geo = spark.createDataFrame(NO_UNIDADE_GEO)


# COMMAND ----------

SG_UF = [
        {'NO_UNIDADE_GEO': 'Rondônia', 'SG_UF': 'RO'},
        {'NO_UNIDADE_GEO': 'RO', 'SG_UF': 'RO'},
        {'NO_UNIDADE_GEO': 'Acre', 'SG_UF': 'AC'},
        {'NO_UNIDADE_GEO': 'AC', 'SG_UF': 'AC'},
        {'NO_UNIDADE_GEO': 'Amazonas', 'SG_UF': 'AM'},
        {'NO_UNIDADE_GEO': 'AM', 'SG_UF': 'AM'},
        {'NO_UNIDADE_GEO': 'Roraima', 'SG_UF': 'RR'},
        {'NO_UNIDADE_GEO': 'RR', 'SG_UF': 'RR'},
        {'NO_UNIDADE_GEO': 'Pará', 'SG_UF': 'PA'},
        {'NO_UNIDADE_GEO': 'PA', 'SG_UF': 'PA'},
        {'NO_UNIDADE_GEO': 'Amapá', 'SG_UF': 'AP'},
        {'NO_UNIDADE_GEO': 'AP', 'SG_UF': 'AP'},
        {'NO_UNIDADE_GEO': 'Tocantins', 'SG_UF': 'TO'},
        {'NO_UNIDADE_GEO': 'TO', 'SG_UF': 'TO'},
        {'NO_UNIDADE_GEO': 'Maranhão', 'SG_UF': 'MA'},
        {'NO_UNIDADE_GEO': 'MA', 'SG_UF': 'MA'},
        {'NO_UNIDADE_GEO': 'Piauí', 'SG_UF': 'PI'},
        {'NO_UNIDADE_GEO': 'PI', 'SG_UF': 'PI'},
        {'NO_UNIDADE_GEO': 'Ceará', 'SG_UF': 'CE'},
        {'NO_UNIDADE_GEO': 'CE', 'SG_UF': 'CE'},
        {'NO_UNIDADE_GEO': 'Rio Grande do Norte', 'SG_UF': 'RN'},
        {'NO_UNIDADE_GEO': 'RN', 'SG_UF': 'RN'},
        {'NO_UNIDADE_GEO': 'Paraíba', 'SG_UF': 'PB'},
        {'NO_UNIDADE_GEO': 'PB', 'SG_UF': 'PB'},
        {'NO_UNIDADE_GEO': 'Pernambuco', 'SG_UF': 'PE'},
        {'NO_UNIDADE_GEO': 'PE', 'SG_UF': 'PE'},
        {'NO_UNIDADE_GEO': 'Alagoas', 'SG_UF': 'AL'},
        {'NO_UNIDADE_GEO': 'AL', 'SG_UF': 'AL'},
        {'NO_UNIDADE_GEO': 'Sergipe', 'SG_UF': 'SE'},
        {'NO_UNIDADE_GEO': 'SE', 'SG_UF': 'SE'},
        {'NO_UNIDADE_GEO': 'Bahia', 'SG_UF': 'BA'},
        {'NO_UNIDADE_GEO': 'BA', 'SG_UF': 'BA'},
        {'NO_UNIDADE_GEO': 'Minas Gerais', 'SG_UF': 'MG'},
        {'NO_UNIDADE_GEO': 'MG', 'SG_UF': 'MG'},
        {'NO_UNIDADE_GEO': 'Espírito Santo', 'SG_UF': 'ES'},
        {'NO_UNIDADE_GEO': 'ES', 'SG_UF': 'ES'},
        {'NO_UNIDADE_GEO': 'Rio de Janeiro', 'SG_UF': 'RJ'},
        {'NO_UNIDADE_GEO': 'RJ', 'SG_UF': 'RJ'},
        {'NO_UNIDADE_GEO': 'São Paulo', 'SG_UF': 'SP'},
        {'NO_UNIDADE_GEO': 'SP', 'SG_UF': 'SP'},
        {'NO_UNIDADE_GEO': 'Paraná', 'SG_UF': 'PR'},
        {'NO_UNIDADE_GEO': 'PR', 'SG_UF': 'PR'},
        {'NO_UNIDADE_GEO': 'Santa Catarina', 'SG_UF': 'SC'},
        {'NO_UNIDADE_GEO': 'SC', 'SG_UF': 'SC'},
        {'NO_UNIDADE_GEO': 'Rio Grande do Sul', 'SG_UF': 'RS'},
        {'NO_UNIDADE_GEO': 'RS', 'SG_UF': 'RS'},
        {'NO_UNIDADE_GEO': 'Mato Grosso do Sul', 'SG_UF': 'MS'},
        {'NO_UNIDADE_GEO': 'MS', 'SG_UF': 'MS'},
        {'NO_UNIDADE_GEO': 'Mato Grosso', 'SG_UF': 'MT'},
        {'NO_UNIDADE_GEO': 'MT', 'SG_UF': 'MT'},
        {'NO_UNIDADE_GEO': 'Goiás', 'SG_UF': 'GO'},
        {'NO_UNIDADE_GEO': 'GO', 'SG_UF': 'GO'},
        {'NO_UNIDADE_GEO': 'Distrito Federal', 'SG_UF': 'DF'},
        {'NO_UNIDADE_GEO': 'DF', 'SG_UF': 'DF'}
]


# COMMAND ----------

df_sg_uf = spark.createDataFrame(SG_UF)

# COMMAND ----------

CD_REGIAO = [
    {'UNIDADE_GEO': 'Norte', 'CD_REGIAO': 1},
    {'UNIDADE_GEO': 'Nordeste', 'CD_REGIAO': 2},
    {'UNIDADE_GEO': 'Sudeste', 'CD_REGIAO': 3},
    {'UNIDADE_GEO': 'Sul', 'CD_REGIAO': 4},
    {'UNIDADE_GEO': 'Centro-Oeste', 'CD_REGIAO': 5},
    {'UNIDADE_GEO': 'Centro - Oeste', 'CD_REGIAO': 5},
    {'UNIDADE_GEO': 'Centro - Oeste', 'CD_REGIAO': 5},
    {'UNIDADE_GEO': 'Brasil', 'CD_REGIAO': 9}
]


# COMMAND ----------

df_regiao = spark.createDataFrame(CD_REGIAO)


# COMMAND ----------

NO_REGIAO = [
    {'UNIDADE_GEO': 'Norte', 'NO_REGIAO': 'Norte'},
    {'UNIDADE_GEO': 'Nordeste', 'NO_REGIAO': 'Nordeste'},
    {'UNIDADE_GEO': 'Sudeste', 'NO_REGIAO': 'Sudeste'},
    {'UNIDADE_GEO': 'Sul', 'NO_REGIAO': 'Sul'},
    {'UNIDADE_GEO': 'Centro-Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'UNIDADE_GEO': 'Centro - Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'UNIDADE_GEO': 'Centro - Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'UNIDADE_GEO': 'Brasil', 'NO_REGIAO': 'Brasil'}
]


# COMMAND ----------

df_nome_regiao = spark.createDataFrame(NO_REGIAO)


# COMMAND ----------

TP_LOCALIZACAO = [ 
  {'NO_LOCALIZACAO': 'Urbana',  'TP_LOCALIZACAO': 1},
  {'NO_LOCALIZACAO': 'Rural',  'TP_LOCALIZACAO': 2},
  {'NO_LOCALIZACAO': 'Total',  'TP_LOCALIZACAO': 9}
]


# COMMAND ----------

df_localizacao = spark.createDataFrame(TP_LOCALIZACAO)


# COMMAND ----------

TP_DEPENDENCIA = [
    {'NO_DEPENDENCIA': 'Federal',  'TP_DEPENDENCIA': 1},
    {'NO_DEPENDENCIA': 'Estadual',  'TP_DEPENDENCIA': 2},
    {'NO_DEPENDENCIA': 'Municipal',  'TP_DEPENDENCIA': 3},
    {'NO_DEPENDENCIA': 'Privada',  'TP_DEPENDENCIA': 4},
    {'NO_DEPENDENCIA': 'Particular',  'TP_DEPENDENCIA': 4},
    {'NO_DEPENDENCIA': 'Pública',  'TP_DEPENDENCIA': 5},
    {'NO_DEPENDENCIA': 'Publico',  'TP_DEPENDENCIA': 5},
    {'NO_DEPENDENCIA': 'Público',  'TP_DEPENDENCIA': 5},
    {'NO_DEPENDENCIA': 'Total',  'TP_DEPENDENCIA': 9}
]


# COMMAND ----------

df_tp_dependencia = spark.createDataFrame(TP_DEPENDENCIA)


# COMMAND ----------

NO_DEPENDENCIA = [
    {'NO_DEPENDENCIA_OLD': 'Federal',  'NO_DEPENDENCIA': 'Federal'},
    {'NO_DEPENDENCIA_OLD': 'Estadual',  'NO_DEPENDENCIA': 'Estadual'},
    {'NO_DEPENDENCIA_OLD': 'Municipal',  'NO_DEPENDENCIA': 'Municipal'},
    {'NO_DEPENDENCIA_OLD': 'Privada',  'NO_DEPENDENCIA': 'Privada'},
    {'NO_DEPENDENCIA_OLD': 'Particular',  'NO_DEPENDENCIA': 'Privada'},
    {'NO_DEPENDENCIA_OLD': 'Pública',  'NO_DEPENDENCIA': 'Pública'},
    {'NO_DEPENDENCIA_OLD': 'Publico',  'NO_DEPENDENCIA': 'Pública'},
    {'NO_DEPENDENCIA_OLD': 'Público',  'NO_DEPENDENCIA': 'Pública'},
    {'NO_DEPENDENCIA_OLD': 'Total',  'NO_DEPENDENCIA': 'Total'}
]


# COMMAND ----------

df_no_dependencia = spark.createDataFrame(NO_DEPENDENCIA)


# COMMAND ----------

df = df.withColumn("NO_UNIDADE_GEO", f.trim(df.NO_UNIDADE_GEO))
df = df.withColumn("NO_LOCALIZACAO", f.trim(df.NO_LOCALIZACAO))

df = df.join(df_unidade_geo, f.upper(df.NO_UNIDADE_GEO)==f.upper(df_unidade_geo.NO_UNIDADE_GEO), how='left') \
       .drop(df.CD_UF) \
       .drop(df_unidade_geo.NO_UNIDADE_GEO)

df = df.join(df_sg_uf, f.upper(df.NO_UNIDADE_GEO)==f.upper(df_sg_uf.NO_UNIDADE_GEO), how='left') \
       .drop(df.SG_UF) \
       .drop(df_sg_uf.NO_UNIDADE_GEO)

df = df.join(df_regiao, f.upper(df.NO_UNIDADE_GEO)==f.upper(df_regiao.UNIDADE_GEO), how='left') \
       .drop(df.CD_REGIAO) \
       .drop(df_regiao.UNIDADE_GEO)

df = df.join(df_nome_regiao, f.upper(df.NO_UNIDADE_GEO)==f.upper(df_nome_regiao.UNIDADE_GEO), how='left') \
       .drop(df.NO_REGIAO) \
       .drop(df_nome_regiao.UNIDADE_GEO)

df = df.join(df_localizacao, f.upper(df.NO_LOCALIZACAO)==f.upper(df_localizacao.NO_LOCALIZACAO), how='left') \
       .drop(df.TP_LOCALIZACAO) \
       .drop(df_localizacao.NO_LOCALIZACAO)

df = df.join(df_tp_dependencia, f.upper(df.NO_DEPENDENCIA)==f.upper(df_tp_dependencia.NO_DEPENDENCIA), how='left') \
       .drop(df.TP_DEPENDENCIA) \
       .drop(df_tp_dependencia.NO_DEPENDENCIA)

df = df.join(df_no_dependencia, f.upper(df.NO_DEPENDENCIA)==f.upper(df_no_dependencia.NO_DEPENDENCIA_OLD), how='left') \
       .drop(df.NO_DEPENDENCIA) \
       .drop(df_no_dependencia.NO_DEPENDENCIA_OLD)

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# display(df)


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO_CENSO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


