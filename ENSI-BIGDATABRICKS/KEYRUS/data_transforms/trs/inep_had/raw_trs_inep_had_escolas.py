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
#  'schema': 'inep_had',
#  'table': 'escolas',
#  'prm_path': '/tmp/dev/prm/usr/inep_had/FIEC_inep_had_escolas_mapeamento_unificado_trusted.xlsx'
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

SG_UF = [
        {'SG_UF': 'Rondônia', 'CD_UF': 11},
        {'SG_UF': 'RO', 'CD_UF': 11},
        {'SG_UF': 'Acre', 'CD_UF': 12},
        {'SG_UF': 'AC', 'CD_UF': 12},
        {'SG_UF': 'Amazonas', 'CD_UF': 13},
        {'SG_UF': 'AM', 'CD_UF': 13},
        {'SG_UF': 'Roraima', 'CD_UF': 14},
        {'SG_UF': 'RR', 'CD_UF': 14},
        {'SG_UF': 'Pará', 'CD_UF': 15},
        {'SG_UF': 'PA', 'CD_UF': 15},
        {'SG_UF': 'Amapá', 'CD_UF': 16},
        {'SG_UF': 'AP', 'CD_UF': 16},
        {'SG_UF': 'Tocantins', 'CD_UF': 17},
        {'SG_UF': 'TO', 'CD_UF': 17},
        {'SG_UF': 'Maranhão', 'CD_UF': 21},
        {'SG_UF': 'MA', 'CD_UF': 21},
        {'SG_UF': 'Piauí', 'CD_UF': 22},
        {'SG_UF': 'PI', 'CD_UF': 22},
        {'SG_UF': 'Ceará', 'CD_UF': 23},
        {'SG_UF': 'CE', 'CD_UF': 23},
        {'SG_UF': 'Rio Grande do Norte', 'CD_UF': 24},
        {'SG_UF': 'RN', 'CD_UF': 24},
        {'SG_UF': 'Paraíba', 'CD_UF': 25},
        {'SG_UF': 'PB', 'CD_UF': 25},
        {'SG_UF': 'Pernambuco', 'CD_UF': 26},
        {'SG_UF': 'PE', 'CD_UF': 26},
        {'SG_UF': 'Alagoas', 'CD_UF': 27},
        {'SG_UF': 'AL', 'CD_UF': 27},
        {'SG_UF': 'Sergipe', 'CD_UF': 28},
        {'SG_UF': 'SE', 'CD_UF': 28},
        {'SG_UF': 'Bahia', 'CD_UF': 29},
        {'SG_UF': 'BA', 'CD_UF': 29},
        {'SG_UF': 'Minas Gerais', 'CD_UF': 31},
        {'SG_UF': 'MG', 'CD_UF': 31},
        {'SG_UF': 'Espírito Santo', 'CD_UF': 32},
        {'SG_UF': 'ES', 'CD_UF': 32},
        {'SG_UF': 'Rio de Janeiro', 'CD_UF': 33},
        {'SG_UF': 'RJ', 'CD_UF': 33},
        {'SG_UF': 'São Paulo', 'CD_UF': 35},
        {'SG_UF': 'SP', 'CD_UF': 35},
        {'SG_UF': 'Paraná', 'CD_UF': 41},
        {'SG_UF': 'PR', 'CD_UF': 41},
        {'SG_UF': 'Santa Catarina', 'CD_UF': 42},
        {'SG_UF': 'SC', 'CD_UF': 42},
        {'SG_UF': 'Rio Grande do Sul', 'CD_UF': 43},
        {'SG_UF': 'RS', 'CD_UF': 43},
        {'SG_UF': 'Mato Grosso do Sul', 'CD_UF': 50},
        {'SG_UF': 'MS', 'CD_UF': 50},
        {'SG_UF': 'Mato Grosso', 'CD_UF': 51},
        {'SG_UF': 'MT', 'CD_UF': 51},
        {'SG_UF': 'Goiás', 'CD_UF': 52},
        {'SG_UF': 'GO', 'CD_UF': 52},
        {'SG_UF': 'Distrito Federal', 'CD_UF': 53},
        {'SG_UF': 'DF', 'CD_UF': 53}
]

# COMMAND ----------

df_sg_uf = spark.createDataFrame(SG_UF)

# COMMAND ----------

CD_REGIAO = [
    {'NO_REGIAO': 'Norte', 'CD_REGIAO': 1},
    {'NO_REGIAO': 'Nordeste', 'CD_REGIAO': 2},
    {'NO_REGIAO': 'Sudeste', 'CD_REGIAO': 3},
    {'NO_REGIAO': 'Sul', 'CD_REGIAO': 4},
    {'NO_REGIAO': 'Centro-Oeste', 'CD_REGIAO': 5},
    {'NO_REGIAO': 'Centro - Oeste', 'CD_REGIAO': 5},
    {'NO_REGIAO': 'Centro_Oeste', 'CD_REGIAO': 5},
    {'NO_REGIAO': 'Brasil', 'CD_REGIAO': 9}
]

# COMMAND ----------

df_regiao = spark.createDataFrame(CD_REGIAO)

# COMMAND ----------

NO_REGIAO = [
    {'NO_REGIAO_OLD': 'Norte', 'NO_REGIAO': 'Norte'},
    {'NO_REGIAO_OLD': 'Nordeste', 'NO_REGIAO': 'Nordeste'},
    {'NO_REGIAO_OLD': 'Sudeste', 'NO_REGIAO': 'Sudeste'},
    {'NO_REGIAO_OLD': 'Sul', 'NO_REGIAO': 'Sul'},
    {'NO_REGIAO_OLD': 'Centro-Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'NO_REGIAO_OLD': 'Centro - Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'NO_REGIAO_OLD': 'Centro_Oeste', 'NO_REGIAO': 'Centro-Oeste'},
    {'NO_REGIAO_OLD': 'Brasil', 'NO_REGIAO': 'Brasil'}
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

df = df.withColumn("NO_REGIAO", f.trim(df.NO_REGIAO))
df = df.withColumn("NO_LOCALIZACAO", f.trim(df.NO_LOCALIZACAO))

#CD_UF
df = df.join(df_sg_uf, f.upper(df.SG_UF)==f.upper(df_sg_uf.SG_UF), how='left') \
       .drop(df.CD_UF).drop(df_sg_uf.SG_UF)

#CD_REGIAO
df = df.join(df_regiao, f.upper(df.NO_REGIAO)==f.upper(df_regiao.NO_REGIAO), how='left') \
       .drop(df.CD_REGIAO) \
       .drop(df_regiao.NO_REGIAO)

#NO_REGIAO
df = df.join(df_nome_regiao, f.upper(df.NO_REGIAO)==f.upper(df_nome_regiao.NO_REGIAO_OLD), how='left') \
       .drop(df.NO_REGIAO) \
       .drop(df_nome_regiao.NO_REGIAO_OLD)

#TP_LOCALIZACAO
df = df.join(df_localizacao, f.upper(df.NO_LOCALIZACAO)==f.upper(df_localizacao.NO_LOCALIZACAO), how='left') \
       .drop(df.TP_LOCALIZACAO) \
       .drop(df_localizacao.NO_LOCALIZACAO)

#TP_DEPENDENCIA
df = df.join(df_tp_dependencia, f.upper(df.NO_DEPENDENCIA)==f.upper(df_tp_dependencia.NO_DEPENDENCIA), how='left') \
       .drop(df.TP_DEPENDENCIA) \
       .drop(df_tp_dependencia.NO_DEPENDENCIA)

#NO_DEPENDENCIA
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

