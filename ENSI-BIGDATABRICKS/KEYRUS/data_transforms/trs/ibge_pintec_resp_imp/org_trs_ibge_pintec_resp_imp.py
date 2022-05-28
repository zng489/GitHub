# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from unicodedata import normalize
import datetime 
import numpy as np
import crawler.functions as cf
import json
import pyspark.sql.functions as f
from pyspark.sql.functions import substring, length, col, expr
from pyspark.sql.types import IntegerType, StringType
import re

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])
adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])
adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

def __column_cd_regiao(nm_unidade_geo):
  dict_cd_regiao = {
    'Norte':1,
    'Nordeste':2,
    'Sudeste':3,
    'Sul':4,
    'Centro-Oeste':5,
    'Brasil':9
  }
  
  try:
    cd_regiao = dict_cd_regiao[nm_unidade_geo]
  except:
    cd_regiao = None
  
  return cd_regiao

def __column_cd_uf(nm_unidade_geo):
  dict_cd_uf = {
    'Amazonas':13,
    'Pará':15,
    'Ceará':23,
    'Pernambuco':26,
    'Bahia':29,
    'Minas Gerais':31,
    'Espírito Santo':32,
    'Rio de Janeiro':33,
    'São Paulo':35,
    'Paraná':41,
    'Santa Catarina':42,
    'Rio Grande do Sul':43,
    'Mato Grosso':51,
    'Goiás':52
  }
  
  try:
    cd_uf = dict_cd_uf[nm_unidade_geo]
  except:
    cd_uf = None
  
  return cd_uf

def __column_sg_uf(nm_unidade_geo):
  dict_sg_uf = {
    'Amazonas':'AM',
    'Pará':'PA',
    'Ceará':'CE',
    'Pernambuco':'PE',
    'Bahia':'BA',
    'Minas Gerais':'MG',
    'Espírito Santo':'ES',
    'Rio de Janeiro':'RJ',
    'São Paulo':'SP',
    'Paraná':'PR',
    'Santa Catarina':'SC',
    'Rio Grande do Sul':'RS',
    'Mato Grosso':'MT',
    'Goiás':'GO'
  }
  
  try:
    sg_uf = dict_sg_uf[nm_unidade_geo]
  except:
    sg_uf = None
  
  return sg_uf

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

sheet_name='2017'
df = df.select(*__transform_columns())

# COMMAND ----------

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# COMMAND ----------

udf_cd_regiao = f.udf(__column_cd_regiao)
df = df.withColumn("CD_REGIAO", udf_cd_regiao("NM_UNIDADE_GEO"))

# COMMAND ----------

udf_cd_uf = f.udf(__column_cd_uf)
df = df.withColumn("CD_UF", udf_cd_uf("NM_UNIDADE_GEO"))

# COMMAND ----------

udf_sg_uf = f.udf(__column_sg_uf)
df = df.withColumn("SG_UF", udf_sg_uf("NM_UNIDADE_GEO"))

# COMMAND ----------

df.write.partitionBy('NR_ANO').parquet(path=adl_trs, mode='overwrite')
