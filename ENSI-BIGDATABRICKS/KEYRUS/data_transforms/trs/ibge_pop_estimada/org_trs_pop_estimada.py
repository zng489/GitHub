# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from unicodedata import normalize
import datetime 

import crawler.functions as cf
import json
import pyspark.sql.functions as f
from pyspark.sql.functions import substring, length, col, expr
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

headers = {'name_header':'Campo Origem', 'pos_header':'C', 'pos_org':'C', 'pos_dst':'E', 'pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

def __transform_cod_uf():
  return df.withColumn('CD_UF', f.substring('CD_MUNICIPIO', 1, 2))

def __transform_nome_uf():
  return df.withColumn('NM_UF', f.substring('NM_MUNICIPIO', -2, 2))

def __transform_nome_municipio():
  return df.withColumn("NM_MUNICIPIO", expr("substring(NM_MUNICIPIO, 1, length(NM_MUNICIPIO)-5)"))

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

sheet_name='2021'
df = df.select(*__transform_columns())
df = __transform_cod_uf()
df = __transform_nome_uf()
df = __transform_nome_municipio()

# COMMAND ----------

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# COMMAND ----------

df.write.parquet(path=adl_trs, mode='overwrite')
