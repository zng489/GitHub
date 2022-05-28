# Databricks notebook source
import datetime

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

sheet_name = 'GERA_ENERGIA_DISTRIB'
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

df = df.withColumn('DATAGERACAOCONJUNTO', f.to_timestamp(f.col('DATAGERACAOCONJUNTO'), 'yyyy-MM-dd HH:mm:ss'))


df = df.withColumn('DTHCONEXAO', f.to_date(f.col('DTHCONEXAO'), 'yyyy-MM-dd'))

df = df.withColumn('CNPJ_DISTRIBUIDORA', df.CNPJ_DISTRIBUIDORA.cast('long'))
df = df.withColumn('CODCLASSECONSUMO', df.CODCLASSECONSUMO.cast('int'))
df = df.withColumn('CODIGOSUBGRUPOTARIFARIO', df.CODIGOSUBGRUPOTARIFARIO.cast('int'))
df = df.withColumn('CODUFIBGE', df.CODUFIBGE.cast('int'))
df = df.withColumn('CODREGIAO', df.CODREGIAO.cast('int'))
df = df.withColumn('CODMUNICIPIOIBGE', df.CODMUNICIPIOIBGE.cast('int'))


# COMMAND ----------

df = df.select(*__transform_columns())

df = df.withColumn('CD_CNPJ_DISTRIBUIDORA', f.when(f.length(f.col('CD_CNPJ_DISTRIBUIDORA'))<14, f.lpad('CD_CNPJ_DISTRIBUIDORA', 14, "0")).otherwise(f.col('CD_CNPJ_DISTRIBUIDORA')))


# COMMAND ----------

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write.parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


