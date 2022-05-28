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
from pyspark.sql.types import IntegerType
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

raw_novo_caged_mov_path = "{raw}/crw/{schema}/{table}_mov".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw_novo_caged_mov = "{adl_path}{raw_novo_caged_mov_path}".format(adl_path=var_adls_uri, raw_novo_caged_mov_path=raw_novo_caged_mov_path)

raw_novo_caged_for_path = "{raw}/crw/{schema}/{table}_for".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw_novo_caged_for = "{adl_path}{raw_novo_caged_for_path}".format(adl_path=var_adls_uri, raw_novo_caged_for_path=raw_novo_caged_for_path)

raw_novo_caged_exc_path = "{raw}/crw/{schema}/{table}_exc".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw_novo_caged_exc = "{adl_path}{raw_novo_caged_exc_path}".format(adl_path=var_adls_uri, raw_novo_caged_exc_path=raw_novo_caged_exc_path)



# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)



# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


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

origem = 'CAGEDMOV'
df_novo_caged_mov = spark.read.parquet(adl_raw_novo_caged_mov)
sheet_name='NOVO_CAGED_MOV'
cf.check_ba_doc(df_novo_caged_mov, parse_ba=var_prm_dict, sheet=sheet_name)

df_novo_caged_mov = df_novo_caged_mov.select(*__transform_columns())

df_novo_caged_mov = df_novo_caged_mov.withColumn('ORIGEM', f.lit(origem).cast('string'))



# COMMAND ----------

origem = 'CAGEDFOR'

df_novo_caged_for = spark.read.parquet(adl_raw_novo_caged_for)
sheet_name='NOVO_CAGED_FOR'
cf.check_ba_doc(df_novo_caged_for, parse_ba=var_prm_dict, sheet=sheet_name)

df_novo_caged_for = df_novo_caged_for.select(*__transform_columns())

df_novo_caged_for = df_novo_caged_for.withColumn('ORIGEM', f.lit(origem).cast('string'))



# COMMAND ----------

origem = 'CAGEDEXC'  

df_novo_caged_exc = spark.read.parquet(adl_raw_novo_caged_exc)
sheet_name='NOVO_CAGED_EXC'
cf.check_ba_doc(df_novo_caged_exc, parse_ba=var_prm_dict, sheet=sheet_name)

df_novo_caged_exc = df_novo_caged_exc.select(*__transform_columns())

df_novo_caged_exc = df_novo_caged_exc.withColumn('ORIGEM', f.lit(origem).cast('string'))

# COMMAND ----------

df = df_novo_caged_mov.union(df_novo_caged_for)
df = df.union(df_novo_caged_exc)


# COMMAND ----------

df = df.withColumn('CD_ANO_COMPETENCIA', f.substring(f.col('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO'), 0, 4).cast(IntegerType()))

# COMMAND ----------

(df
 .write
 .partitionBy('CD_ANO_COMPETENCIA')
 .parquet(path=adl_trs, mode='overwrite'))

# COMMAND ----------


