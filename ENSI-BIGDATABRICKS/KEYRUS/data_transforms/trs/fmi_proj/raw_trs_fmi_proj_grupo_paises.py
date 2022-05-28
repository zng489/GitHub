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
prm = dls['folders']['prm']


# COMMAND ----------

prm_path = "{prm}{prm_path}".format(prm=prm, prm_path=table["prm_path"])


# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)




# COMMAND ----------

aux_table = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["aux_table"])


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)




# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)


# COMMAND ----------

sheet_name='FMI_GRUPO_PAISES'


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

df_aux = spark.read.parquet(aux_table)

# COMMAND ----------

df_aux = df_aux.select(['WEO Subject Code', 'CD_BR', 'DS_BR'])

# COMMAND ----------

df = df.join(df_aux, f.upper(df['WEO Subject Code'])==f.upper(df_aux['WEO Subject Code']), how='left').drop(df_aux['WEO Subject Code'])



# COMMAND ----------

df = df.withColumn('VALUE_PROJ', f.regexp_replace('VALUE_PROJ', ',', ''))

df = df.select(*__transform_columns())


# COMMAND ----------

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# COMMAND ----------

df.write \
  .partitionBy('NR_ANO_PUBLICACAO', 'NR_MES_PUBLICACAO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


