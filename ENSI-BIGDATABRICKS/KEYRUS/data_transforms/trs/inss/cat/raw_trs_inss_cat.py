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

sheet_name = 'CAT'
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

df = df.withColumn('DATA_ACIDENTE__', f.to_date(f.col('DATA_ACIDENTE__'), 'dd/MM/yyyy'))

df = df.withColumn('DATA_NASCIMENTO', f.to_date(f.col('DATA_NASCIMENTO'), 'dd/MM/yyyy'))

df = df.withColumn('DATA_EMISSAO_CAT', f.to_date(f.col('DATA_EMISSAO_CAT'), 'dd/MM/yyyy'))


# COMMAND ----------

df = df.select(*__transform_columns())


# COMMAND ----------

df = df.withColumn('CD_CBO', f.when((f .col('DT_COMP_ACIDENTE')>='2021/01') & (~f.col('DS_CBO').contains('{ñ class}')), f.substring(f.col('DS_CBO'), 0, 6)).\
                   otherwise(f.col('CD_CBO')))

df = df.withColumn('CD_CID_10', f.when((f.col('DT_COMP_ACIDENTE')>='2021/01') & ((~f.col('DS_CID_10').contains('{ñ class}')) & (~f.col('DS_CID_10').contains('Em Branco'))), f.substring(f.col('DS_CID_10'), 0, 5)).\
                   otherwise(f.col('CD_CID_10')))

df = df.withColumn('CD_CID_10', f.translate(f.col('CD_CID_10'), '.' ,''))

df = df.withColumn('CD_MUNIC_EMPR', f.substring(f.col('DS_MUNIC_EMPR'), 0 ,6))


df = df.withColumn('CD_CNPJ_CEI_EMPREGADOR', f.translate(f.col('CD_CNPJ_CEI_EMPREGADOR'), '-./' ,''))



# COMMAND ----------

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO', 'NM_TRIMESTRE') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


