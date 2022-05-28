# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Trusted specific parameter section

# COMMAND ----------

import re
import json
import datetime

import crawler.functions as cf
import pyspark.sql.functions as f

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

raw_path = f'{raw}/crw/{table["schema"]}/{table["table"]}'
trs_path = f'{trs}/{table["schema"]}/{table["table"]}'

adl_path = var_adls_uri
adl_raw = f"{adl_path}{raw_path}"
adl_trs = f"{adl_path}{trs_path}"

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns(sheet_name):
    for org, dst, _type in var_prm_dict[sheet_name]:
        if org == 'N/A':
            yield f.lit(None).cast(_type).alias(dst)
        else:
            yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

df = spark.read.parquet(adl_raw)

sheet_name = table["table"]
cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet_name)
df = df.select(*__transform_columns(sheet_name))

# COMMAND ----------

data = [
    {'NM_INDICADOR':'Real GDP Forecast', 'CD_INDICADOR':'REALGDPFORECAST'},
    {'NM_INDICADOR':'Inflation forecast', 'CD_INDICADOR':'CPIFORECAST'},
    {'NM_INDICADOR':'Investiment forecast', 'CD_INDICADOR':'GFCFFORECAST'},
    {'NM_INDICADOR':'Current acount balance forecast', 'CD_INDICADOR':'BOPFORECAST'},
    {'NM_INDICADOR':'Unemployment rate forecast', 'CD_INDICADOR':'UNEMPFORECAST'},
    {'NM_INDICADOR':'Trade in goods and services forecast', 'CD_INDICADOR':'TRADEGOODSERVFORECAST'}
]

_df = spark.createDataFrame(data)
df = df.join(_df, on='CD_INDICADOR', how='left').drop(df.NM_INDICADOR)

columns = [name[1] for name in var_prm_dict[sheet_name]]
df = df.select(columns)

# COMMAND ----------

df = df.withColumn('NR_PERIODO', f.regexp_replace('NR_PERIODO', '-Q', '.'))
df = df.withColumn('CD_FREQUENCIA', f.regexp_replace('CD_FREQUENCIA', 'Q', 'T'))

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

df.write.mode('overwrite').partitionBy('CD_INDICADOR').parquet(path=adl_trs)
