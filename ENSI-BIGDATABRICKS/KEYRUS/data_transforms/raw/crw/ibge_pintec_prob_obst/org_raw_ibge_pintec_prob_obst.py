# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from unicodedata import normalize

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

# COMMAND ----------

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .upper())

# COMMAND ----------

table["prm_path"]

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

for sub_plan in var_prm_dict.values():
  for row in sub_plan:
    row[0] = __normalize_str(row[0])

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[year]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

_df = None
for lnd_year_path in cf.list_subdirectory(dbutils, lnd_path):
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)

  files_folder = cf.list_subdirectory(dbutils, lnd_year_path)
  for i, file in enumerate(files_folder):
    path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)
    year = lnd_year_path.split('/')[-1]
    df = spark.read.parquet(path_origin)
    cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=year)

    df = df.select(*__transform_columns())
    df = df.withColumn("NR_ANO", f.lit(year))
    
    _df_n_columns = len(_df.columns) if _df else len(df.columns)

    if len(df.columns) != _df_n_columns:
      biggest_df = df if len(df.columns) > len(_df.columns) else _df
      smallest_df = df if len(df.columns) > len(_df.columns) else _df      
    else:
      biggest_df = _df
      smallest_df = df
      
    if _df:
      _df = biggest_df.unionByName(smallest_df)      
    else:
      _df = df
      
  if _df:
    df = _df
  df = df.withColumn("NM_UNIDADE_GEO", f.trim("NM_UNIDADE_GEO"))
  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')
  
  (df
   .write
   .partitionBy('NR_ANO')
   .parquet(path=adl_sink, mode='overwrite'))

# COMMAND ----------

display(df)
