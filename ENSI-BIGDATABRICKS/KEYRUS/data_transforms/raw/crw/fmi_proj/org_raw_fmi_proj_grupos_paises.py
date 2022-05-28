# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize
from datetime import date
  
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

lnd = dls['folders']['landing']
raw = dls['folders']['raw']
prm = dls['folders']['prm']

# COMMAND ----------

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
prm_path = "{prm}{prm_path}".format(prm=prm, prm_path=table["prm_path"])


# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}


# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

def find_set_name_org(find_names, set_name):
  index = 0
  for i, v in enumerate(var_prm_dict['FMI_GRUPO_PAISES']):
    if v[0] in find_names:
      index = i
 
  var_prm_dict['FMI_GRUPO_PAISES'][index][0] = set_name[0] if set_name[0] in df.columns else set_name[1]
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

from functools import reduce, partial
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType, StringType
import os

sheet_name = 'FMI_GRUPO_PAISES'
  
for lnd_year_path in sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: int(p.split('/')[-1])):
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)
  
  year = lnd_year_path.split('/')[-1]
  files_folder = cf.list_subdirectory(dbutils, lnd_year_path)
  
  _df = None
  for i, file in enumerate(files_folder):
    path_origin = '{adl_path}/{file_path}'.format(adl_path=var_adls_uri, file_path=file)

    mes = os.path.basename(file)[:3]

    df = spark.read.parquet(path_origin)
    
    var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)
    
    find_set_name_org(['Country/Series-specific Notes', 'Series-specific Notes'], ['Country/Series-specific Notes', 'Series-specific Notes'])
    find_set_name_org(['Estimates Start After'], ['Estimates Start After', 'N/A'])

    filter_w = [cl for cl in df.columns if not cl.isnumeric()]
    filter_n = sorted([cl for cl in df.columns if cl.isnumeric()])
    
    df = df.select(filter_w + filter_n)
    
    cl_dict = 'dict_ano_valor'
    df = df.withColumn(cl_dict, f.to_json(f.struct(filter_n)))
    df = df.withColumn(cl_dict,f.from_json(f.col(cl_dict), MapType(StringType(), StringType())))

    list_columns = [f.col(cl) for cl in filter_w] + [f.explode(df.dict_ano_valor)]
    df = df.select(list_columns)

    df = df.withColumnRenamed('key','YEAR_PROJ').withColumnRenamed('value','VALUE_PROJ')
    
    df = df.withColumn('ANO_PUBLICACAO', f.lit(year).cast('string'))
    df = df.withColumn('MES_PUBLICACAO', f.lit(mes).cast('string'))

    cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet_name)
    
    df = df.select(*__transform_columns())

    df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
    df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

    (df
     .write
     .partitionBy('ANO_PUBLICACAO', 'MES_PUBLICACAO')
     .parquet(path=adl_sink, mode='overwrite'))
  

# COMMAND ----------


