# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from unicodedata import normalize

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_source

# COMMAND ----------

var_sink = "{adl_path}{raw}/usr/{namespace}/{file_folder}/".format(adl_path=var_adls_uri, raw=raw, 
                                                                   namespace=var_file['namespace'], file_folder=var_file['file_folder'].split('/')[0])
var_sink

# COMMAND ----------

var_year = var_file['file_folder'].split('/')[-1]
var_year

# COMMAND ----------

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % var_source)

# COMMAND ----------

var_source_txt_files = '{source}/*.txt'.format(source=var_adls_uri + var_source)
var_source_txt_files

# COMMAND ----------

df = spark.read.csv(path=var_source_txt_files, sep=';', encoding=var_file['encoding'], mode='FAILFAST', 
                    header=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)

# COMMAND ----------

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())
  
for column in df.columns:
  df = df.withColumnRenamed(column, __normalize_str(column))

# COMMAND ----------

prm_path = '/prm/usr/me/KC2332_ME_RAIS_VINCULO_mapeamento_raw.xlsx'
headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}

parse_ba_doc = cf.parse_ba_doc(dbutils, prm_path, headers=headers, sheet_names=[var_year])
cf.check_ba_doc(df, parse_ba=parse_ba_doc, sheet=var_year)

# COMMAND ----------

def __select(parse_ba_doc, year):
  for org, dst, _type in parse_ba_doc[year]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower() == 'double':
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)
      
df = df.select(*__select(parse_ba_doc=parse_ba_doc, year=var_year))

# COMMAND ----------

df = df.withColumn('ANO', f.lit(var_year).cast('Int'))

# COMMAND ----------

dt_insertion_raw = var_adf["adf_trigger_time"].split(".")[0]
dt_insertion_raw

# COMMAND ----------

df = cf.append_control_columns(df, dt_insertion_raw)

# COMMAND ----------

adl_file_time = cf.list_adl_files(spark, dbutils, var_source)
df = df.join(adl_file_time, on='nm_arq_in', how='inner')

# COMMAND ----------

df.repartition(40).write.partitionBy('ANO').parquet(path=var_sink, mode='overwrite')
