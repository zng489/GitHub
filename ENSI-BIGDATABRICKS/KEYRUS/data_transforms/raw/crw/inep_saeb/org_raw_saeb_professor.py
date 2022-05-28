# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

import json
import re
import os
import crawler.functions as cf
import pyspark.sql.functions as f

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'schema': 'inep_saeb',
#   'table': 'professor',
#   'path_destination': 'crw/inep_saeb/saeb_professor_unificada',
#   'prm_path': '/prm/usr/inep_saeb/KC2332_SAEB_PROFESSOR_mapeamento_unificado_raw_V1.xlsx',
#   'partition': 'ID_PROVA_BRASIL'
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_saeb_escola",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-24T00:00:00.0000000Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders": {"landing": "/lnd", "error": "/err", "staging": "/stg", "log": "/log", "raw": "/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

def transform_df(df, parse_ba, sheet):
  cf.check_ba_doc(df, parse_ba=parse_ba, sheet=sheet)

  for org, dst, _type in parse_ba[sheet]:
    df = fill_column(df, org, dst, _type)

  for col_with_tmp_name in [col for col in df.columns if '_tmp' in col]:
    dst = col_with_tmp_name.split('_tmp')[0]
    df = df.withColumnRenamed(col_with_tmp_name, dst)
  
  return df

# COMMAND ----------

def fill_column(df, org, dst, _type):
  if org != dst and dst in df.columns:
    dst = dst + '_tmp'
  
  if org != dst and org != 'N/A':
    df = df.withColumnRenamed(org, dst)
  
  _type = _type.lower()
  if org == 'N/A':
    df = df.withColumn(dst, f.lit(None).cast(_type))
  else:
    df = df.withColumn(dst, f.col(dst).cast(_type))
  
  return df

# COMMAND ----------

parse_ba = cf.parse_ba_doc(dbutils, table['prm_path'], {'name_header': 'Tabela Origem', 'pos_header': 'B', 'pos_org': 'C', 'pos_dst': 'E', 'pos_type': 'F'})

# COMMAND ----------

adl_base_path = '{lnd}/crw/{schema}__{table}/'.format(lnd=dls['folders']['landing'], schema=table['schema'], table=table['table'])
years = sorted(map(lambda path: path.split('/')[-1], cf.list_subdirectory(dbutils, adl_base_path)), key=int)

# COMMAND ----------

for year in years:
  lnd_path = '{adl_path}{lnd}/crw/{schema}__{table}/{year}'.format(adl_path=var_adls_uri, lnd=lnd, schema=table['schema'], table=table['table'], year=year)
  sheet = next((sheet for sheet in parse_ba if year in sheet))
  sep = ';' if year == '2011' else ','
  
  df = spark.read.csv(lnd_path, header=True, sep=sep)
  df = transform_df(df, parse_ba, sheet)
  df = cf.append_control_columns(df, adf["adf_trigger_time"].split(".")[0])
  
  adl_lnd_path = '{base_dir}/{year}'.format(base_dir=adl_base_path, year=year)
  df_files = cf.list_adl_files(spark, dbutils, adl_lnd_path)
  df = df.join(f.broadcast(df_files), on='nm_arq_in', how='inner')

  uri = '{adl_path}{raw}/{dst}'.format(adl_path=var_adls_uri, raw=raw, dst=table['path_destination'])
  df.coalesce(1).write.partitionBy(table['partition']).parquet(uri, mode='overwrite')