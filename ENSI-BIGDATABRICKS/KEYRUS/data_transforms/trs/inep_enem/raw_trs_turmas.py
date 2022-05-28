# Databricks notebook source
from pyspark.sql.window import Window

import cni_connectors.adls_gen1_connector as connector
import pyspark.sql.functions as f

import json
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'path_origin': 'crw/inep_censo_escolar/turmas/',
#   'path_destination': 'inep_enem/turmas'
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_elegiveis_16_18',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

# COMMAND ----------

df = df.where(
  (f.col('NU_ANO_CENSO') > f.lit(2015)) &
  (f.col('QT_MATRICULAS') > f.lit(0)) &
  (f.col('IN_REGULAR') == f.lit(1)) &
  ((f.col('TP_ETAPA_ENSINO') == f.lit(27)) | 
   (f.col('TP_ETAPA_ENSINO') == f.lit(28)) |
   (f.col('TP_ETAPA_ENSINO') == f.lit(32)) |
   (f.col('TP_ETAPA_ENSINO') == f.lit(33)) |
   (f.col('TP_ETAPA_ENSINO') == f.lit(37)) |
   (f.col('TP_ETAPA_ENSINO') == f.lit(38)))
  )

# COMMAND ----------

df = df.withColumn('mat_3ano', f.when(f.col('TP_ETAPA_ENSINO').isin([27, 32, 37]), f.col('qt_matriculas')).otherwise(f.lit(0)))
df = df.withColumn('mat_4ano', f.when(f.col('TP_ETAPA_ENSINO').isin([28, 33, 38]), f.col('qt_matriculas')).otherwise(f.lit(0)))

# COMMAND ----------

df_agg = (df
          .groupby('NU_ANO_CENSO', 'CO_ENTIDADE')
          .agg(f.sum('mat_3ano').alias('mat_3ano'), f.sum('mat_4ano').alias('mat_4ano')))

df_agg = df_agg.withColumn('COM_3E4', f.when((f.col('mat_3ano') > 0) & (f.col('mat_4ano') > 0), 1).otherwise(0))
df_agg = df_agg.withColumn('MATRIC_CONC', f.when(f.col('mat_4ano') > 0, f.col('mat_4ano')).otherwise(f.col('mat_3ano')))

# COMMAND ----------

df_agg = df_agg.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df_agg.coalesce(1).write.parquet(target, mode='overwrite')