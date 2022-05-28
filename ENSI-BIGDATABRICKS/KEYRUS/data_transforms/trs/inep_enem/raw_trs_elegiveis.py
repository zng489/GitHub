# Databricks notebook source
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
#   'path_origin': 'crw/inep_enem/microdados_enem/',
#   'path_destination': 'inep_enem/elegiveis'
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
df = df.where((f.col('NU_ANO') > f.lit(2015)) &
              (f.col('TP_ST_CONCLUSAO') == f.lit(2)) &
              (f.col('CO_ENTIDADE') > f.lit(0)) &
              (f.col('IN_TREINEIRO') == f.lit(0)) &
              (f.col('TP_ENSINO') == f.lit(1)) &
              (f.col('TP_PRESENCA_CN') == f.lit(1)) &
              (f.col('TP_PRESENCA_CH') == f.lit(1)) &
              (f.col('TP_PRESENCA_LC') == f.lit(1)) &
              (f.col('TP_PRESENCA_MT') == f.lit(1)) &
              (f.col('NU_NOTA_CN') > f.lit(0)) &
              (f.col('NU_NOTA_CH') > f.lit(0)) &
              (f.col('NU_NOTA_LC') > f.lit(0)) &
              (f.col('NU_NOTA_MT') > f.lit(0)) &
              (f.col('TP_STATUS_REDACAO') == f.lit(1)) &
              (f.col('NU_NOTA_REDACAO') > f.lit(0)))

# COMMAND ----------

df_agg = (df
      .groupby('NU_ANO', 'CO_ENTIDADE')
      .agg(f.count(f.lit(1)).alias('VL_TOT_ALUNOS_ENEM_ESCOLA'),
           f.round(f.avg((f.col('NU_NOTA_CH') + f.col('NU_NOTA_CN') + f.col('NU_NOTA_LC') + f.col('NU_NOTA_MT')) / f.lit(4)), 5).alias('NU_NOTA_GERAL'),
           f.round(f.avg((f.col('NU_NOTA_CH') + f.col('NU_NOTA_CN') + f.col('NU_NOTA_LC') + f.col('NU_NOTA_MT') + f.col('NU_NOTA_REDACAO')) / f.lit(5)), 5).alias('NU_NOTA_GERAL_REDAC'),
           f.round(f.avg(f.col('NU_NOTA_CH')), 5).alias('VL_NOTA_CH'), 
           f.round(f.avg(f.col('NU_NOTA_CN')), 5).alias('VL_NOTA_CN'), 
           f.round(f.avg(f.col('NU_NOTA_LC')), 5).alias('VL_NOTA_LC'), 
           f.round(f.avg(f.col('NU_NOTA_MT')), 5).alias('VL_NOTA_MT'),
           f.round(f.avg(f.col('NU_NOTA_REDACAO')), 5).alias('VL_NOTA_REDACAO')
          )
     )

# COMMAND ----------

df_agg = df_agg.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df_agg.coalesce(1).write.parquet(target, mode='overwrite')