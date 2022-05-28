# Databricks notebook source
import cni_connectors.adls_gen1_connector as connector
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'path_origin_ajuste_mt': 'usr/inep_saeb/competencias_e_habilidades_ajuste_mt',
#   'path_origin_ajuste_lp': 'usr/inep_saeb/competencias_e_habilidades_ajuste_lp',
#   'path_destination': 'inep_saeb/competencias_habilidades_ajuste',
# }

# # dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_competencias_habilidades_ajuste',
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

source_lp = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin_ajuste_lp"])
source_lp

# COMMAND ----------

source_mt = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin_ajuste_mt"])
source_mt

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

df_mt = spark.read.parquet(source_mt).select('nivel_mt','nivel_mt_ajustado')

# COMMAND ----------

df_mt = df_mt.withColumnRenamed('nivel_mt','vl_nivel')
df_mt = df_mt.withColumnRenamed('nivel_mt_ajustado','vl_nivel_ajustado')

# COMMAND ----------

df_mt = df_mt.withColumn('sg_disciplina', f.lit('MT'))

# COMMAND ----------

df_lp = spark.read.parquet(source_lp).select('nivel_lp','nivel_lp_ajustado')

# COMMAND ----------

df_lp = df_lp.withColumnRenamed('nivel_lp','vl_nivel')
df_lp = df_lp.withColumnRenamed('nivel_lp_ajustado','vl_nivel_ajustado')

# COMMAND ----------

df_lp = df_lp.withColumn('sg_disciplina', f.lit('LP'))

# COMMAND ----------

df = df_lp.union(df_mt)

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.write.parquet(target, mode='overwrite')