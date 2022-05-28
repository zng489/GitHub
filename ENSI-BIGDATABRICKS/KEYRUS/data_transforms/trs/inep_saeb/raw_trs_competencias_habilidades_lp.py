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
#   'path_origin': 'usr/uniepro/saeb/competencias_e_habilidades/lp',
#   'path_destination': 'inep_saeb/competencias_habilidades_lp',
# }
#
#dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

#adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_competencias_e_habilidades_lp',
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

df = df.withColumnRenamed('ano','nr_ano')
df = df.withColumnRenamed('disciplina','sg_disciplina')
df = df.withColumnRenamed('serie','nr_serie')
df = df.withColumnRenamed('base','ds_base')
df = df.withColumnRenamed('proficiencia_acima_de','vl_proficiencia')
df = df.withColumnRenamed('intervalor_de_proficiencia','ds_intervalor_proficiencia')
df = df.withColumnRenamed('nivel_mt','vl_nivel')
df = df.withColumnRenamed('tema','sg_tema')
df = df.withColumnRenamed('habilidades','ds_habilidades')
df = df.withColumnRenamed('habilidades_resumo','ds_habilidades_resumo')
df = df.withColumnRenamed('habilidades_resumo2','ds_habilidades_resumo2')
df = df.withColumnRenamed('conector_mt','ds_conector')
df = df.withColumnRenamed('class_profic_mt','vl_class_profic')

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df = df.drop('dh_insercao_raw')

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet(target, mode='overwrite')