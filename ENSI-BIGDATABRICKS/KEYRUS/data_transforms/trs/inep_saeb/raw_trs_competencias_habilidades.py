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
#   'path_origin_mt': 'usr/inep_saeb/competencias_e_habilidades_mt',
#   'path_origin_lp': 'usr/inep_saeb/competencias_e_habilidades_lp',
#   'path_destination': 'inep_saeb/competencias_habilidades'
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_competencias_habilidades',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#  'adf_trigger_name': 'author_dev',
#  'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#  'adf_trigger_type': 'Manual'
# }


# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source_mt = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin_mt"])
source_mt

# COMMAND ----------

source_lp = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin_lp"])
source_lp

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

df_mt = spark.read.parquet(source_mt)
df_mt = df_mt.withColumnRenamed('ano','nr_ano')
df_mt = df_mt.withColumnRenamed('disciplina','sg_disciplina')
df_mt = df_mt.withColumnRenamed('serie','nr_serie')
df_mt = df_mt.withColumnRenamed('base','ds_base')
df_mt = df_mt.withColumnRenamed('descricao_do_tema','ds_tema')
df_mt = df_mt.withColumnRenamed('proficiencia_acima_de','vl_proficiencia')
df_mt = df_mt.withColumnRenamed('intervalor_de_proficiencia','ds_intervalor_proficiencia')
df_mt = df_mt.withColumnRenamed('nivel_mt','vl_nivel')
df_mt = df_mt.withColumnRenamed('tema','sg_tema')
df_mt = df_mt.withColumnRenamed('habilidades','ds_habilidades')
df_mt = df_mt.withColumnRenamed('habilidades_resumo','ds_habilidades_resumo')
df_mt = df_mt.withColumnRenamed('habilidades_resumo2','ds_habilidades_resumo2')
df_mt = df_mt.withColumnRenamed('conector_mt','ds_conector')
df_mt = df_mt.withColumnRenamed('class_profic_mt','vl_class_profic')

# COMMAND ----------

df_lp = spark.read.parquet(source_lp)
df_lp = df_lp.withColumnRenamed('ano','nr_ano')
df_lp = df_lp.withColumnRenamed('disciplina','sg_disciplina')
df_lp = df_lp.withColumnRenamed('serie','nr_serie')
df_lp = df_lp.withColumnRenamed('base','ds_base')
df_lp = df_lp.withColumnRenamed('descricao_do_tema','ds_tema')
df_lp = df_lp.withColumnRenamed('proficiencia_acima_de','vl_proficiencia')
df_lp = df_lp.withColumnRenamed('intervalor_de_proficiencia','ds_intervalor_proficiencia')
df_lp = df_lp.withColumnRenamed('nivel_lp','vl_nivel')
df_lp = df_lp.withColumnRenamed('tema','sg_tema')
df_lp = df_lp.withColumnRenamed('habilidades','ds_habilidades')
df_lp = df_lp.withColumnRenamed('habilidades_resumo','ds_habilidades_resumo')
df_lp = df_lp.withColumnRenamed('habilidades_resumo2','ds_habilidades_resumo2')
df_lp = df_lp.withColumnRenamed('conector_lp','ds_conector')
df_lp = df_lp.withColumnRenamed('class_profic_lp','vl_class_profic')

# COMMAND ----------

df = df_lp.union(df_mt.select(*df_lp.columns))

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df = df.drop('dh_insercao_raw')

# COMMAND ----------

df.coalesce(1).write.parquet(target, mode='overwrite')