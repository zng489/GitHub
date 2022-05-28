# Databricks notebook source
# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import json
import re

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES

# table =  {
#   "path_origin": "inep_saeb/competencias_habilidades",
#   "path_competence": "inep_saeb/competencias_habilidades_ajuste",
#   "path_destination": "uniepro/dim_saeb_competencia_habilidade",
#   "destination": "/uniepro/dim_saeb_competencia_habilidade",
#   "databricks": {
#     "notebook": "/biz/inep_saeb/trs_biz_dim_competencia_habilidade"
#   }
# }

# # dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/trs","business":"/biz"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

# adf = {
#   'adf_factory_name': 'development', 
#   'adf_pipeline_name': 'development',
#   'adf_pipeline_run_id': 'development',
#   'adf_trigger_id': 'development',
#   'adf_trigger_name': 'development',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_origin"])
source

# COMMAND ----------

source_competence = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_competence"])
source_competence

# COMMAND ----------

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=table["path_destination"])
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source = df_source.select('NR_ANO', 'DS_BASE', 'DS_TEMA', 'SG_DISCIPLINA', 'DS_HABILIDADES', 'DS_HABILIDADES_RESUMO', 
                             'DS_HABILIDADES_RESUMO2', 'DS_INTERVALOR_PROFICIENCIA', 'VL_PROFICIENCIA', 'NR_SERIE', 'SG_TEMA', 'VL_NIVEL', 
                             f.when(f.col('SG_DISCIPLINA') == f.lit('LP'), f.col('VL_CLASS_PROFIC')).alias('VL_CLASS_PROFIC_LP'),
                             f.when(f.col('SG_DISCIPLINA') == f.lit('MT'), f.col('VL_CLASS_PROFIC')).alias('VL_CLASS_PROFIC_MT'),
                             f.when(f.col('SG_DISCIPLINA') == f.lit('LP'), f.col('DS_CONECTOR')).alias('DS_CONECTOR_LP'),
                             f.when(f.col('SG_DISCIPLINA') == f.lit('MT'), f.col('DS_CONECTOR')).alias('DS_CONECTOR_MT'),
                             f.when(f.col('SG_DISCIPLINA') == f.lit('LP'), f.col('VL_NIVEL')).alias('VL_NIVEL_LP'),
                             f.when(f.col('SG_DISCIPLINA') == f.lit('MT'), f.col('VL_NIVEL')).alias('VL_NIVEL_MT'))

# COMMAND ----------

df_competence = spark.read.parquet(source_competence)
df_competence = df_competence.select('VL_NIVEL',
                                     'SG_DISCIPLINA',
                                     f.when(f.col('SG_DISCIPLINA') == f.lit('LP'), f.col('VL_NIVEL_AJUSTADO')).alias('VL_NIVEL_LP_AJUSTADO'),
                                     f.when(f.col('SG_DISCIPLINA') == f.lit('MT'), f.col('VL_NIVEL_AJUSTADO')).alias('VL_NIVEL_MT_AJUSTADO'))
df_competence = df_competence.drop_duplicates()

# COMMAND ----------

df = df_source.join(df_competence, on=['SG_DISCIPLINA', 'VL_NIVEL'], how='left')

# COMMAND ----------

ordered_columns = ['NR_ANO', 'DS_BASE', 'VL_CLASS_PROFIC_LP', 'VL_CLASS_PROFIC_MT', 'DS_CONECTOR_LP', 'DS_CONECTOR_MT', 'DS_TEMA', 'SG_DISCIPLINA', 'DS_HABILIDADES', 'DS_HABILIDADES_RESUMO', 'DS_HABILIDADES_RESUMO2', 'DS_INTERVALOR_PROFICIENCIA', 'VL_NIVEL_LP', 'VL_NIVEL_LP_AJUSTADO', 'VL_NIVEL_MT', 'VL_NIVEL_MT_AJUSTADO', 'VL_PROFICIENCIA', 'NR_SERIE', 'SG_TEMA']
df = df.select(*ordered_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

str_cols = [str_col for str_col, _type in df.dtypes if _type == 'string']
for str_col in str_cols:
  df = df.withColumn(str_col, f.when(f.length(str_col) > f.lit(4000), f.substring(f.col(str_col), 0, 4000)).otherwise(f.col(str_col)))

# COMMAND ----------

df.write.parquet(target, mode='overwrite')