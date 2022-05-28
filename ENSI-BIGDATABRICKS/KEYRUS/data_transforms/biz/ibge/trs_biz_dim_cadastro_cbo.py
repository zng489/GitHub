# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

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
import pyspark.sql.functions as f

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# tables =  {
#   "path_origin": "ibge/cadastro_cbo",
#   "path_destination": "ibge/dim_cadastro_cbo",
#   "destination": "/ibge/dim_cadastro_cbo",
#   "databricks": {
#     "notebook": "/biz/ibge/trs_biz_dim_cadastro_cbo"
#   }
# }

# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'trs_biz_dim_estrutura_territorial',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

df_columns = ['CD_CBO6', 'DS_CBO6', 'CD_CBO4', 'DS_CBO4', 'CD_CBO3', 'DS_CBO3', 'CD_CBO2', 'DS_CBO2', 'CD_CBO1', 'DS_CBO1', 'CD_TIPO_FAMILIA', 'DS_TIPO_FAMILIA', 'CD_GRUPO_FAMILIA', 'DS_GRUPO_FAMILIA', 'CD_CORPORATIVAS_INDUSTRIAIS', 'DS_CORPORATIVAS_INDUSTRIAIS', 'CD_AREA_FORMACAO_INDUSTRIAL', 'DS_AREA_FORMACAO_INDUSTRIAL', 'CD_NIVEL_ESCOLARIDADE', 'DS_NIVEL_ESCOLARIDADE', 'CD_QUALIFICACAO_UNITRAB', 'DS_QUALIFICACAO_UNITRAB', 'CD_TIPOLOGIA_OE', 'DS_TIPOLOGIA_OE', 'CD_ESTRATO_TECNOLOGICO', 'DS_ESTRATO_TECNOLOGICO', 'CD_NATUREZA_OCUPACAO', 'DS_NATUREZA_OCUPACAO', 'CD_ENGENHARIA', 'DS_ENGENHARIA']

df = df.select(*df_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.parquet(target, mode='overwrite')