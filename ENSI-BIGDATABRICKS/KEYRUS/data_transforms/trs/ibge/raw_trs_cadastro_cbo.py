# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES
# tables =  {
#   "path_origin": "usr/ibge/cadastro_cbo",
#   "path_destination": "ibge/cadastro_cbo/"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_cadastro_cbo',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

df_columns = [('COD_CBO6', 'CD_CBO6'), ('DESC_CBO6', 'DS_CBO6'), ('COD_CBO4', 'CD_CBO4'), ('DESC_CBO4', 'DS_CBO4'), ('COD_CBO3', 'CD_CBO3'), ('DESC_CBO3', 'DS_CBO3'), ('COD_CBO2', 'CD_CBO2'), ('DESC_CBO2', 'DS_CBO2'), ('COD_CBO1', 'CD_CBO1'), ('DESC_CBO1', 'DS_CBO1'), ('COD_TIPO_FAMILIA', 'CD_TIPO_FAMILIA'), ('DESC_TIPO_FAMILIA', 'DS_TIPO_FAMILIA'), ('COD_GRUPO_FAMILIA', 'CD_GRUPO_FAMILIA'), ('DESC_GRUPO_FAMILIA', 'DS_GRUPO_FAMILIA'), ('COD_CORPORATIVAS_INDUSTRIAIS', 'CD_CORPORATIVAS_INDUSTRIAIS'), ('DESC_CORPORATIVAS_INDUSTRIAIS', 'DS_CORPORATIVAS_INDUSTRIAIS'), ('COD_AREA_FORMACAO_INDUSTRIAL', 'CD_AREA_FORMACAO_INDUSTRIAL'), ('DESC_AREA_FORMACAO_INDUSTRIAL', 'DS_AREA_FORMACAO_INDUSTRIAL'), ('COD_NIVEL_ESCOLARIDADE', 'CD_NIVEL_ESCOLARIDADE'), ('DESC_NIVEL_ESCOLARIDADE', 'DS_NIVEL_ESCOLARIDADE'), ('COD_QUALIFICACAO_UNITRAB', 'CD_QUALIFICACAO_UNITRAB'), ('DESC_QUALIFICACAO_UNITRAB', 'DS_QUALIFICACAO_UNITRAB'), ('COD_TIPOLOGIA_OE', 'CD_TIPOLOGIA_OE'), ('DESC_TIPOLOGIA_OE', 'DS_TIPOLOGIA_OE'), ('COD_ESTRATO_TECNOLOGICO', 'CD_ESTRATO_TECNOLOGICO'), ('DESC_ESTRATO_TECNOLOGICO', 'DS_ESTRATO_TECNOLOGICO'), ('COD_NATUREZA_OCUPACAO', 'CD_NATUREZA_OCUPACAO'), ('DESC_NATUREZA_OCUPACAO', 'DS_NATUREZA_OCUPACAO'), ('COD_ENGENHARIA', 'CD_ENGENHARIA'), ('DESC_ENGENHARIA', 'DS_ENGENHARIA')]
df_columns = (f.col(org).alias(dst) for org, dst in df_columns)

df = df.select(*df_columns)

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.save(path=target, format="parquet", mode='overwrite')