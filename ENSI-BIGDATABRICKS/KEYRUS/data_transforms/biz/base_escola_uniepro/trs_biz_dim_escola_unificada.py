# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
from pyspark.sql.functions import *

import json
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES
# tables =  {
#   "path_origin": "mtd/corp/base_escolas/",
#   "path_destination": "corporativo/dim_escola_unificada/",
#   "destination": "/corporativo/dim_escola_unificada",
#   "databricks": {
#     "notebook": "/biz/base_escola_uniepro/trs_biz_dim_escola_unificada"
#   }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
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

# COMMAND ----------

df = df.withColumn('nm_dependencia_adm_escola',
                  when(col('tp_dependencia') == lit(2), lit('Estadual'))
                   .when(col('tp_dependencia') == lit(3), lit('Municipal'))
                   .when(col('tp_dependencia') == lit(1), lit('Federal'))
                   .when(col('tp_dependencia') == lit(4), lit('Privada'))
                   .when(col('tp_dependencia') == lit(11), lit('SESI'))
                   .when(col('tp_dependencia') == lit(12), lit('SENAI'))
                   .when(col('tp_dependencia') == lit(13), lit('SESI SENAI'))
                   .otherwise(lit('Não Informado')))

# COMMAND ----------

df = df.select('cd_entidade', 
          'nm_unidade_atendimento_inep', 
          col('cd_oba').alias('cd_unidade_atendimento_dr'), 
          col('tp_dependencia').alias('cd_dependencia_adm_escola'), 
          'cd_dep_adm_escola_sesi_senai', 
          'nm_dependencia_adm_escola')

# COMMAND ----------

df = df.drop_duplicates()

# COMMAND ----------

records_to_add = spark.createDataFrame([[-98, 'NÃO INFORMADA',-98,-98,-98, 'NÃO INFORMADA'], [-99,'NÃO SE APLICA',-99,-99,-99,'NÃO SE APLICA']], schema = df.schema)

# COMMAND ----------

df = df.union(records_to_add)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.parquet(path=target, mode='overwrite')