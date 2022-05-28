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
#   "path_origin": "ibge/faixa_etaria",
#   "path_destination": "ibge/dim_faixa_etaria",
#   "destination": "/ibge/dim_faixa_etaria",
#   "databricks": {
#     "notebook": "/biz/ibge/trs_biz_dim_faixa_etaria"
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
df_columns = ['CD_TIPO_FAIXA_ETARIA', 'CD_FAIXA_ETARIA', 'NR_DE', 'NR_ATE', 'DS_FAIXA_ETARIA']

df = df.select(*df_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.parquet(target, mode='overwrite')