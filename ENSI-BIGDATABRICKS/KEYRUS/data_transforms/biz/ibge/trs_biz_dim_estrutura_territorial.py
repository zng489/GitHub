# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type truncate/full insert
# MAGIC 
# MAGIC <pre>
# MAGIC Processo raw_trs_estrutura_territorial
# MAGIC Tabela/Arquivo Origem /trs/mtd/corp/estrutura_territorial
# MAGIC Tabela/Arquivo Destino /biz/organizacao/dim_estrutura_territorial
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino Estrutura territorial forma uma estrutura de códigos que visa orientar, organizar e consolidar nacionalmente os estados,municipios e suas regiões negócios.
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga trusted 
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import *
from pyspark.sql.window import Window
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
from datetime import datetime, timedelta, date
import collections

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#tables =  {
#   "path_origin": "mtd/corp/estrutura_territorial/",
#   "path_destination": "corporativo/dim_estrutura_territorial/",
#   "destination": "/corporativo/dim_estrutura_territorial",
##   "databricks": {
#   "notebook": "/biz/ibge/trs_biz_dim_estrutura_territorial"
#   }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz"}}
#dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

#adf = {
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

# COMMAND ----------

df = df.withColumn('cd_municipio_6posicoes', substring('cd_municipio', 1, 6))

# COMMAND ----------

df = df.drop("dh_ultima_atualizacao_oltp","dh_insercao_trs")

# COMMAND ----------

records_to_add = spark.createDataFrame([[-98, 'NÃO INFORMADA', -98, 'NÃO INFORMADA' , -98, 'NÃO INFORMADA' , -98, 'NÃO INFORMADA' , -98, 'NÃO INFORMADA', -98], [-99, 'NÃO SE APLICA', -99, 'NÃO SE APLICA' , -99, 'NÃO SE APLICA' , -99, 'NÃO SE APLICA' , -99, 'NÃO SE APLICA', -99]], schema = df.schema)

# COMMAND ----------

df = df.union(records_to_add)

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_biz', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

tableExists = True
try:
  df_sink = spark.read.parquet(target)
  df = df.join(df_sink, df.cd_municipio == df_sink.cd_municipio, how='left_anti')
except:
  tableExists = False

# COMMAND ----------

write_mode = 'append' if tableExists else 'overwrite'
write_mode 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.save(path=target, format="parquet", mode=write_mode)