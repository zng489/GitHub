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
# MAGIC Processo raw_trs_estrutura_territorial
# MAGIC Tabela/Arquivo Origem /raw/crw/ibge/relatorio_dtb_brasil_municipio
# MAGIC Tabela/Arquivo Destino /trs/mtd/corp/estrutura_territorial/estrutura_territorial
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino Estrutura territorial forma uma estrutura de códigos que visa orientar, organizar e consolidar nacionalmente os estados,municipios e suas regiões negócios.
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga raw 

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

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
#   "path_origin": "usr/ibge/classificacao_ocde",
#   "path_destination": "ibge/classificacao_ocde/"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/tmp/dev/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_classificacao_ocde',
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

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"codigo_ocde" : "int",                    
                   "descricao_ocde": "varchar(250)",
                   "intensidade_tecnologica_ocde": "varchar(250)"}
for c in column_type_map:
  df = df.withColumn(c, df[c].cast(column_type_map[c]))

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

write_mode = 'overwrite'
write_mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.save(path=target, format="parquet", mode=write_mode)