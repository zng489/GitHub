# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC This object is type truncate/full insert
# MAGIC 
# MAGIC Processo raw_trs_etapa_ensino
# MAGIC Tabela/Arquivo Origem /raw/crw/inep_curso_educacao_profissional/
# MAGIC Tabela/Arquivo Destino /trs/uniepro/curso_educacao_profissional/
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino etapa de ensino forma uma estrutura de códigos que visa orientar, organizar e consolidar 
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga raw 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
import pyspark.sql.functions as f

import re
import json

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

try:
  tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
except:
  # USE THIS ONLY FOR DEVELOPMENT PURPOSES
  from datetime import datetime
  
  tables =  {
    "path_origin": "crw/inep_censo_escolar/curso_educacao_profissional/",
    "path_destination": "inep_censo_escolar/curso_educacao_profissional/"
  }

  dls = {"folders":{"landing":"/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/raw","trusted":"/trs"}}

  adf = {
    'adf_factory_name': 'cnibigdatafactory', 
    'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
    'adf_pipeline_run_id': 'p1',
    'adf_trigger_id': 't1',
    'adf_trigger_name': 'author_dev',
    'adf_trigger_time': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    'adf_trigger_type': 'Manual'
  }

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

df = df.select(f.substring(f.col('EIXO'), 0, 2).cast('int').alias('cd_eixo'),
               f.col('EIXO').alias('nm_eixo'),
               f.col('COD_CURSO').cast('int').alias('cd_curso_educacao_profissional'),
               f.col('NOME_CURSO').alias('nm_curso_educacao_profissional'),
               'ano')

# COMMAND ----------

w = Window.partitionBy('cd_eixo', 'cd_curso_educacao_profissional')
df = (df
      .withColumn('tmp', f.max('ano').over(w))
      .where(f.col('ano') == f.col('tmp'))
      .drop('ano', 'tmp'))

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.save(path=target, format="parquet", mode='overwrite')