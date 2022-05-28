# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC This object is type truncate/full insert
# MAGIC 
# MAGIC Processo	trs_biz_dim_curso_educacao_profissional				
# MAGIC Tabela/Arquivo Origem	/trs/inep_censo_escolar/curso_educacao_profissional				
# MAGIC Tabela/Arquivo Destino	/biz/corporativo/dim_curso_educacao_profissional				
# MAGIC Particionamento Tabela/Arquivo Destino	N/A				
# MAGIC Descrição Tabela/Arquivo Destino	Base gerada pela INEP com as informações de Cusros de educação profissional técnica bem como as áreas de formação profissional				
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)				
# MAGIC Detalhe Atualização	Não se aplica				
# MAGIC Periodicidade/Horario Execução	Anual				
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
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

# COMMAND ----------

try:
  tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
  user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))
except:
  # USE THIS ONLY FOR DEVELOPMENT PURPOSES
  from datetime import datetime

  tables =  {
    "path_origin": "inep_censo_escolar/curso_educacao_profissional/",
    "path_destination": "corporativo/dim_curso_educacao_profissional/",
    "destination": "/corporativo/dim_curso_educacao_profissional/",
    "databricks": {
      "notebook": "/biz/inep_censo_escolar/trs_biz_dim_curso_educacao_profissional"
    }
  }

  dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}
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
df = df.select('cd_curso_educacao_profissional', 'nm_curso_educacao_profissional', 'cd_eixo', 'nm_eixo')

# COMMAND ----------

records_to_add = spark.createDataFrame([[-98, 'NÃO INFORMADA', -98, 'NÃO INFORMADA'], [-99, 'NÃO SE APLICA', -99, 'NÃO SE APLICA']], schema=df.schema)

# COMMAND ----------

df = df.union(records_to_add)

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.save(path=target, format="parquet", mode='overwrite')