# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_matricula				
# MAGIC Tabela/Arquivo Origem	/trs/mtd/corp/matriculas				
# MAGIC Tabela/Arquivo Destino	/biz/uniepro/fta_matricula				
# MAGIC Particionamento Tabela/Arquivo Destino	nr_ano_censo				
# MAGIC Descrição Tabela/Arquivo Destino	Base de matrículas da Educação Profissional				
# MAGIC Tipo Atualização	A = append (insert)				
# MAGIC Detalhe Atualização	Apenas inserir caso 'nr_ano_censo não existir na tabela, se existir não carregar.				
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

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES

# tables =  {
#  "path_origin": "inep_censo_escolar/matriculas/",
#  "path_destination": "uniepro/fta_matricula",
# "destination": "/uniepro/fta_matricula/",
#  "databricks": {
#    "notebook": "/biz/inep_censo_escolar/trs_biz_fta_matricula"
#  }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'trs_biz_fta_matricula',
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

df = df.where(f.col('fl_profissionalizante') == f.lit(1))

# COMMAND ----------

df = df.select('nr_ano_censo', 'id_turma', 'cd_mediacao_didatico_pedago', 'cd_curso_educacao_profissional', 'tp_etapa_ensino', 'cd_entidade', 'cd_municipio_escola', 'nr_idade_censo', 'cd_sexo', 'cd_municipio_residencia', 'cd_localizacao', 'fl_eja', 'fl_necessidade_especial', 'fl_profissionalizante', 'fl_mant_escola_privada_sist_s', 'id_matricula','cd_dep_adm_escola_sesi_senai','cd_dependencia_adm_escola')

# COMMAND ----------

df = (df
      .groupBy('nr_ano_censo', 'id_turma', 'cd_mediacao_didatico_pedago', 'cd_curso_educacao_profissional', 'tp_etapa_ensino', 'cd_entidade', 'cd_municipio_escola', 'nr_idade_censo', 'cd_sexo', 'cd_municipio_residencia', 'cd_localizacao', 'fl_eja', 'fl_necessidade_especial', 'fl_profissionalizante', 'fl_mant_escola_privada_sist_s','cd_dep_adm_escola_sesi_senai','cd_dependencia_adm_escola')
      .agg(f.count('id_matricula').cast('int').alias('qt_matricula')))

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.partitionBy('nr_ano_censo').save(path=target, format='parquet', mode='overwrite')