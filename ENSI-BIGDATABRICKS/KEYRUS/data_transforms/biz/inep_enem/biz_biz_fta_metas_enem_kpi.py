# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_enem_agregado
# MAGIC Tabela/Arquivo Origem	/biz/uniepro/base_metas_enem_2017
# MAGIC Tabela/Arquivo Destino	/biz/uniepro/base_metas_enem_2017_pivot
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
#   "year": 2017,
#   "path_origin": "uniepro/fta_base_metas_enem",
#   "path_destination": "uniepro/fta_base_metas_enem_kpi",
#   "destination": "/uniepro/fta_base_metas_enem_kpi",
#   "databricks": {
#     "notebook": "/biz/inep_enem/biz_biz_fta_metas_enem_kpi"
#   }
# }

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

biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=table["path_origin"])
source

# COMMAND ----------

biz_target = "{biz}/{origin}".format(biz=biz, origin=table["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)
target

# COMMAND ----------

df_source = spark.read.parquet(source)

goals_columns = [col for col in df_source.columns if col.startswith('VL_META_')]
goals_renamed_columns = ['Meta_%s' % col.split('_')[-1] for col in goals_columns]

df_source_columns = [('CD_ENTIDADE', 'COENTIDADE'), ('FL_ESCOLA_FORA', 'EscolaFora'), ('CD_PERFIL', 'PERFIL'), ('FL_EXCLUI_PERFIL', 'ExcluiPerfil')] + list(zip(goals_columns, goals_renamed_columns)) + [('VL_NOTA_MEDIA_MELHORES_5PCT_COM_PERFIL', 'NOTA_MEDIA_MELHORES_5PCT_com_perfil'), ('VL_NU_NOTA_GERAL_REDAC', 'NuNotaGeralRedac')]
df_source_columns = (f.col(org).alias(dst) for org, dst in df_source_columns)

df_source = df_source.select(*df_source_columns)

# COMMAND ----------

na_columns = ['MetaA_5pct', 'MetaA75', 'MetaB_5pct', 'MetaB80', 'MetaC_5pct', 'MetaC85', 'NotaMaximaEscola', 'NotaMaximaPerfil', 'NotaMaximaUf', 'NOTA_MEDIA_MELHORES_5PCT']
for na in na_columns:
  df_source = df_source.withColumn(na, f.lit(0))

# COMMAND ----------

reorder_columns = ['COENTIDADE', 'EscolaFora', 'PERFIL', 'ExcluiPerfil'] + goals_renamed_columns + ['MetaA_5pct', 'MetaA75', 'MetaB_5pct', 'MetaB80', 'MetaC_5pct', 'MetaC85', 'NotaMaximaEscola', 'NotaMaximaPerfil', 'NotaMaximaUf', 'NOTA_MEDIA_MELHORES_5PCT', 'NOTA_MEDIA_MELHORES_5PCT_com_perfil', 'NuNotaGeralRedac']
df_source = df_source.select(*reorder_columns)

# COMMAND ----------

df_source = df_source.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df_source.coalesce(1).write.parquet(target, mode='overwrite')