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
# MAGIC Processo	??
# MAGIC Tabela/Arquivo Origem	biz/uniepro/dim_saeb_competencia_habilidade
# MAGIC Tabela/Arquivo Destino	biz/uniepro/dim_saeb_competencia_habilidade_kpi
# MAGIC Particionamento Tabela/Arquivo Destino	
# MAGIC Descrição Tabela/Arquivo Destino	Relação das Competências e Habilidades dos níveis de proficiência
# MAGIC Tipo Atualização	A = append (insert)
# MAGIC Detalhe Atualização	
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

table =  {
  "path_origin": "uniepro/dim_saeb_competencia_habilidade",
  "path_destination": "uniepro/dim_saeb_competencia_habilidade_kpi",
  "destination": "/uniepro/dim_saeb_competencia_habilidade_kpi",
  "databricks": {
    "notebook": "/biz/inep_saeb/biz_biz_dim_saeb_competencia_habilidade_kpi"
  }
}

# # dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/trs","business":"/biz"}}
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

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=table["path_destination"])
target

# COMMAND ----------

df_source = spark.read.parquet(source)

df_source_columns = [('NR_ANO', 'ANO'), ('DS_BASE', 'BASE'), ('VL_CLASS_PROFIC_LP', 'CLASS_PROFIC_LP'), ('VL_CLASS_PROFIC_MT', 'CLASS_PROFIC_MT'), ('DS_CONECTOR_LP', 'conector_LP'), ('DS_CONECTOR_MT', 'conector_MT'), ('DS_TEMA', 'DESCRICAO_DO_TEMA'), ('SG_DISCIPLINA', 'DISCIPLINA'), ('DS_HABILIDADES', 'HABILIDADES'), ('DS_HABILIDADES_RESUMO', 'HABILIDADES_RESUMO'), ('DS_HABILIDADES_RESUMO2', 'HABILIDADES_RESUMO2'), ('DS_INTERVALOR_PROFICIENCIA', 'INTERVALOR_DE_PROFICIENCIA'), ('VL_NIVEL_LP', 'NIVEL_LP'), ('VL_NIVEL_LP_AJUSTADO', 'NIVEL_LP_AJUSTADO'), ('VL_NIVEL_MT', 'NIVEL_MT'), ('VL_NIVEL_MT_AJUSTADO', 'NIVEL_MT_AJUSTADO'), ('VL_PROFICIENCIA', 'PROFICIENCIA_ACIMA_DE'), ('NR_SERIE', 'SERIE'), ('SG_TEMA', 'TEMA')]
df_source_columns = (f.col(org).alias(dst) for org, dst in df_source_columns)

df_source = df_source.select(*df_source_columns)

# COMMAND ----------

df_source = df_source.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df_source.write.parquet(target, mode='overwrite')