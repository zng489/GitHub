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

import crawler.functions as cf
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
#   "path_origin": "uniepro/fta_saeb_alunos",
#   "path_territorial": "corporativo/dim_estrutura_territorial",
#   "path_destination": "uniepro/fta_saeb_alunos_kpi",
#   "destination": "/uniepro/fta_saeb_alunos_kpi",
#   "databricks": {
#     "notebook": "/biz/inep_saeb/biz_biz_fta_saeb_alunos_kpi"
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

source_territorial = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=table["path_territorial"])
source_territorial

# COMMAND ----------

biz_target = "{biz}/{origin}".format(biz=biz, origin=table["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source_columns = ['ID_PROVA_BRASIL', 'ID_REGIAO', 'ID_UF', 'ID_MUNICIPIO', 'ID_AREA', 'ID_DEPENDENCIA_ADM', 'ID_LOCALIZACAO', 'ID_TURMA', 'ID_TURNO', 'ID_SERIE', 'ID_ALUNO', 'ID_CADERNO', 'ID_BLOCO_1', 'ID_BLOCO_2', 'TX_RESP_BLOCO_1_LP', 'TX_RESP_BLOCO_2_LP', 'TX_RESP_BLOCO_1_MT', 'TX_RESP_BLOCO_2_MT', 'ID_ESCOLA_ORIGINAL', 'ID_ESCOLASEMMASCARA', 'ID_MASCARA', 'PC_FORMACAO_DOCENTE_INICIAL', 'PC_FORMACAO_DOCENTE_FINAL', 'PC_FORMACAO_DOCENTE_MEDIO', 'NIVEL_SOCIO_ECONOMICO']

df_source_renamed_columns = [('FL_SITUACAO_CENSO', 'IN_SITUACAO_CENSO'), ('FL_PREENCHIMENTO_PROVA', 'IN_PREENCHIMENTO_PROVA'), ('FL_PRESENCA_PROVA', 'IN_PRESENCA_PROVA'), ('FL_PROFICIENCIA', 'IN_PROFICIENCIA'), ('FL_PROVA_BRASIL', 'IN_PROVA_BRASIL'), ('DS_ESTRATO_ANEB', 'ESTRATO_ANEB'), ('VL_PESO_ALUNO_LP', 'PESO_ALUNO_LP'), ('VL_PESO_ALUNO_MT', 'PESO_ALUNO_MT'), ('VL_PROFICIENCIA_LP', 'PROFICIENCIA_LP'), ('VL_ERRO_PADRAO_LP', 'ERRO_PADRAO_LP'), ('VL_PROFICIENCIA_LP_SAEB', 'PROFICIENCIA_LP_SAEB'), ('VL_ERRO_PADRAO_LP_SAEB', 'ERRO_PADRAO_LP_SAEB'), ('VL_PROFICIENCIA_MT', 'PROFICIENCIA_MT'), ('VL_ERRO_PADRAO_MT', 'ERRO_PADRAO_MT'), ('VL_PROFICIENCIA_MT_SAEB', 'PROFICIENCIA_MT_SAEB'), ('VL_ERRO_PADRAO_MT_SAEB', 'ERRO_PADRAO_MT_SAEB'), ('FL_PREENCHIMENTO_QUESTIONARIO', 'IN_PREENCHIMENTO_QUESTIONARIO'), ('VL_TX_RESP_Q001', 'TX_RESP_Q001'), ('VL_TX_RESP_Q002', 'TX_RESP_Q002'), ('VL_TX_RESP_Q003', 'TX_RESP_Q003'), ('VL_TX_RESP_Q004', 'TX_RESP_Q004'), ('VL_TX_RESP_Q005', 'TX_RESP_Q005'), ('VL_TX_RESP_Q006', 'TX_RESP_Q006'), ('VL_TX_RESP_Q007', 'TX_RESP_Q007'), ('VL_TX_RESP_Q008', 'TX_RESP_Q008'), ('VL_TX_RESP_Q009', 'TX_RESP_Q009'), ('VL_TX_RESP_Q010', 'TX_RESP_Q010'), ('VL_TX_RESP_Q011', 'TX_RESP_Q011'), ('VL_TX_RESP_Q012', 'TX_RESP_Q012'), ('VL_TX_RESP_Q013', 'TX_RESP_Q013'), ('VL_TX_RESP_Q014', 'TX_RESP_Q014'), ('VL_TX_RESP_Q015', 'TX_RESP_Q015'), ('VL_TX_RESP_Q016', 'TX_RESP_Q016'), ('VL_TX_RESP_Q017', 'TX_RESP_Q017'), ('VL_TX_RESP_Q018', 'TX_RESP_Q018'), ('VL_TX_RESP_Q019', 'TX_RESP_Q019'), ('VL_TX_RESP_Q020', 'TX_RESP_Q020'), ('VL_TX_RESP_Q021', 'TX_RESP_Q021'), ('VL_TX_RESP_Q022', 'TX_RESP_Q022'), ('VL_TX_RESP_Q023', 'TX_RESP_Q023'), ('VL_TX_RESP_Q024', 'TX_RESP_Q024'), ('VL_TX_RESP_Q025', 'TX_RESP_Q025'), ('VL_TX_RESP_Q026', 'TX_RESP_Q026'), ('VL_TX_RESP_Q027', 'TX_RESP_Q027'), ('VL_TX_RESP_Q028', 'TX_RESP_Q028'), ('VL_TX_RESP_Q029', 'TX_RESP_Q029'), ('VL_TX_RESP_Q030', 'TX_RESP_Q030'), ('VL_TX_RESP_Q031', 'TX_RESP_Q031'), ('VL_TX_RESP_Q032', 'TX_RESP_Q032'), ('VL_TX_RESP_Q033', 'TX_RESP_Q033'), ('VL_TX_RESP_Q034', 'TX_RESP_Q034'), ('VL_TX_RESP_Q035', 'TX_RESP_Q035'), ('VL_TX_RESP_Q036', 'TX_RESP_Q036'), ('VL_TX_RESP_Q037', 'TX_RESP_Q037'), ('VL_TX_RESP_Q038', 'TX_RESP_Q038'), ('VL_TX_RESP_Q039', 'TX_RESP_Q039'), ('VL_TX_RESP_Q040_3EM', 'TX_RESP_Q040_3EM'), ('VL_TX_RESP_Q041', 'TX_RESP_Q041'), ('VL_TX_RESP_Q042', 'TX_RESP_Q042'), ('VL_TX_RESP_Q043_3EM', 'TX_RESP_Q043_3EM'), ('VL_TX_RESP_Q044', 'TX_RESP_Q044'), ('VL_TX_RESP_Q045', 'TX_RESP_Q045'), ('VL_TX_RESP_Q046_3EM', 'TX_RESP_Q046_3EM'), ('VL_TX_RESP_Q047_3EM', 'TX_RESP_Q047_3EM'), ('VL_TX_RESP_Q048_3EM', 'TX_RESP_Q048_3EM'), ('VL_TX_RESP_Q049_3EM', 'TX_RESP_Q049_3EM'), ('VL_TX_RESP_Q050_3EM', 'TX_RESP_Q050_3EM'), ('VL_TX_RESP_Q051', 'TX_RESP_Q051'), ('VL_TX_RESP_Q052', 'TX_RESP_Q052'), ('VL_TX_RESP_Q053', 'TX_RESP_Q053'), ('VL_TX_RESP_Q054_3EM', 'TX_RESP_Q054_3EM'), ('VL_TX_RESP_Q055_3EM', 'TX_RESP_Q055_3EM'), ('VL_TX_RESP_Q056_3EM', 'TX_RESP_Q056_3EM'), ('VL_TX_RESP_Q057_3EM', 'TX_RESP_Q057_3EM'), ('VL_TX_RESP_Q058_3EM', 'TX_RESP_Q058_3EM'), ('VL_TX_RESP_Q059', 'TX_RESP_Q059'), ('VL_TX_RESP_Q060', 'TX_RESP_Q060'), ('DS_BASE', 'BASE'), ('VL_NIVEL_LP', 'NIVEL_LP'), ('VL_NIVEL_MT', 'NIVEL_MT'), ('VL_TX_RESP_Q004_5EF', 'TX_RESP_Q004_5EF'), ('VL_TX_RESP_Q034_9EF', 'TX_RESP_Q034_9EF'), ('VL_TX_RESP_Q035_9EF', 'TX_RESP_Q035_9EF'), ('VL_TX_RESP_Q037_9EF', 'TX_RESP_Q037_9EF'), ('VL_TX_RESP_Q042_9EF', 'TX_RESP_Q042_9EF'), ('VL_TX_RESP_Q044_5EF', 'TX_RESP_Q044_5EF'), ('VL_TX_RESP_Q047_9EF', 'TX_RESP_Q047_9EF'), ('VL_TX_RESP_Q057_9EF', 'TX_RESP_Q057_9EF'), ('VL_TX_RESP_Q092', 'TX_RESP_Q092'), ('VL_TX_RESP_Q097', 'TX_RESP_Q097'), ('VL_TX_RESP_Q098', 'TX_RESP_Q098'), ('VL_TX_RESP_Q099', 'TX_RESP_Q099'), ('CD_ENTIDADE', 'CO_ENTIDADE'), ('NM_ENTIDADE', 'NO_ENTIDADE'), ('FL_ESCOLA_SESI', 'escola_sesi'), ('NR_MATRICULADOS_CENSO_5EF', 'NU_MATRICULADOS_CENSO_5EF'), ('NR_PRESENTES_5EF', 'NU_PRESENTES_5EF'), ('TX_PARTICIPACAO_5EF', 'TAXA_PARTICIPACAO_5EF'), ('NR_MATRICULADOS_CENSO_9EF', 'NU_MATRICULADOS_CENSO_9EF'), ('NR_PRESENTES_9EF', 'NU_PRESENTES_9EF'), ('TX_PARTICIPACAO_9EF', 'TAXA_PARTICIPACAO_9EF'), ('NR_MATRICULADOS_CENSO_3EM', 'NU_MATRICULADOS_CENSO_3EM'), ('NR_PRESENTES_3EM', 'NU_PRESENTES_3EM'), ('TX_PARTICIPACAO_3EM', 'TAXA_PARTICIPACAO_3EM'), ('DS_CONECTOR_LP', 'conector_LP'), ('DS_CONECTOR_MT', 'conector_MT')]
df_source_renamed_columns = (f.col(org).alias(dst) for org, dst in df_source_renamed_columns)

df_source = df_source.select(*df_source_columns, *df_source_renamed_columns)

# COMMAND ----------

df_source_territorial = spark.read.parquet(source_territorial)
df_source_territorial = df_source_territorial.select(f.col('cd_municipio').alias('ID_MUNICIPIO'), f.col('nm_municipio').alias('ID_NOME_MUNICIPIO'))

# COMMAND ----------

df = df_source.join(df_source_territorial, on='ID_MUNICIPIO', how='left')

# COMMAND ----------

ordered_columns = ['ID_PROVA_BRASIL', 'ID_REGIAO', 'ID_UF', 'ID_MUNICIPIO', 'ID_AREA', 'ID_DEPENDENCIA_ADM', 'ID_LOCALIZACAO', 'ID_TURMA', 'ID_TURNO', 'ID_SERIE', 'ID_ALUNO', 'IN_SITUACAO_CENSO', 'IN_PREENCHIMENTO_PROVA', 'IN_PRESENCA_PROVA', 'ID_CADERNO', 'ID_BLOCO_1', 'ID_BLOCO_2', 'TX_RESP_BLOCO_1_LP', 'TX_RESP_BLOCO_2_LP', 'TX_RESP_BLOCO_1_MT', 'TX_RESP_BLOCO_2_MT', 'IN_PROFICIENCIA', 'IN_PROVA_BRASIL', 'ESTRATO_ANEB', 'PESO_ALUNO_LP', 'PESO_ALUNO_MT', 'PROFICIENCIA_LP', 'ERRO_PADRAO_LP', 'PROFICIENCIA_LP_SAEB', 'ERRO_PADRAO_LP_SAEB', 'PROFICIENCIA_MT', 'ERRO_PADRAO_MT', 'PROFICIENCIA_MT_SAEB', 'ERRO_PADRAO_MT_SAEB', 'IN_PREENCHIMENTO_QUESTIONARIO', 'TX_RESP_Q001', 'TX_RESP_Q002', 'TX_RESP_Q003', 'TX_RESP_Q004', 'TX_RESP_Q005', 'TX_RESP_Q006', 'TX_RESP_Q007', 'TX_RESP_Q008', 'TX_RESP_Q009', 'TX_RESP_Q010', 'TX_RESP_Q011', 'TX_RESP_Q012', 'TX_RESP_Q013', 'TX_RESP_Q014', 'TX_RESP_Q015', 'TX_RESP_Q016', 'TX_RESP_Q017', 'TX_RESP_Q018', 'TX_RESP_Q019', 'TX_RESP_Q020', 'TX_RESP_Q021', 'TX_RESP_Q022', 'TX_RESP_Q023', 'TX_RESP_Q024', 'TX_RESP_Q025', 'TX_RESP_Q026', 'TX_RESP_Q027', 'TX_RESP_Q028', 'TX_RESP_Q029', 'TX_RESP_Q030', 'TX_RESP_Q031', 'TX_RESP_Q032', 'TX_RESP_Q033', 'TX_RESP_Q034', 'TX_RESP_Q035', 'TX_RESP_Q036', 'TX_RESP_Q037', 'TX_RESP_Q038', 'TX_RESP_Q039', 'TX_RESP_Q040_3EM', 'TX_RESP_Q041', 'TX_RESP_Q042', 'TX_RESP_Q043_3EM', 'TX_RESP_Q044', 'TX_RESP_Q045', 'TX_RESP_Q046_3EM', 'TX_RESP_Q047_3EM', 'TX_RESP_Q048_3EM', 'TX_RESP_Q049_3EM', 'TX_RESP_Q050_3EM', 'TX_RESP_Q051', 'TX_RESP_Q052', 'TX_RESP_Q053', 'TX_RESP_Q054_3EM', 'TX_RESP_Q055_3EM', 'TX_RESP_Q056_3EM', 'TX_RESP_Q057_3EM', 'TX_RESP_Q058_3EM', 'TX_RESP_Q059', 'TX_RESP_Q060', 'BASE', 'NIVEL_LP', 'NIVEL_MT', 'TX_RESP_Q004_5EF', 'TX_RESP_Q034_9EF', 'TX_RESP_Q035_9EF', 'TX_RESP_Q037_9EF', 'TX_RESP_Q042_9EF', 'TX_RESP_Q044_5EF', 'TX_RESP_Q047_9EF', 'TX_RESP_Q057_9EF', 'TX_RESP_Q092', 'TX_RESP_Q097', 'TX_RESP_Q098', 'TX_RESP_Q099', 'CO_ENTIDADE', 'ID_ESCOLA_ORIGINAL', 'ID_ESCOLASEMMASCARA', 'NO_ENTIDADE', 'escola_sesi', 'ID_MASCARA', 'ID_NOME_MUNICIPIO', 'PC_FORMACAO_DOCENTE_INICIAL', 'PC_FORMACAO_DOCENTE_FINAL', 'PC_FORMACAO_DOCENTE_MEDIO', 'NIVEL_SOCIO_ECONOMICO', 'NU_MATRICULADOS_CENSO_5EF', 'NU_PRESENTES_5EF', 'TAXA_PARTICIPACAO_5EF', 'NU_MATRICULADOS_CENSO_9EF', 'NU_PRESENTES_9EF', 'TAXA_PARTICIPACAO_9EF', 'NU_MATRICULADOS_CENSO_3EM', 'NU_PRESENTES_3EM', 'TAXA_PARTICIPACAO_3EM', 'conector_LP', 'conector_MT']
df = df.select(*ordered_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.repartition(10).write.partitionBy('ID_PROVA_BRASIL').parquet(target, mode='overwrite')