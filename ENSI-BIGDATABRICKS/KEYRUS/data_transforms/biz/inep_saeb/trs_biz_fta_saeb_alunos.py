# Databricks notebook source
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
#   "path_origin": "inep_saeb/prova_brasil_saeb_aluno_unificado",
#   "path_competence": "inep_saeb/competencias_habilidades",
#   "path_mask": "inep_saeb/mascara_escola_sesi",
#   "path_base_school": "mtd/corp/base_escolas",
#   "path_unified_school": "inep_saeb/escola_unificada",
#   "path_destination": "uniepro/fta_saeb_alunos",
#   "destination": "/uniepro/fta_saeb_alunos",
#   "databricks": {
#     "notebook": "/biz/inep_saeb/trs_biz_fta_saeb_alunos"
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

trs = dls['folders']['trusted']
biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_origin"])
source

# COMMAND ----------

source_competence = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_competence"])
source_competence

# COMMAND ----------

source_mask = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_mask"])
source_mask

# COMMAND ----------

source_base_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_base_school"])
source_base_school

# COMMAND ----------

source_unified_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_unified_school"])
source_unified_school

# COMMAND ----------

biz_target = "{biz}/{origin}".format(biz=biz, origin=table["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source_columns = ['ID_PROVA_BRASIL', 'ID_REGIAO', 'ID_UF', 'ID_MUNICIPIO', 'ID_AREA', 'ID_DEPENDENCIA_ADM', 'ID_LOCALIZACAO', 'ID_TURMA', 'ID_TURNO', 'ID_SERIE', 'ID_ALUNO', 'ID_CADERNO', 'ID_BLOCO_1', 'ID_BLOCO_2', 'TX_RESP_BLOCO_1_LP', 'TX_RESP_BLOCO_2_LP', 'TX_RESP_BLOCO_1_MT', 'TX_RESP_BLOCO_2_MT']

df_source_renamed_columns = [('IN_SITUACAO_CENSO', 'FL_SITUACAO_CENSO'), ('IN_PREENCHIMENTO_PROVA', 'FL_PREENCHIMENTO_PROVA'), ('IN_PRESENCA_PROVA', 'FL_PRESENCA_PROVA'), ('IN_PROFICIENCIA', 'FL_PROFICIENCIA'), ('IN_PROVA_BRASIL', 'FL_PROVA_BRASIL'), ('ESTRATO_ANEB', 'DS_ESTRATO_ANEB'), ('PESO_ALUNO_LP', 'VL_PESO_ALUNO_LP'), ('PESO_ALUNO_MT', 'VL_PESO_ALUNO_MT'), ('PROFICIENCIA_LP', 'VL_PROFICIENCIA_LP'), ('ERRO_PADRAO_LP', 'VL_ERRO_PADRAO_LP'), ('PROFICIENCIA_LP_SAEB', 'VL_PROFICIENCIA_LP_SAEB'), ('ERRO_PADRAO_LP_SAEB', 'VL_ERRO_PADRAO_LP_SAEB'), ('PROFICIENCIA_MT', 'VL_PROFICIENCIA_MT'), ('ERRO_PADRAO_MT', 'VL_ERRO_PADRAO_MT'), ('PROFICIENCIA_MT_SAEB', 'VL_PROFICIENCIA_MT_SAEB'), ('ERRO_PADRAO_MT_SAEB', 'VL_ERRO_PADRAO_MT_SAEB'), ('IN_PREENCHIMENTO_QUESTIONARIO', 'FL_PREENCHIMENTO_QUESTIONARIO'), ('TX_RESP_Q001', 'VL_TX_RESP_Q001'), ('TX_RESP_Q002', 'VL_TX_RESP_Q002'), ('TX_RESP_Q003', 'VL_TX_RESP_Q003'), ('TX_RESP_Q004', 'VL_TX_RESP_Q004'), ('TX_RESP_Q005', 'VL_TX_RESP_Q005'), ('TX_RESP_Q006', 'VL_TX_RESP_Q006'), ('TX_RESP_Q007', 'VL_TX_RESP_Q007'), ('TX_RESP_Q008', 'VL_TX_RESP_Q008'), ('TX_RESP_Q009', 'VL_TX_RESP_Q009'), ('TX_RESP_Q010', 'VL_TX_RESP_Q010'), ('TX_RESP_Q011', 'VL_TX_RESP_Q011'), ('TX_RESP_Q012', 'VL_TX_RESP_Q012'), ('TX_RESP_Q013', 'VL_TX_RESP_Q013'), ('TX_RESP_Q014', 'VL_TX_RESP_Q014'), ('TX_RESP_Q015', 'VL_TX_RESP_Q015'), ('TX_RESP_Q016', 'VL_TX_RESP_Q016'), ('TX_RESP_Q017', 'VL_TX_RESP_Q017'), ('TX_RESP_Q018', 'VL_TX_RESP_Q018'), ('TX_RESP_Q019', 'VL_TX_RESP_Q019'), ('TX_RESP_Q020', 'VL_TX_RESP_Q020'), ('TX_RESP_Q021', 'VL_TX_RESP_Q021'), ('TX_RESP_Q022', 'VL_TX_RESP_Q022'), ('TX_RESP_Q023', 'VL_TX_RESP_Q023'), ('TX_RESP_Q024', 'VL_TX_RESP_Q024'), ('TX_RESP_Q025', 'VL_TX_RESP_Q025'), ('TX_RESP_Q026', 'VL_TX_RESP_Q026'), ('TX_RESP_Q027', 'VL_TX_RESP_Q027'), ('TX_RESP_Q028', 'VL_TX_RESP_Q028'), ('TX_RESP_Q029', 'VL_TX_RESP_Q029'), ('TX_RESP_Q030', 'VL_TX_RESP_Q030'), ('TX_RESP_Q031', 'VL_TX_RESP_Q031'), ('TX_RESP_Q032', 'VL_TX_RESP_Q032'), ('TX_RESP_Q033', 'VL_TX_RESP_Q033'), ('TX_RESP_Q034', 'VL_TX_RESP_Q034'), ('TX_RESP_Q035', 'VL_TX_RESP_Q035'), ('TX_RESP_Q036', 'VL_TX_RESP_Q036'), ('TX_RESP_Q037', 'VL_TX_RESP_Q037'), ('TX_RESP_Q038', 'VL_TX_RESP_Q038'), ('TX_RESP_Q039', 'VL_TX_RESP_Q039'), ('TX_RESP_Q040', 'VL_TX_RESP_Q040_3EM'), ('TX_RESP_Q041', 'VL_TX_RESP_Q041'), ('TX_RESP_Q042', 'VL_TX_RESP_Q042'), ('TX_RESP_Q043', 'VL_TX_RESP_Q043_3EM'), ('TX_RESP_Q044', 'VL_TX_RESP_Q044'), ('TX_RESP_Q045', 'VL_TX_RESP_Q045'), ('TX_RESP_Q046', 'VL_TX_RESP_Q046_3EM'), ('TX_RESP_Q047', 'VL_TX_RESP_Q047_3EM'), ('TX_RESP_Q048', 'VL_TX_RESP_Q048_3EM'), ('TX_RESP_Q049', 'VL_TX_RESP_Q049_3EM'), ('TX_RESP_Q050', 'VL_TX_RESP_Q050_3EM'), ('TX_RESP_Q051', 'VL_TX_RESP_Q051'), ('TX_RESP_Q052', 'VL_TX_RESP_Q052'), ('TX_RESP_Q053', 'VL_TX_RESP_Q053'), ('TX_RESP_Q054', 'VL_TX_RESP_Q054_3EM'), ('TX_RESP_Q055', 'VL_TX_RESP_Q055_3EM'), ('TX_RESP_Q056', 'VL_TX_RESP_Q056_3EM'), ('TX_RESP_Q057', 'VL_TX_RESP_Q057_3EM'), ('TX_RESP_Q058', 'VL_TX_RESP_Q058_3EM'), ('TX_RESP_Q059', 'VL_TX_RESP_Q059'), ('TX_RESP_Q060', 'VL_TX_RESP_Q060'), ('TP_ARQUIVO', 'DS_BASE'), ('TX_RESP_Q004_5', 'VL_TX_RESP_Q004_5EF'), ('TX_RESP_Q034_9', 'VL_TX_RESP_Q034_9EF'), ('TX_RESP_Q035a', 'VL_TX_RESP_Q035_9EF'), ('TX_RESP_Q037_9', 'VL_TX_RESP_Q037_9EF'), ('TX_RESP_Q042_9', 'VL_TX_RESP_Q042_9EF'), ('TX_RESP_Q044_59', 'VL_TX_RESP_Q044_5EF'), ('TX_RESP_Q047_59', 'VL_TX_RESP_Q047_9EF'), ('TX_RESP_Q057_9', 'VL_TX_RESP_Q057_9EF'), ('TX_RESP_Q032_59', 'VL_TX_RESP_Q092'), ('TX_RESP_Q037_59', 'VL_TX_RESP_Q097'), ('TX_RESP_Q038_59a', 'VL_TX_RESP_Q098'), ('TX_RESP_Q039_59', 'VL_TX_RESP_Q099'), ('ID_ESCOLA', 'CD_ENTIDADE')]
df_source_renamed_columns = (f.col(org).alias(dst) for org, dst in df_source_renamed_columns)

df_source = df_source.select(*df_source_columns, *df_source_renamed_columns, f.col('ID_ESCOLA').alias('ID_ESCOLA_ORIGINAL'))

# COMMAND ----------

df_mask = spark.read.parquet(source_mask)

df_mask_columns = [('CD_ENTIDADE', 'ID_ESCOLASEMMASCARA'), ('cd_entidade_mascara', 'ID_ESCOLA_ORIGINAL'), ('nr_ano', 'ID_PROVA_BRASIL')]
df_mask_columns = (f.col(org).alias(dst) for org, dst in df_mask_columns)

df_mask = df_mask.select(*df_mask_columns)

# COMMAND ----------

df_base_school = spark.read.parquet(source_base_school)
df_base_school = df_base_school.select('CD_ENTIDADE',
                                       'NM_ENTIDADE', 
                                       f.when((f.coalesce(f.col('FL_ESCOLA_SENAI'), f.lit(0)) + 
                                               f.coalesce(f.col('FL_ESCOLA_SESI'), f.lit(0)) + 
                                               f.coalesce(f.col('FL_ESCOLA_SESI_SENAI'), f.lit(0))) > 0, f.lit(1))
                                       .otherwise(f.lit(0)).alias('FL_ESCOLA_SESI'))

# COMMAND ----------

df_unified_school = spark.read.parquet(source_unified_school)

df_unified_school_columns = ['PC_FORMACAO_DOCENTE_INICIAL', 'PC_FORMACAO_DOCENTE_FINAL', 'PC_FORMACAO_DOCENTE_MEDIO', 'NR_MATRICULADOS_CENSO_5EF', 'NR_PRESENTES_5EF', 'TX_PARTICIPACAO_5EF', 'NR_MATRICULADOS_CENSO_9EF', 'NR_PRESENTES_9EF', 'TX_PARTICIPACAO_9EF', 'NR_MATRICULADOS_CENSO_3EM', 'NR_PRESENTES_3EM', 'TX_PARTICIPACAO_3EM', 'ID_PROVA_BRASIL']

df_unified_school_renamed_columns = [('NR_NIVEL_SOCIO_ECONOMICO', 'NIVEL_SOCIO_ECONOMICO'), ('ID_ESCOLA', 'CD_ENTIDADE')]
df_unified_school_renamed_columns = (f.col(org).alias(dst) for org, dst in df_unified_school_renamed_columns)

df_unified_school = df_unified_school.select(*df_unified_school_columns, *df_unified_school_renamed_columns)

# COMMAND ----------

df_competence = spark.read.parquet(source_competence)
df_competence = (df_competence
                 .withColumn('ds_intervalor_proficiencia', f.split(f.col('ds_intervalor_proficiencia'), '-').cast('array<int>'))
                 .select(f.col('NR_ANO').alias('ID_PROVA_BRASIL'), f.col('NR_SERIE').alias('ID_SERIE'), 'SG_DISCIPLINA', 'DS_INTERVALOR_PROFICIENCIA', 'VL_NIVEL', 'DS_BASE'))
df_competence = df_competence.distinct()
df_competence = (df_competence
                 .groupby('ID_PROVA_BRASIL', 'ID_SERIE', 'DS_BASE', 'VL_NIVEL')
                 .pivot('SG_DISCIPLINA')
                 .agg(f.first('DS_INTERVALOR_PROFICIENCIA')))

# COMMAND ----------

def join_competence(df, df_competence, _type):
  column = 'VL_PROFICIENCIA_%s_SAEB' % _type
  
  expr = (
    (df['ID_SERIE'] == df_competence['ID_SERIE']) &
    (df['DS_BASE'] == df_competence['DS_BASE']) &
    ((df[column] > df_competence[_type].getItem(0)) & (df[column] <= df_competence[_type].getItem(1)))
  )

  df_columns = map(lambda col: 'l.{col}'.format(col=col), df.columns)
  df = df.alias('l').join(df_competence.alias('r'), on=expr, how='left')
  
  new_column = 'VL_NIVEL_%s' % _type
  df = df.withColumn(new_column, f.when(((f.col(column) > f.col(_type).getItem(0)) & 
                                            (f.col(column) <= f.col(_type).getItem(1))), f.col('VL_NIVEL')))
  
  return df.select(*df_columns, new_column)

# COMMAND ----------

df = df_source.alias('l').join(df_mask.alias('r'), on=['ID_PROVA_BRASIL', 'ID_ESCOLA_ORIGINAL'], how='left')
df = df.withColumn('CD_ENTIDADE', f.when(f.col('l.CD_ENTIDADE') != f.col('r.ID_ESCOLASEMMASCARA'), f.col('r.ID_ESCOLASEMMASCARA')).otherwise(f.col('l.CD_ENTIDADE')))
df = df.join(df_unified_school, on=['ID_PROVA_BRASIL', 'CD_ENTIDADE'], how='left')
df = df.join(df_base_school, on=['CD_ENTIDADE'], how='left')
df = join_competence(df, df_competence, _type='LP')
df = join_competence(df, df_competence, _type='MT')

# COMMAND ----------

df = (df
      .withColumn('ID_MASCARA', f.lit(0))
      .withColumn('DS_CONECTOR_LP', f.concat_ws('', f.lit('LP'), f.col('VL_NIVEL_LP'), f.col('DS_BASE')))
      .withColumn('DS_CONECTOR_MT', f.concat_ws('', f.lit('MT'), f.col('VL_NIVEL_MT'), f.col('DS_BASE')))
      .withColumn('FL_ESCOLA_SESI', f.when(f.col('FL_ESCOLA_SESI').isNull(), f.lit(0)).otherwise(f.col('FL_ESCOLA_SESI'))))

# COMMAND ----------

ordered_columns = ['ID_PROVA_BRASIL', 'ID_REGIAO', 'ID_UF', 'ID_MUNICIPIO', 'ID_AREA', 'ID_DEPENDENCIA_ADM', 'ID_LOCALIZACAO', 'ID_TURMA', 'ID_TURNO', 'ID_SERIE', 'ID_ALUNO', 'FL_SITUACAO_CENSO', 'FL_PREENCHIMENTO_PROVA', 'FL_PRESENCA_PROVA', 'ID_CADERNO', 'ID_BLOCO_1', 'ID_BLOCO_2', 'TX_RESP_BLOCO_1_LP', 'TX_RESP_BLOCO_2_LP', 'TX_RESP_BLOCO_1_MT', 'TX_RESP_BLOCO_2_MT', 'FL_PROFICIENCIA', 'FL_PROVA_BRASIL', 'DS_ESTRATO_ANEB', 'VL_PESO_ALUNO_LP', 'VL_PESO_ALUNO_MT', 'VL_PROFICIENCIA_LP', 'VL_ERRO_PADRAO_LP', 'VL_PROFICIENCIA_LP_SAEB', 'VL_ERRO_PADRAO_LP_SAEB', 'VL_PROFICIENCIA_MT', 'VL_ERRO_PADRAO_MT', 'VL_PROFICIENCIA_MT_SAEB', 'VL_ERRO_PADRAO_MT_SAEB', 'FL_PREENCHIMENTO_QUESTIONARIO', 'VL_TX_RESP_Q001', 'VL_TX_RESP_Q002', 'VL_TX_RESP_Q003', 'VL_TX_RESP_Q004', 'VL_TX_RESP_Q005', 'VL_TX_RESP_Q006', 'VL_TX_RESP_Q007', 'VL_TX_RESP_Q008', 'VL_TX_RESP_Q009', 'VL_TX_RESP_Q010', 'VL_TX_RESP_Q011', 'VL_TX_RESP_Q012', 'VL_TX_RESP_Q013', 'VL_TX_RESP_Q014', 'VL_TX_RESP_Q015', 'VL_TX_RESP_Q016', 'VL_TX_RESP_Q017', 'VL_TX_RESP_Q018', 'VL_TX_RESP_Q019', 'VL_TX_RESP_Q020', 'VL_TX_RESP_Q021', 'VL_TX_RESP_Q022', 'VL_TX_RESP_Q023', 'VL_TX_RESP_Q024', 'VL_TX_RESP_Q025', 'VL_TX_RESP_Q026', 'VL_TX_RESP_Q027', 'VL_TX_RESP_Q028', 'VL_TX_RESP_Q029', 'VL_TX_RESP_Q030', 'VL_TX_RESP_Q031', 'VL_TX_RESP_Q032', 'VL_TX_RESP_Q033', 'VL_TX_RESP_Q034', 'VL_TX_RESP_Q035', 'VL_TX_RESP_Q036', 'VL_TX_RESP_Q037', 'VL_TX_RESP_Q038', 'VL_TX_RESP_Q039', 'VL_TX_RESP_Q040_3EM', 'VL_TX_RESP_Q041', 'VL_TX_RESP_Q042', 'VL_TX_RESP_Q043_3EM', 'VL_TX_RESP_Q044', 'VL_TX_RESP_Q045', 'VL_TX_RESP_Q046_3EM', 'VL_TX_RESP_Q047_3EM', 'VL_TX_RESP_Q048_3EM', 'VL_TX_RESP_Q049_3EM', 'VL_TX_RESP_Q050_3EM', 'VL_TX_RESP_Q051', 'VL_TX_RESP_Q052', 'VL_TX_RESP_Q053', 'VL_TX_RESP_Q054_3EM', 'VL_TX_RESP_Q055_3EM', 'VL_TX_RESP_Q056_3EM', 'VL_TX_RESP_Q057_3EM', 'VL_TX_RESP_Q058_3EM', 'VL_TX_RESP_Q059', 'VL_TX_RESP_Q060', 'DS_BASE', 'VL_NIVEL_LP', 'VL_NIVEL_MT', 'VL_TX_RESP_Q004_5EF', 'VL_TX_RESP_Q034_9EF', 'VL_TX_RESP_Q035_9EF', 'VL_TX_RESP_Q037_9EF', 'VL_TX_RESP_Q042_9EF', 'VL_TX_RESP_Q044_5EF', 'VL_TX_RESP_Q047_9EF', 'VL_TX_RESP_Q057_9EF', 'VL_TX_RESP_Q092', 'VL_TX_RESP_Q097', 'VL_TX_RESP_Q098', 'VL_TX_RESP_Q099', 'CD_ENTIDADE', 'ID_ESCOLA_ORIGINAL', 'ID_ESCOLASEMMASCARA', 'NM_ENTIDADE', 'FL_ESCOLA_SESI', 'ID_MASCARA', 'PC_FORMACAO_DOCENTE_INICIAL', 'PC_FORMACAO_DOCENTE_FINAL', 'PC_FORMACAO_DOCENTE_MEDIO', 'NIVEL_SOCIO_ECONOMICO', 'NR_MATRICULADOS_CENSO_5EF', 'NR_PRESENTES_5EF', 'TX_PARTICIPACAO_5EF', 'NR_MATRICULADOS_CENSO_9EF', 'NR_PRESENTES_9EF', 'TX_PARTICIPACAO_9EF', 'NR_MATRICULADOS_CENSO_3EM', 'NR_PRESENTES_3EM', 'TX_PARTICIPACAO_3EM', 'DS_CONECTOR_LP', 'DS_CONECTOR_MT']

df = df.select(*ordered_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.repartition(10).write.partitionBy('ID_PROVA_BRASIL').parquet(target, mode='overwrite')