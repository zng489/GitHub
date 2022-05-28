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
# MAGIC Tabela/Arquivo Origem	/trs/inep_enem/microdados_enem / /trs/mtd/corp/base_escolas
# MAGIC Tabela/Arquivo Destino	/biz/uniepro/base_metas_enem_2017
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
#   "year": 2017,
#   "path_origin": "inep_enem/microdados_enem",
#   "path_base_school": "mtd/corp/base_escolas",
#   "path_classes": "inep_enem/turmas",
#   "path_classes_max_scores": "inep_enem/nota_maxima_turma",
#   "path_ramp": "inep_enem/parametros_rampa_projecao",
#   "path_destination": "uniepro/fta_base_metas_enem",
#   "destination": "/uniepro/fta_base_metas_enem",
#   "databricks": {
#     "notebook": "/biz/inep_enem/trs_biz_fta_metas_enem"
#   }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/trs","business":"/biz"}}
# # dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

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

source_classes = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_classes"])
source_classes

# COMMAND ----------

source_classes_max_scores = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_classes_max_scores"])
source_classes_max_scores

# COMMAND ----------

source_ramp = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_ramp"])
source_ramp

# COMMAND ----------

source_base_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_base_school"])
source_base_school

# COMMAND ----------

biz_target = "{biz}/{origin}".format(biz=biz, origin=table["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source = df_source.where((f.col('NR_ANO') == f.lit(table['year'])) &
                            (f.col('TP_ST_CONCLUSAO') == f.lit(2)) &
                            (f.col('CD_ENTIDADE') > f.lit(0)) &
                            (f.col('FL_TREINEIRO') == f.lit(0)) &
                            (f.col('TP_ENSINO') == f.lit(1)) &
                            (f.col('TP_PRESENCA_CN') == f.lit(1)) &
                            (f.col('TP_PRESENCA_CH') == f.lit(1)) &
                            (f.col('TP_PRESENCA_LC') == f.lit(1)) &
                            (f.col('TP_PRESENCA_MT') == f.lit(1)) &
                            (f.col('NR_NOTA_CN') > f.lit(0)) &
                            (f.col('NR_NOTA_CH') > f.lit(0)) &
                            (f.col('NR_NOTA_LC') > f.lit(0)) &
                            (f.col('NR_NOTA_MT') > f.lit(0)) &
                            (f.col('TP_STATUS_REDACAO') == f.lit(1)) &
                            (f.col('NR_NOTA_REDACAO') > f.lit(0)))

# COMMAND ----------

ethnicity = ((f.col('CD_UF_ESC') * f.lit(10000)) +
             ((f.col('TP_SEXO') == f.lit('F')).cast('Int') * f.lit(1000)) +
             ((f.col('NR_IDADE') > f.lit(18)).cast('Int') * f.lit(100)) +
             ((f.when(f.col('TP_COR_RACA').isin([0, 6]), f.lit(0))
                         .otherwise(f.when(f.col('TP_COR_RACA') == f.lit(1), f.lit(1))
                                     .otherwise(f.when(f.col('TP_COR_RACA').isin([2, 3, 5]), f.lit(2))
                                                 .otherwise(f.when(f.col('TP_COR_RACA') == f.lit(4), f.lit(3)))))) * f.lit(10)) + 
             f.when(f.col('DS_ESCOLAR_MAE').isin(['A', 'B', 'C']), f.lit(1))
                         .otherwise(f.when(f.col('DS_ESCOLAR_MAE') == f.lit('D'), f.lit(2))
                                     .otherwise(f.when(f.col('DS_ESCOLAR_MAE') == f.lit('E'), f.lit(3))
                                                 .otherwise(f.when(f.col('DS_ESCOLAR_MAE').isin(['F', 'G']), f.lit(4))
                                                             .otherwise(f.when(f.col('DS_ESCOLAR_MAE') == f.lit('H'), f.lit(0)))))))

# COMMAND ----------

df_source = (df_source
             .select(
               f.lit(table['year']).alias('NR_ANO_BASE_META'),
               f.col('CD_ENTIDADE'),
               ethnicity.alias('CD_PERFIL'),
               f.when((f.col('CD_UF_ESC').isNull()) |
                      (f.col('TP_SEXO').isNull()) |
                      (f.col('NR_IDADE').isNull()) |
                      (f.col('TP_COR_RACA').isNull()) |
                      (f.col('DS_ESCOLAR_MAE').isNull()), f.lit(1))
                .otherwise(f.lit(0)).alias('FL_EXCLUI_PERFIL'),
               ((f.col('NR_NOTA_CN') + f.col('NR_NOTA_CH') + f.col('NR_NOTA_LC') + f.col('NR_NOTA_MT') + f.col('NR_NOTA_REDACAO')) / f.lit(5)).alias('VL_NU_NOTA_GERAL_REDAC')))

# COMMAND ----------

df_base_school = spark.read.parquet(source_base_school)
df_base_school = (df_base_school
                  .where((f.col('fl_escola_senai') == f.lit(1)) | 
                         (f.col('fl_escola_sesi') == f.lit(1)) | 
                         (f.col('fl_escola_sesi_senai') == f.lit(1)))
                  .select('cd_entidade'))

# COMMAND ----------

df_classes = spark.read.parquet(source_classes)
df_classes = (df_classes
              .where(f.col('NU_ANO_CENSO') == f.lit(table['year']))
              .select(
                f.col('co_entidade').alias('CD_ENTIDADE'),
                f.when(f.col('MATRIC_CONC').isNull(), f.lit(1)).otherwise(0).alias('FL_ESCOLA_FORA')))

# COMMAND ----------

df_max_scores = spark.read.parquet(source_classes_max_scores)
df_max_scores = (df_max_scores
                 .where(f.col('NR_ANO') == f.lit(table['year']))
                 .select('CD_PERFIL', f.col('VL_NOTA').alias('VL_NOTA_MEDIA_MELHORES_5PCT_COM_PERFIL')))

# COMMAND ----------

df = df_source.join(df_base_school, on='CD_ENTIDADE', how='inner')
df = df.join(df_classes, on='CD_ENTIDADE', how='left')
df = df.join(df_max_scores, on='CD_PERFIL', how='left')

# COMMAND ----------

df_ramp = spark.read.parquet(source_ramp)
df_ramp = df_ramp.where(f.col('NR_ANO_BASE') == f.lit(table['year']))

df_ramp = df_ramp.groupby('NR_ANO_BASE').pivot('NR_ANO_PROJECAO').agg(f.first(f.col('PC_PROJECAO')))
df_ramp_cols = (f.col(col).alias(renamed) for col, renamed in zip(df_ramp.columns[1:], map(lambda col: 'VL_META_' + col, df_ramp.columns[1:])))

df_ramp = df_ramp.select(*df_ramp_cols)
df = df.crossJoin(df_ramp)

for col in df_ramp.columns:
  df = df.withColumn(col, f.col('VL_NOTA_MEDIA_MELHORES_5PCT_COM_PERFIL') * f.col(col) / f.lit(100))

# COMMAND ----------

reorder_columns = ['NR_ANO_BASE_META', 'CD_ENTIDADE', 'FL_ESCOLA_FORA', 'CD_PERFIL', 'FL_EXCLUI_PERFIL'] + df_ramp.columns + ['VL_NOTA_MEDIA_MELHORES_5PCT_COM_PERFIL', 'VL_NU_NOTA_GERAL_REDAC']
df = df.select(*reorder_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.coalesce(1).write.parquet(target, mode='overwrite')