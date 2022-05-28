# Databricks notebook source
from pyspark.sql.window import Window

import cni_connectors.adls_gen1_connector as connector
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'path_origin': 'crw/inep_enem/microdados_enem/',
#   'path_destination': 'inep_enem/nota_maxima_turma'
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_elegiveis_16_18',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)
df = df.where((f.col('NU_ANO') > f.lit(2015)) &
              (f.col('TP_ST_CONCLUSAO') == f.lit(2)) &
              (f.col('CO_ENTIDADE') > f.lit(0)) &
              (f.col('IN_TREINEIRO') == f.lit(0)) &
              (f.col('TP_ENSINO') == f.lit(1)) &
              (f.col('TP_PRESENCA_CN') == f.lit(1)) &
              (f.col('TP_PRESENCA_CH') == f.lit(1)) &
              (f.col('TP_PRESENCA_LC') == f.lit(1)) &
              (f.col('TP_PRESENCA_MT') == f.lit(1)) &
              (f.col('NU_NOTA_CN') > f.lit(0)) &
              (f.col('NU_NOTA_CH') > f.lit(0)) &
              (f.col('NU_NOTA_LC') > f.lit(0)) &
              (f.col('NU_NOTA_MT') > f.lit(0)) &
              (f.col('TP_STATUS_REDACAO') == f.lit(1)) &
              (f.col('NU_NOTA_REDACAO') > f.lit(0)) &
              (f.col('CO_UF_ESC').isNotNull()) &
              (f.col('TP_SEXO').isNotNull()) &
              (f.col('NU_IDADE').isNotNull()) &
              (f.col('TP_COR_RACA').isNotNull()) &
              (f.col('ESCOLAR_MAE').isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### To create CD_PERFIL column
# MAGIC <pre>
# MAGIC RECODE TP_COR_RACA (0=0) (1=1) (2=2) (3=2) (4=3) (5=2) (6=0) INTO COR.
# MAGIC EXECUTE.
# MAGIC VARIABLE LABELS COR  'COR DO ALUNO'.
# MAGIC VALUE LABELS COR  0 'Não informado' 1 'Branco'  2 'PPI'  3 'Amarelo'.
# MAGIC 
# MAGIC RECODE Q002  ('A'=1) ('B'=1) ('C'=1) ('D'=2) ('E'=3) ('F'=4) ('G'=4) ('H'=0) INTO ESCOLAR_MAE.
# MAGIC EXECUTE.
# MAGIC VARIABLE LABELS ESCOLAR_MAE  'ESCOLARIDADE DA MÃE'.
# MAGIC VALUE LABELS ESCOLAR_MAE  1 'EF incompleto'  2 'EF completo'  3 'EM completo'  4 'Faculdade completa'   0 'Não sabe'.
# MAGIC </pre>

# COMMAND ----------

ethnicity = ((f.col('CO_UF_ESC') * f.lit(10000)) +
             ((f.col('TP_SEXO') == f.lit('F')).cast('Int') * f.lit(1000)) +
             ((f.col('NU_IDADE') > f.lit(18)).cast('Int') * f.lit(100)) +
             ((f.when(f.col('TP_COR_RACA').isin([0, 6]), f.lit(0))
                         .otherwise(f.when(f.col('TP_COR_RACA') == f.lit(1), f.lit(1))
                                     .otherwise(f.when(f.col('TP_COR_RACA').isin([2, 3, 5]), f.lit(2))
                                                 .otherwise(f.when(f.col('TP_COR_RACA') == f.lit(4), f.lit(3)))))) * f.lit(10)) + 
             f.when(f.col('ESCOLAR_MAE').isin(['A', 'B', 'C']), f.lit(1))
                         .otherwise(f.when(f.col('ESCOLAR_MAE') == f.lit('D'), f.lit(2))
                                     .otherwise(f.when(f.col('ESCOLAR_MAE') == f.lit('E'), f.lit(3))
                                                 .otherwise(f.when(f.col('ESCOLAR_MAE').isin(['F', 'G']), f.lit(4))
                                                             .otherwise(f.when(f.col('ESCOLAR_MAE') == f.lit('H'), f.lit(0)))))))

# COMMAND ----------

df = df.select(f.col('NU_ANO').alias('NR_ANO'), 
               ethnicity.alias('CD_PERFIL'),
              ((f.col('NU_NOTA_CN') + f.col('NU_NOTA_CH') + f.col('NU_NOTA_LC') + f.col('NU_NOTA_MT') + f.col('NU_NOTA_REDACAO')) / f.lit(5)).alias('VL_NOTA'))

# COMMAND ----------

w = Window.partitionBy('NR_ANO', 'CD_PERFIL')
df = df.withColumn('TOTAL_ALUNOS_PERFIL', f.count(f.lit(1)).over(w))

w = Window.partitionBy('NR_ANO', 'CD_PERFIL').orderBy(f.col('VL_NOTA').desc())
df = df.withColumn('rank', f.row_number().over(w))
df = df.withColumn('top_5_profile', f.col('TOTAL_ALUNOS_PERFIL') * f.lit(0.05))
df = df.withColumn('use_to_agg', f.when((f.col('top_5_profile') >= f.lit(1)) & (f.col('rank') <= f.col('top_5_profile')), f.lit(1))
                                  .otherwise(f.when(f.col('rank') == f.lit(1), f.lit(1))))

df_agg = (df
          .where(f.col('use_to_agg') == f.lit(1))
          .groupby('NR_ANO', 'CD_PERFIL')
          .agg(f.avg('VL_NOTA').alias('VL_NOTA')))

# COMMAND ----------

df_agg = df_agg.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df_agg.coalesce(1).write.parquet(target, mode='overwrite')