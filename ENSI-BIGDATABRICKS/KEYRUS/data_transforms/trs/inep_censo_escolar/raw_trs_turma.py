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
# MAGIC Processo raw_trs_etapa_ensino
# MAGIC Tabela/Arquivo Origem /raw/crw/inep_matricula/
# MAGIC Tabela/Arquivo Destino /trs/uniepro/matricula/
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino etapa de ensino forma uma estrutura de códigos que visa orientar, organizar e consolidar 
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga raw 

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
from pyspark.sql.functions import *

import re
import json

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

# USE THIS ONLY FOR DEVELOPMENT PURPOSES
# tables =  {
#   "path_origin": "crw/inep_censo_escolar/turmas/",
#   "path_destination": "inep_censo_escolar/censo_escolar_turmas"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
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

df_source = spark.read.parquet(source)

# COMMAND ----------

transformation = [('NU_ANO_CENSO', 'NR_ANO_CENSO', 'Int'), ('ID_TURMA', 'ID_TURMA', 'Int'), ('NO_TURMA', 'NR_TURMA', 'String'), ('TP_MEDIACAO_DIDATICO_PEDAGO', 'TP_MEDIACAO_DIDATICO_PEDAGO', 'Int'), ('TX_HR_INICIAL', 'HR_INICIAL', 'String'), ('TX_MI_INICIAL', 'MI_INICIAL', 'String'), ('IN_DIA_SEMANA_DOMINGO', 'FL_DIA_SEMANA_DOMINGO', 'Int'), ('IN_DIA_SEMANA_SEGUNDA', 'FL_DIA_SEMANA_SEGUNDA', 'Int'), ('IN_DIA_SEMANA_TERCA', 'FL_DIA_SEMANA_TERCA', 'Int'), ('IN_DIA_SEMANA_QUARTA', 'FL_DIA_SEMANA_QUARTA', 'Int'), ('IN_DIA_SEMANA_QUINTA', 'FL_DIA_SEMANA_QUINTA', 'Int'), ('IN_DIA_SEMANA_SEXTA', 'FL_DIA_SEMANA_SEXTA', 'Int'), ('IN_DIA_SEMANA_SABADO', 'FL_DIA_SEMANA_SABADO', 'Int'), ('NU_DIAS_ATIVIDADE', 'NU_DIAS_ATIVIDADE', 'Int'), ('NU_DURACAO_TURMA', 'NU_DURACAO_TURMA', 'Int'), ('TP_TIPO_TURMA', 'TP_TIPO_TURMA', 'Int'), ('TP_TIPO_ATENDIMENTO_TURMA', 'TP_TIPO_ATENDIMENTO_TURMA', 'Int'), ('TP_TIPO_LOCAL_TURMA', 'TP_TIPO_LOCAL_TURMA', 'Int'), ('IN_MAIS_EDUCACAO', 'FL_MAIS_EDUCACAO', 'Int'), ('CO_TIPO_ATIVIDADE_1', 'CD_TIPO_ATIVIDADE_1', 'Int'), ('CO_TIPO_ATIVIDADE_2', 'CD_TIPO_ATIVIDADE_2', 'Int'), ('CO_TIPO_ATIVIDADE_3', 'CD_TIPO_ATIVIDADE_3', 'Int'), ('CO_TIPO_ATIVIDADE_4', 'CD_TIPO_ATIVIDADE_4', 'Int'), ('CO_TIPO_ATIVIDADE_5', 'CD_TIPO_ATIVIDADE_5', 'Int'), ('CO_TIPO_ATIVIDADE_6', 'CD_TIPO_ATIVIDADE_6', 'Int'), ('TP_ETAPA_ENSINO', 'TP_ETAPA_ENSINO', 'Int'), ('CO_CURSO_EDUC_PROFISSIONAL', 'CD_CURSO_EDUC_PROFISSIONAL', 'Int'), ('IN_ESPECIAL_EXCLUSIVA', 'FL_ESPECIAL_EXCLUSIVA', 'Int'), ('IN_REGULAR', 'FL_REGULAR', 'Int'), ('IN_EJA', 'FL_EJA', 'Int'), ('IN_PROFISSIONALIZANTE', 'FL_PROFISSIONALIZANTE', 'Int'), ('QT_MATRICULAS', 'QT_MATRICULAS', 'Int'), ('IN_BRAILLE', 'FL_BRAILLE', 'Int'), ('IN_RECURSOS_BAIXA_VISAO', 'FL_RECURSOS_BAIXA_VISAO', 'Int'), ('IN_PROCESSOS_MENTAIS', 'FL_PROCESSOS_MENTAIS', 'Int'), ('IN_ORIENTACAO_MOBILIDADE', 'FL_ORIENTACAO_MOBILIDADE', 'Int'), ('IN_SINAIS', 'FL_SINAIS', 'Int'), ('IN_COMUNICACAO_ALT_AUMENT', 'FL_COMUNICACAO_ALT_AUMENT', 'Int'), ('IN_ENRIQ_CURRICULAR', 'FL_ENRIQ_CURRICULAR', 'Int'), ('IN_SOROBAN', 'FL_SOROBAN', 'Int'), ('IN_INFORMATICA_ACESSIVEL', 'FL_INFORMATICA_ACESSIVEL', 'Int'), ('IN_PORT_ESCRITA', 'FL_PORT_ESCRITA', 'Int'), ('IN_AUTONOMIA_ESCOLAR', 'FL_AUTONOMIA_ESCOLAR', 'Int'), ('IN_DISC_LINGUA_PORTUGUESA', 'FL_DISC_LINGUA_PORTUGUESA', 'Int'), ('IN_DISC_EDUCACAO_FISICA', 'FL_DISC_EDUCACAO_FISICA', 'Int'), ('IN_DISC_ARTES', 'FL_DISC_ARTES', 'Int'), ('IN_DISC_LINGUA_INGLES', 'FL_DISC_LINGUA_INGLES', 'Int'), ('IN_DISC_LINGUA_ESPANHOL', 'FL_DISC_LINGUA_ESPANHOL', 'Int'), ('IN_DISC_LINGUA_FRANCES', 'FL_DISC_LINGUA_FRANCES', 'Int'), ('IN_DISC_LINGUA_OUTRA', 'FL_DISC_LINGUA_OUTRA', 'Int'), ('IN_DISC_LIBRAS', 'FL_DISC_LIBRAS', 'Int'), ('IN_DISC_LINGUA_INDIGENA', 'FL_DISC_LINGUA_INDIGENA', 'Int'), ('IN_DISC_PORT_SEGUNDA_LINGUA', 'FL_DISC_PORT_SEGUNDA_LINGUA', 'Int'), ('IN_DISC_MATEMATICA', 'FL_DISC_MATEMATICA', 'Int'), ('IN_DISC_CIENCIAS', 'FL_DISC_CIENCIAS', 'Int'), ('IN_DISC_FISICA', 'FL_DISC_FISICA', 'Int'), ('IN_DISC_QUIMICA', 'FL_DISC_QUIMICA', 'Int'), ('IN_DISC_BIOLOGIA', 'FL_DISC_BIOLOGIA', 'Int'), ('IN_DISC_HISTORIA', 'FL_DISC_HISTORIA', 'Int'), ('IN_DISC_GEOGRAFIA', 'FL_DISC_GEOGRAFIA', 'Int'), ('IN_DISC_SOCIOLOGIA', 'FL_DISC_SOCIOLOGIA', 'Int'), ('IN_DISC_FILOSOFIA', 'FL_DISC_FILOSOFIA', 'Int'), ('IN_DISC_ESTUDOS_SOCIAIS', 'FL_DISC_ESTUDOS_SOCIAIS', 'Int'), ('IN_DISC_EST_SOCIAIS_SOCIOLOGIA', 'FL_DISC_EST_SOCIAIS_SOCIOLOGIA', 'Int'), ('IN_DISC_INFORMATICA_COMPUTACAO', 'FL_DISC_INFORMATICA_COMPUTACAO', 'Int'), ('IN_DISC_ENSINO_RELIGIOSO', 'FL_DISC_ENSINO_RELIGIOSO', 'Int'), ('IN_DISC_PROFISSIONALIZANTE', 'FL_DISC_PROFISSIONALIZANTE', 'Int'), ('IN_DISC_ESTAGIO_SUPERVISIONADO', 'FL_DISC_ESTAGIO_SUPERVISIONADO', 'Int'), ('IN_DISC_PEDAGOGICAS', 'FL_DISC_PEDAGOGICAS', 'Int'), ('IN_DISC_OUTRAS', 'FL_DISC_OUTRAS', 'Int'), ('IN_DISC_ATENDIMENTO_ESPECIAIS', 'FL_DISC_ATENDIMENTO_ESPECIAIS', 'Int'), ('IN_DISC_DIVER_SOCIO_CULTURAL', 'FL_DISC_DIVER_SOCIO_CULTURAL', 'Int'), ('CO_ENTIDADE', 'CD_ENTIDADE', 'Int'), ('CO_REGIAO', 'CD_REGIAO', 'Int'), ('CO_MESORREGIAO', 'CD_MESORREGIAO', 'Int'), ('CO_MICRORREGIAO', 'CD_MICRORREGIAO', 'Int'), ('CO_UF', 'CD_UF', 'Int'), ('CO_MUNICIPIO', 'CD_MUNICIPIO', 'Int'), ('CO_DISTRITO', 'CD_DISTRITO', 'Int'), ('TP_DEPENDENCIA', 'TP_DEPENDENCIA', 'Int'), ('TP_LOCALIZACAO', 'TP_LOCALIZACAO', 'Int'), ('TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_CATEGORIA_ESCOLA_PRIVADA', 'Int'), ('IN_CONVENIADA_PP', 'FL_CONVENIADA_PP', 'Int'), ('TP_CONVENIO_PODER_PUBLICO', 'TP_CONVENIO_PODER_PUBLICO', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_EMP', 'FL_MANT_ESCOLA_PRIVADA_EMP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_ONG', 'FL_MANT_ESCOLA_PRIVADA_ONG', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_OSCIP', 'FL_MANT_ESCOLA_PRIVADA_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIV_ONG_OSCIP', 'FL_MANT_ESCOLA_PRIV_ONG_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIND', 'FL_MANT_ESCOLA_PRIVADA_SIND', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIST_S', 'FL_MANT_ESCOLA_PRIVADA_SIST_S', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_S_FINS', 'FL_MANT_ESCOLA_PRIVADA_S_FINS', 'Int'), ('TP_REGULAMENTACAO', 'TP_REGULAMENTACAO', 'Int'), ('TP_LOCALIZACAO_DIFERENCIADA', 'TP_LOCALIZACAO_DIFERENCIADA', 'Int'), ('IN_EDUCACAO_INDIGENA', 'FL_EDUCACAO_INDIGENA', 'Int')]

# COMMAND ----------

for column, renamed, _type in transformation:
  df_source = df_source.withColumn(column, col(column).cast(_type))
  df_source = df_source.withColumnRenamed(column, renamed)

# COMMAND ----------

df_2009_a_2014 = df_source.filter((df_source.NR_ANO_CENSO >= 2009) & (df_source.NR_ANO_CENSO <= 2014))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_CONVENIO_PODER_PUBLICO',\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 1, 2).\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 2, 1).otherwise(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_MEDIACAO_DIDATICO_PEDAGO',\
                  when(col("TP_ETAPA_ENSINO").isin([47, 48, 61, 63]),2).otherwise(lit(1)))


# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_TIPO_ATENDIMENTO_TURMA',\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 0, 1).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 1, 1).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 3, 1).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 4, 3).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 5, 4).otherwise(lit(None)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_TIPO_LOCAL_TURMA',\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 0, 0).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 2, 2).\
                  when(df_2009_a_2014.TP_TIPO_TURMA == 3, 3).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_ESPECIAL_EXCLUSIVA',\
                  when(col("FK_COD_MOD_ENSINO") == 2,1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_REGULAR',\
                  when(col("TP_ETAPA_ENSINO").isin([1, 2, 3, 56, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 41, 25, 26, 27, 28, 29, 30, 31,  32, 33, 34, 35, 36, 37, 38]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EJA',\
                  when(col("TP_ETAPA_ENSINO").isin([65, 67, 69, 70, 71, 72, 73, 74]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_PROFISSIONALIZANTE',\
                  when(col("TP_ETAPA_ENSINO").isin([65, 67, 69, 70, 71, 72, 73, 74]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_DISC_EST_SOCIAIS_SOCIOLOGIA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2012,2013,2014])) & (df_2009_a_2014.FL_DISC_SOCIOLOGIA == lit(1)), 1).\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2012,2013,2014])) & (df_2009_a_2014.FL_DISC_ESTUDOS_SOCIAIS == lit(0)), 1).\
                  when(df_2009_a_2014.NR_ANO_CENSO.isin([2009,2010,2011]),df_2009_a_2014.FL_DISC_EST_SOCIAIS_SOCIOLOGIA).otherwise(lit(0)))

# COMMAND ----------

df_2015_a_2018 = df_source.filter((df_source.NR_ANO_CENSO >= 2015) & (df_source.NR_ANO_CENSO <= 2018))

# COMMAND ----------

df_2015_a_2018 = df_2015_a_2018.withColumn('TP_TIPO_ATENDIMENTO_TURMA',\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 0, 1).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 1, 1).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 3, 1).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 4, 3).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 5, 4).otherwise(lit(None)))

# COMMAND ----------

df_2015_a_2018 = df_2015_a_2018.withColumn('TP_TIPO_LOCAL_TURMA',\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 0, 0).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 2, 2).\
                  when(df_2015_a_2018.TP_TIPO_TURMA == 3, 3).otherwise(lit(0)))

# COMMAND ----------

df_2019 = df_source.filter(df_source.NR_ANO_CENSO >= 2019)

# COMMAND ----------

df = df_2015_a_2018.union(df_2019).union(df_2009_a_2014)

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('NR_ANO_CENSO').save(path=target, format="parquet", mode='overwrite')