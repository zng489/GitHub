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
# MAGIC Tabela/Arquivo Origem /raw/crw/inep_docente/
# MAGIC Tabela/Arquivo Destino /trs/uniepro/docente/
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
from pyspark.sql.functions import concat, col, lit

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
#tables =  {
#   "path_origin": "crw/inep_censo_escolar/docente/",
#   "path_destination": "inep_censo_escolar/censo_escolar_docentes",
#   "path_territorio": "mtd/corp/estrutura_territorial/"
# }

#dls = {"folders":{"landing":"/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/raw","trusted":"/trs"}}

#adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_docente',
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

source_territorio = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_territorio"])
source_territorio

# COMMAND ----------

df_territorio = spark.read.parquet(source_territorio).select('CD_MUNICIPIO','CD_REGIAO_GEOGRAFICA','CD_MICRORREGIAO_GEOGRAFICA','CD_MESORREGIAO_GEOGRAFICA')

# COMMAND ----------

df_source = spark.read.parquet(source)

# COMMAND ----------

df_source = df_source.withColumnRenamed('NU_ANO_CENSO','NR_ANO_CENSO')
df_source = df_source.withColumnRenamed('NU_DIA','NR_DIA')
df_source = df_source.withColumnRenamed('NU_MES','NR_MES')
df_source = df_source.withColumnRenamed('NU_ANO','NR_ANO')
df_source = df_source.withColumnRenamed('NU_IDADE_REFERENCIA','NR_IDADE_REFERENCIA')
df_source = df_source.withColumnRenamed('NU_IDADE','NR_IDADE')
df_source = df_source.withColumnRenamed('CO_PAIS_ORIGEM','CD_PAIS_ORIGEM')
df_source = df_source.withColumnRenamed('CO_UF_NASC','CD_UF_NASC')
df_source = df_source.withColumnRenamed('CO_MUNICIPIO_NASC','CD_MUNICIPIO_NASC')
df_source = df_source.withColumnRenamed('CO_UF_END','CD_UF_END')
df_source = df_source.withColumnRenamed('CO_MUNICIPIO_END','CD_MUNICIPIO_END')
df_source = df_source.withColumnRenamed('ID_DIFERENCIADA','TP_LOCAL_RESID_DIFERENCIADA')
df_source = df_source.withColumnRenamed('IN_NECESSIDADE_ESPECIAL','FL_NECESSIDADE_ESPECIAL')
df_source = df_source.withColumnRenamed('IN_BAIXA_VISAO','FL_BAIXA_VISAO')
df_source = df_source.withColumnRenamed('IN_CEGUEIRA','FL_CEGUEIRA')
df_source = df_source.withColumnRenamed('IN_DEF_AUDITIVA','FL_DEF_AUDITIVA')
df_source = df_source.withColumnRenamed('IN_DEF_FISICA','FL_DEF_FISICA')
df_source = df_source.withColumnRenamed('IN_DEF_INTELECTUAL','FL_DEF_INTELECTUAL')
df_source = df_source.withColumnRenamed('IN_SURDEZ','FL_SURDEZ')
df_source = df_source.withColumnRenamed('IN_SURDOCEGUEIRA','FL_SURDOCEGUEIRA')
df_source = df_source.withColumnRenamed('IN_DEF_MULTIPLA','FL_DEF_MULTIPLA')
df_source = df_source.withColumnRenamed('IN_AUTISMO','FL_AUTISMO')
df_source = df_source.withColumnRenamed('IN_SUPERDOTACAO','FL_SUPERDOTACAO')
df_source = df_source.withColumnRenamed('TP_ESCOLARIDADE','TP_ESCOLARIDADE')
df_source = df_source.withColumnRenamed('TP_ENSINO_MEDIO','TP_ENSINO_MEDIO')
df_source = df_source.withColumnRenamed('TP_SITUACAO_CURSO_1','TP_SITUACAO_CURSO_1')
df_source = df_source.withColumnRenamed('CO_AREA_CURSO_1','CD_AREA_CURSO_1')
df_source = df_source.withColumnRenamed('CO_CURSO_1','CD_CURSO_1')
df_source = df_source.withColumnRenamed('IN_LICENCIATURA_1','FL_LICENCIATURA_1')
df_source = df_source.withColumnRenamed('IN_COM_PEDAGOGICA_1','FL_COM_PEDAGOGICA_1')
df_source = df_source.withColumnRenamed('NU_ANO_INICIO_1','NR_ANO_INICIO_1')
df_source = df_source.withColumnRenamed('NU_ANO_CONCLUSAO_1','NR_ANO_CONCLUSAO_1')
df_source = df_source.withColumnRenamed('TP_TIPO_IES_1','TP_TIPO_IES_1')
df_source = df_source.withColumnRenamed('CO_IES_1','CD_IES_1')
df_source = df_source.withColumnRenamed('TP_SITUACAO_CURSO_2','TP_SITUACAO_CURSO_2')
df_source = df_source.withColumnRenamed('CO_AREA_CURSO_2','CD_AREA_CURSO_2')
df_source = df_source.withColumnRenamed('CO_CURSO_2','CD_CURSO_2')
df_source = df_source.withColumnRenamed('IN_LICENCIATURA_2','FL_LICENCIATURA_2')
df_source = df_source.withColumnRenamed('IN_COM_PEDAGOGICA_2','FL_COM_PEDAGOGICA_2')
df_source = df_source.withColumnRenamed('NU_ANO_INICIO_2','NR_ANO_INICIO_2')
df_source = df_source.withColumnRenamed('NU_ANO_CONCLUSAO_2','NR_ANO_CONCLUSAO_2')
df_source = df_source.withColumnRenamed('CO_IES_2','CD_IES_2')
df_source = df_source.withColumnRenamed('CO_AREA_CURSO_3','CD_AREA_CURSO_3')
df_source = df_source.withColumnRenamed('CO_CURSO_3','CD_CURSO_3')
df_source = df_source.withColumnRenamed('IN_LICENCIATURA_3','FL_LICENCIATURA_3')
df_source = df_source.withColumnRenamed('IN_COM_PEDAGOGICA_3','FL_COM_PEDAGOGICA_3')
df_source = df_source.withColumnRenamed('NU_ANO_INICIO_3','NR_ANO_INICIO_3')
df_source = df_source.withColumnRenamed('NU_ANO_CONCLUSAO_3','NR_ANO_CONCLUSAO_3')
df_source = df_source.withColumnRenamed('TP_TIPO_IES_3','TP_TIPO_IES_3')
df_source = df_source.withColumnRenamed('CO_IES_3','CD_IES_3')
df_source = df_source.withColumnRenamed('IN_COMPLEMENTACAO_PEDAGOGICA','FL_COMPLEMENTACAO_PEDAGOGICA')
df_source = df_source.withColumnRenamed('CO_AREA_COMPL_PEDAGOGICA_1','CD_AREA_COMPL_PEDAGOGICA_1')
df_source = df_source.withColumnRenamed('CO_AREA_COMPL_PEDAGOGICA_2','CD_AREA_COMPL_PEDAGOGICA_2')
df_source = df_source.withColumnRenamed('CO_AREA_COMPL_PEDAGOGICA_3','CD_AREA_COMPL_PEDAGOGICA_3')
df_source = df_source.withColumnRenamed('IN_ESPECIALIZACAO','FL_ESPECIALIZACAO')
df_source = df_source.withColumnRenamed('IN_MESTRADO','FL_MESTRADO')
df_source = df_source.withColumnRenamed('IN_DOUTORADO','FL_DOUTORADO')
df_source = df_source.withColumnRenamed('IN_POS_NENHUM','FL_POS_NENHUM')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_CRECHE','FL_ESPECIFICO_CRECHE')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_PRE_ESCOLA','FL_ESPECIFICO_PRE_ESCOLA')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_ANOS_INICIAIS','FL_ESPECIFICO_ANOS_INICIAIS')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_ANOS_FINAIS','FL_ESPECIFICO_ANOS_FINAIS')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_ENS_MEDIO','FL_ESPECIFICO_ENS_MEDIO')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_EJA','FL_ESPECIFICO_EJA')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_ED_ESPECIAL','FL_ESPECIFICO_ED_ESPECIAL')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_ED_INDIGENA','FL_ESPECIFICO_ED_INDIGENA')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_CAMPO','FL_ESPECIFICO_CAMPO')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_AMBIENTAL','FL_ESPECIFICO_AMBIENTAL')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_DIR_HUMANOS','FL_ESPECIFICO_DIR_HUMANOS')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_DIV_SEXUAL','FL_ESPECIFICO_DIV_SEXUAL')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_DIR_ADOLESC','FL_ESPECIFICO_DIR_ADOLESC')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_AFRO','FL_ESPECIFICO_AFRO')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_GESTAO','FL_ESPECIFICO_GESTAO')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_OUTROS','FL_ESPECIFICO_OUTROS')
df_source = df_source.withColumnRenamed('IN_ESPECIFICO_NENHUM','FL_ESPECIFICO_NENHUM')
df_source = df_source.withColumnRenamed('ID_TURMA','NR_TURMA')
df_source = df_source.withColumnRenamed('TP_TIPO_CONTRATACAO','TP_TIPO_CONTRATACAO')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_PORTUGUESA','FL_DISC_LINGUA_PORTUGUESA')
df_source = df_source.withColumnRenamed('IN_DISC_EDUCACAO_FISICA','FL_DISC_EDUCACAO_FISICA')
df_source = df_source.withColumnRenamed('IN_DISC_ARTES','FL_DISC_ARTES')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_INGLES','FL_DISC_LINGUA_INGLES')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_ESPANHOL','FL_DISC_LINGUA_ESPANHOL')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_FRANCES','FL_DISC_LINGUA_FRANCES')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_OUTRA','FL_DISC_LINGUA_OUTRA')
df_source = df_source.withColumnRenamed('IN_DISC_LIBRAS','FL_DISC_LIBRAS')
df_source = df_source.withColumnRenamed('IN_DISC_LINGUA_INDIGENA','FL_DISC_LINGUA_INDIGENA')
df_source = df_source.withColumnRenamed('IN_DISC_PORT_SEGUNDA_LINGUA','FL_DISC_PORT_SEGUNDA_LINGUA')
df_source = df_source.withColumnRenamed('IN_DISC_MATEMATICA','FL_DISC_MATEMATICA')
df_source = df_source.withColumnRenamed('IN_DISC_CIENCIAS','FL_DISC_CIENCIAS')
df_source = df_source.withColumnRenamed('IN_DISC_FISICA','FL_DISC_FISICA')
df_source = df_source.withColumnRenamed('IN_DISC_QUIMICA','FL_DISC_QUIMICA')
df_source = df_source.withColumnRenamed('IN_DISC_BIOLOGIA','FL_DISC_BIOLOGIA')
df_source = df_source.withColumnRenamed('IN_DISC_HISTORIA','FL_DISC_HISTORIA')
df_source = df_source.withColumnRenamed('IN_DISC_GEOGRAFIA','FL_DISC_GEOGRAFIA')
df_source = df_source.withColumnRenamed('IN_DISC_SOCIOLOGIA','FL_DISC_SOCIOLOGIA')
df_source = df_source.withColumnRenamed('IN_DISC_FILOSOFIA','FL_DISC_FILOSOFIA')
df_source = df_source.withColumnRenamed('IN_DISC_ESTUDOS_SOCIAIS','FL_DISC_ESTUDOS_SOCIAIS')
df_source = df_source.withColumnRenamed('IN_DISC_EST_SOCIAIS_SOCIOLOGIA','FL_DISC_EST_SOCIAIS_SOCIOLOGIA')
df_source = df_source.withColumnRenamed('IN_DISC_INFORMATICA_COMPUTACAO','FL_DISC_INFORMATICA_COMPUTACAO')
df_source = df_source.withColumnRenamed('IN_DISC_ENSINO_RELIGIOSO','FL_DISC_ENSINO_RELIGIOSO')
df_source = df_source.withColumnRenamed('IN_DISC_PROFISSIONALIZANTE','FL_DISC_PROFISSIONALIZANTE')
df_source = df_source.withColumnRenamed('IN_DISC_ESTAGIO_SUPERVISIONADO','FL_DISC_ESTAGIO_SUPERVISIONADO')
df_source = df_source.withColumnRenamed('IN_DISC_PEDAGOGICAS','FL_DISC_PEDAGOGICAS')
df_source = df_source.withColumnRenamed('IN_DISC_OUTRAS','FL_DISC_OUTRAS')
df_source = df_source.withColumnRenamed('IN_DISC_ATENDIMENTO_ESPECIAIS','FL_DISC_ATENDIMENTO_ESPECIAIS')
df_source = df_source.withColumnRenamed('IN_DISC_DIVER_SOCIO_CULTURAL','FL_DISC_DIVER_SOCIO_CULTURAL')
df_source = df_source.withColumnRenamed('TP_TIPO_TURMA','TP_TIPO_TURMA')
df_source = df_source.withColumnRenamed('TP_TIPO_ATENDIMENTO_TURMA','TP_TIPO_ATENDIMENTO_TURMA')
df_source = df_source.withColumnRenamed('TP_TIPO_LOCAL_TURMA','TP_TIPO_LOCAL_TURMA')
df_source = df_source.withColumnRenamed('TP_MEDIACAO_DIDATICO_PEDAGO','TP_MEDIACAO_DIDATICO_PEDAGO')
df_source = df_source.withColumnRenamed('TP_ETAPA_ENSINO','TP_ETAPA_ENSINO')
df_source = df_source.withColumnRenamed('CO_CURSO_EDUC_PROFISSIONAL','CD_CURSO_EDUC_PROFISSIONAL')
df_source = df_source.withColumnRenamed('IN_ESPECIAL_EXCLUSIVA','FL_ESPECIAL_EXCLUSIVA')
df_source = df_source.withColumnRenamed('IN_REGULAR','FL_REGULAR')
df_source = df_source.withColumnRenamed('IN_EJA','FL_EJA')
df_source = df_source.withColumnRenamed('IN_PROFISSIONALIZANTE','FL_PROFISSIONALIZANTE')
df_source = df_source.withColumnRenamed('CO_ENTIDADE','CD_ENTIDADE')
df_source = df_source.withColumnRenamed('CO_REGIAO','CD_REGIAO')
df_source = df_source.withColumnRenamed('CO_MESORREGIAO','CD_MESORREGIAO')
df_source = df_source.withColumnRenamed('CO_MICRORREGIAO','CD_MICRORREGIAO')
df_source = df_source.withColumnRenamed('CO_UF','CD_UF')
df_source = df_source.withColumnRenamed('CO_MUNICIPIO','CD_MUNICIPIO')
df_source = df_source.withColumnRenamed('CO_DISTRITO','CD_DISTRITO')
df_source = df_source.withColumnRenamed('IN_CONVENIADA_PP','FL_CONVENIADA_PP')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIVADA_EMP','FL_MANT_ESCOLA_PRIVADA_EMP')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIV_ONG_OSCIP','FL_MANT_ESCOLA_PRIVADA_ONG')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIVADA_OSCIP','FL_MANT_ESCOLA_PRIVADA_OSCIP')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIV_ONG_OSCIP','FL_MANT_ESCOLA_PRIV_ONG_OSCIP')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIVADA_SIND','FL_MANT_ESCOLA_PRIVADA_SIND')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIVADA_SIST_S','FL_MANT_ESCOLA_PRIVADA_SIST_S')
df_source = df_source.withColumnRenamed('IN_MANT_ESCOLA_PRIVADA_S_FINS','FL_MANT_ESCOLA_PRIVADA_S_FINS')
df_source = df_source.withColumnRenamed('IN_EDUCACAO_INDIGENA','FL_EDUCACAO_INDIGENA')
df_source = df_source.withColumnRenamed('CO_PAIS_RESIDENCIA','CD_PAIS_RESIDENCIA')

# COMMAND ----------

df_source = df_source.join(df_territorio, on='CD_MUNICIPIO', how='left')

# COMMAND ----------

df_2009_a_2014 = df_source.filter((df_source.NR_ANO_CENSO >= 2009) & (df_source.NR_ANO_CENSO <= 2014))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_SEXO',\
                  when(df_2009_a_2014.TP_SEXO == "M", 1).\
                  when(df_2009_a_2014.TP_SEXO == "F", 2).otherwise(df_source.TP_SEXO))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_CONVENIO_PODER_PUBLICO',\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 1, 2).\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 2, 1).otherwise(df_source.TP_CONVENIO_PODER_PUBLICO))


# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_MEDIACAO_DIDATICO_PEDAGO',\
                  when(col("TP_ETAPA_ENSINO").isin([47, 48, 61, 63]),2).otherwise(lit(1)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EJA',\
                  when(col("TP_ETAPA_ENSINO").isin([65, 67, 69, 70, 71, 72, 73, 74]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_REGULAR',\
                  when(col("TP_ETAPA_ENSINO").isin([1, 2, 3, 56, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 41, 25, 26, 27, 28, 29, 30, 31,  32, 33, 34, 35, 36, 37, 38]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_PROFISSIONALIZANTE',\
                  when(col("TP_ETAPA_ENSINO").isin([65, 67, 69, 70, 71, 72, 73, 74]),1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_ENSINO_MEDIO',\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 5, 1).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 3, 2).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 4, 4).otherwise(lit(None)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_ESCOLARIDADE',\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 1, 1).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 2, 2).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 3, 3).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 4, 3).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 5, 3).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 6, 4).otherwise(df_source.TP_ESCOLARIDADE))



# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_ENSINO_MEDIO',\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 5, 1).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 3, 2).\
                  when(df_2009_a_2014.TP_ESCOLARIDADE == 4, 4).otherwise(lit(None)))

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
                  when(df_2009_a_2014.TP_TIPO_TURMA == 3, 3).otherwise(lit(None)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_DISC_EST_SOCIAIS_SOCIOLOGIA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2012,2013,2014])) & (df_2009_a_2014.FL_DISC_SOCIOLOGIA == lit(1)), 1).\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2012,2013,2014])) & (df_2009_a_2014.FL_DISC_ESTUDOS_SOCIAIS == lit(1)), 1).\
                  when(df_2009_a_2014.NR_ANO_CENSO.isin([2009,2010,2011]),df_2009_a_2014.FL_DISC_EST_SOCIAIS_SOCIOLOGIA).otherwise(lit(0)))

# COMMAND ----------

# Para calcular quantidade de anos completos comparando a data de nascimento do Docente (NU_DIA / NU_MES / NU_ANO) em relação ao dia 31 de maio do ano em questão.(ANO_CENSO) seguimos os passos abaixo: 1 Passo.

df_2009_a_2014 = df_2009_a_2014.withColumn('TMP_CAMPO_IDADE',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2009,2010,2011,2012,2013])),to_date(concat(col("NR_DIA"),lit('/'),col("NR_MES"),lit('/'),col("NR_ANO")),"dd/MM/yyyy")).otherwise(lit(None)))

# COMMAND ----------

# Para calcular quantidade de anos completos comparando a data de nascimento do Docente (NU_DIA / NU_MES / NU_ANO) em relação ao dia 31 de maio do ano em questão.(ANO_CENSO) seguimos os passos abaixo: 2 Passo.

df_2009_a_2014 = df_2009_a_2014.withColumn('TMP_IDADE_ANO_CENSO',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2009,2010,2011,2012,2013])),to_date(concat(lit('31'),lit('/'),lit('05'),lit('/'),col("NR_ANO_CENSO")),"dd/MM/yyyy")).otherwise(lit(None)))

# COMMAND ----------

# Para calcular quantidade de anos completos comparando a data de nascimento do Docente (NU_DIA / NU_MES / NU_ANO) em relação ao dia 31 de maio do ano em questão.(ANO_CENSO) seguimos os passos abaixo: 3 Passo. datediff(col("TMP_IDADE_ANO_CENSO"),col("TMP_CAMPO_IDADE"))

#### VERIFICAR ESSA REGRA COM O ANSELMO
df_2009_a_2014 = df_2009_a_2014.withColumn('NR_IDADE_REFERENCIA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2009,2010,2011,2012,2013])),(months_between(col("TMP_IDADE_ANO_CENSO"),col("TMP_CAMPO_IDADE"), roundOff=True)/12).cast("decimal(10,2)")).otherwise(col("NR_IDADE_REFERENCIA").cast("decimal(10,2)")))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.drop('TMP_IDADE_ANO_CENSO','TMP_CAMPO_IDADE')

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

#df = df.withColumn('TP_ENSINO_MEDIO',\
#                  when(((df.NR_ANO_CENSO >= 2015) & (df.NR_ANO_CENSO <= 2018)) & df.TP_NORMAL_MAGISTERIO == 0, 9).\
#                  when(((df.NR_ANO_CENSO >= 2015) & (df.NR_ANO_CENSO <= 2018)) & df.TP_NORMAL_MAGISTERIO == 1, 2).\
##                  when(((df.NR_ANO_CENSO >= 2015) & (df.NR_ANO_CENSO <= 2018)) & df.TP_NORMAL_MAGISTERIO == 2, 4).\
#                   when( df.NR_ANO_CENSO.isin([2009,2010,2011,2012,2013,2014,2019]),df.TP_ENSINO_MEDIO).otherwise(lit(None)))

# COMMAND ----------

reorder_columns = ['NR_ANO_CENSO','ID_DOCENTE','NR_DIA','NR_MES','NR_ANO','NR_IDADE_REFERENCIA','NR_IDADE','TP_SEXO','TP_COR_RACA','TP_NACIONALIDADE','CD_PAIS_ORIGEM',
'CD_UF_NASC','CD_MUNICIPIO_NASC','CD_PAIS_RESIDENCIA','CD_UF_END','CD_MUNICIPIO_END','TP_ZONA_RESIDENCIAL','TP_LOCAL_RESID_DIFERENCIADA','FL_NECESSIDADE_ESPECIAL',
'FL_BAIXA_VISAO','FL_CEGUEIRA','FL_DEF_AUDITIVA','FL_DEF_FISICA','FL_DEF_INTELECTUAL','FL_SURDEZ','FL_SURDOCEGUEIRA','FL_DEF_MULTIPLA','FL_AUTISMO',
'FL_SUPERDOTACAO','TP_ESCOLARIDADE','TP_ENSINO_MEDIO','TP_SITUACAO_CURSO_1','CD_AREA_CURSO_1','CD_CURSO_1','FL_LICENCIATURA_1','FL_COM_PEDAGOGICA_1','NR_ANO_INICIO_1','NR_ANO_CONCLUSAO_1','TP_TIPO_IES_1','CD_IES_1','TP_SITUACAO_CURSO_2',
'CD_AREA_CURSO_2','CD_CURSO_2','FL_LICENCIATURA_2','FL_COM_PEDAGOGICA_2','NR_ANO_INICIO_2','NR_ANO_CONCLUSAO_2','TP_TIPO_IES_2','CD_IES_2',
'TP_SITUACAO_CURSO_3','CD_AREA_CURSO_3','CD_CURSO_3','FL_LICENCIATURA_3','FL_COM_PEDAGOGICA_3','NR_ANO_INICIO_3','NR_ANO_CONCLUSAO_3',
'TP_TIPO_IES_3','CD_IES_3','FL_COMPLEMENTACAO_PEDAGOGICA','CD_AREA_COMPL_PEDAGOGICA_1','CD_AREA_COMPL_PEDAGOGICA_2','CD_AREA_COMPL_PEDAGOGICA_3',
'FL_ESPECIALIZACAO','FL_MESTRADO','FL_DOUTORADO','FL_POS_NENHUM','FL_ESPECIFICO_CRECHE','FL_ESPECIFICO_PRE_ESCOLA','FL_ESPECIFICO_ANOS_INICIAIS',
'FL_ESPECIFICO_ANOS_FINAIS','FL_ESPECIFICO_ENS_MEDIO','FL_ESPECIFICO_EJA','FL_ESPECIFICO_ED_ESPECIAL','FL_ESPECIFICO_ED_INDIGENA','FL_ESPECIFICO_CAMPO',
'FL_ESPECIFICO_AMBIENTAL','FL_ESPECIFICO_DIR_HUMANOS','FL_ESPECIFICO_DIV_SEXUAL','FL_ESPECIFICO_DIR_ADOLESC','FL_ESPECIFICO_AFRO','FL_ESPECIFICO_GESTAO',
'FL_ESPECIFICO_OUTROS','FL_ESPECIFICO_NENHUM','NR_TURMA','TP_TIPO_DOCENTE','TP_TIPO_CONTRATACAO','FL_DISC_LINGUA_PORTUGUESA','FL_DISC_EDUCACAO_FISICA',
'FL_DISC_ARTES','FL_DISC_LINGUA_INGLES','FL_DISC_LINGUA_ESPANHOL','FL_DISC_LINGUA_FRANCES','FL_DISC_LINGUA_OUTRA','FL_DISC_LIBRAS','FL_DISC_LINGUA_INDIGENA',
'FL_DISC_PORT_SEGUNDA_LINGUA','FL_DISC_MATEMATICA','FL_DISC_CIENCIAS','FL_DISC_FISICA','FL_DISC_QUIMICA','FL_DISC_BIOLOGIA','FL_DISC_HISTORIA',
'FL_DISC_GEOGRAFIA','FL_DISC_SOCIOLOGIA','FL_DISC_FILOSOFIA','FL_DISC_ESTUDOS_SOCIAIS','FL_DISC_EST_SOCIAIS_SOCIOLOGIA','FL_DISC_INFORMATICA_COMPUTACAO',
'FL_DISC_ENSINO_RELIGIOSO','FL_DISC_PROFISSIONALIZANTE','FL_DISC_ESTAGIO_SUPERVISIONADO','FL_DISC_PEDAGOGICAS','FL_DISC_OUTRAS','FL_DISC_ATENDIMENTO_ESPECIAIS',
'FL_DISC_DIVER_SOCIO_CULTURAL','TP_TIPO_TURMA','TP_TIPO_ATENDIMENTO_TURMA','TP_TIPO_LOCAL_TURMA','TP_MEDIACAO_DIDATICO_PEDAGO','TP_ETAPA_ENSINO',
'CD_CURSO_EDUC_PROFISSIONAL','FL_ESPECIAL_EXCLUSIVA','FL_REGULAR','FL_EJA','FL_PROFISSIONALIZANTE','CD_ENTIDADE','CD_REGIAO','CD_MESORREGIAO','CD_MICRORREGIAO',
'CD_UF','CD_MUNICIPIO','CD_DISTRITO','TP_DEPENDENCIA','TP_LOCALIZACAO','TP_CATEGORIA_ESCOLA_PRIVADA','FL_CONVENIADA_PP','TP_CONVENIO_PODER_PUBLICO',
'FL_MANT_ESCOLA_PRIVADA_EMP','FL_MANT_ESCOLA_PRIVADA_ONG','FL_MANT_ESCOLA_PRIVADA_SIND',
'FL_MANT_ESCOLA_PRIVADA_SIST_S','FL_MANT_ESCOLA_PRIVADA_S_FINS','TP_REGULAMENTACAO','TP_LOCALIZACAO_DIFERENCIADA','FL_EDUCACAO_INDIGENA']

# COMMAND ----------

df = df.select(*reorder_columns)

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('NR_ANO_CENSO').save(path=target, format="parquet", mode='overwrite')

# COMMAND ----------

#df.createOrReplaceTempView("tb_docente")

# COMMAND ----------

#%sql
#select NR_ANO_CENSO,ID_DOCENTE,NR_TURMA,TP_ETAPA_ENSINO,count(*) from tb_docente
#group by NR_ANO_CENSO,ID_DOCENTE,NR_TURMA,TP_ETAPA_ENSINO
#having count(*)> 1

# COMMAND ----------

#%sql
#select * from tb_docente
#where ID_DOCENTE = '144702225854'