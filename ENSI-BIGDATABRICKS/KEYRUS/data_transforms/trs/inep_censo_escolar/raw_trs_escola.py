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

import json
import re

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

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
#tables =  {
#   "path_origin": "crw/inep_censo_escolar/escola/",
#   "path_escola": "mtd/corp/base_escolas/",
#   "path_territorio": "mtd/corp/estrutura_territorial/",
#   "path_turma": "inep_censo_escolar/censo_escolar_turmas/",
#   "path_destination":"inep_censo_escolar/censo_escolar_escola"
# }

#dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
#dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

#adf = {
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

source_escola = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_escola"])
source_escola

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

source_territorio = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_territorio"])
source_territorio

# COMMAND ----------

source_turma = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_turma"])
source_turma

# COMMAND ----------

df_source = spark.read.parquet(source)

# COMMAND ----------

df_source = df_source.drop('CO_MESORREGIAO','CO_MICRORREGIAO','CO_REGIAO')

# COMMAND ----------

df_territorio = spark.read.parquet(source_territorio).select('CD_MUNICIPIO','CD_REGIAO_GEOGRAFICA','CD_MICRORREGIAO_GEOGRAFICA','CD_MESORREGIAO_GEOGRAFICA')

# COMMAND ----------

df_turma_2009_2014 = spark.read.parquet(source_turma).select('NR_ANO_CENSO','CD_ENTIDADE','TP_ETAPA_ENSINO').where(col('NR_ANO_CENSO') <= lit(2014)).distinct()

# COMMAND ----------

df_turma_2009_2014 = df_turma_2009_2014.withColumn('FL_PROFISSIONALIZANTE_TURMA',\
                  when((df_turma_2009_2014.NR_ANO_CENSO.isin([2009,2010,2011,2012,2013,2014])) & (df_turma_2009_2014.TP_ETAPA_ENSINO.isin([30,31,32,33,34, 35,36,37,38,39,40,60,61,62,63,65,67,68,73,74])), 1).otherwise(lit(0)))

# COMMAND ----------

df_turma_2009_2014 = df_turma_2009_2014.withColumn('FL_MEDIACAO_PRESENCIAL_TURMA',\
                  when((df_turma_2009_2014.TP_ETAPA_ENSINO.isin([47, 48, 61, 63])== False), 1).otherwise(lit(0)))

# COMMAND ----------

df_turma_2009_2014 = df_turma_2009_2014.withColumn('FL_MEDIACAO_SEMIPRESENCIAL_TURMA',\
                  when((df_turma_2009_2014.TP_ETAPA_ENSINO.isin([47, 48, 61, 63])), 1).otherwise(lit(0)))

# COMMAND ----------

df_turma_2009_2014.drop(df_turma_2009_2014.TP_ETAPA_ENSINO)

# COMMAND ----------

 df_turma_2009_2014 =df_turma_2009_2014.groupBy('NR_ANO_CENSO','CD_ENTIDADE').agg(max('FL_PROFISSIONALIZANTE_TURMA'),max('FL_MEDIACAO_PRESENCIAL_TURMA'), max('FL_MEDIACAO_SEMIPRESENCIAL_TURMA'))

# COMMAND ----------

df_turma_2009_2014 = df_turma_2009_2014.withColumnRenamed('max(FL_PROFISSIONALIZANTE_TURMA)','FL_PROFISSIONALIZANTE_TURMA')
df_turma_2009_2014 = df_turma_2009_2014.withColumnRenamed('max(FL_MEDIACAO_PRESENCIAL_TURMA)','FL_MEDIACAO_PRESENCIAL_TURMA')
df_turma_2009_2014 = df_turma_2009_2014.withColumnRenamed('max(FL_MEDIACAO_SEMIPRESENCIAL_TURMA)','FL_MEDIACAO_SEMIPRESENCIAL_TURMA')

# COMMAND ----------

df_turma = df_turma_2009_2014.dropDuplicates()

# COMMAND ----------

df_escola = spark.read.parquet(source_escola).select('nr_ano_censo','cd_entidade','fl_escola_sesi','fl_escola_senai')

# COMMAND ----------

transformation = [('NU_ANO_CENSO', 'NR_ANO_CENSO', 'Int'), ('CO_ENTIDADE', 'CD_ENTIDADE', 'Int'), ('NO_ENTIDADE', 'NO_ENTIDADE', 'String'), ('CO_ORGAO_REGIONAL', 'CD_ORGAO_REGIONAL', 'String'), ('GEO_REF_LATITUDE', 'GEO_REF_LATITUDE', 'Int'), ('GEO_REF_LONGITUDE', 'GEO_REF_LONGITUDE', 'Int'), ('TP_SITUACAO_FUNCIONAMENTO', 'TP_SITUACAO_FUNCIONAMENTO', 'Int'), ('DT_ANO_LETIVO_INICIO', 'DT_ANO_LETIVO_INICIO', 'Date'), ('DT_ANO_LETIVO_TERMINO', 'DT_ANO_LETIVO_TERMINO', 'Date'),  ('CO_UF', 'CD_UF', 'Int'), ('CO_MUNICIPIO', 'CD_MUNICIPIO', 'Int'), ('CO_DISTRITO', 'CD_DISTRITO', 'Int'), ('TP_DEPENDENCIA', 'TP_DEPENDENCIA', 'Int'), ('TP_LOCALIZACAO', 'TP_LOCALIZACAO', 'Int'), ('TP_LOCALIZACAO_DIFERENCIADA', 'TP_LOCALIZACAO_DIFERENCIADA', 'Int'), ('IN_VINCULO_SECRETARIA_EDUCACAO', 'FL_VINCULO_SECRETARIA_EDUCACAO', 'Int'), ('IN_VINCULO_SEGURANCA_PUBLICA', 'FL_VINCULO_SEGURANCA_PUBLICA', 'Int'), ('IN_VINCULO_SECRETARIA_SAUDE', 'FL_VINCULO_SECRETARIA_SAUDE', 'Int'), ('IN_VINCULO_OUTRO_ORGAO', 'FL_VINCULO_OUTRO_ORGAO', 'Int'), ('TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_CATEGORIA_ESCOLA_PRIVADA', 'Int'), ('IN_CONVENIADA_PP', 'FL_CONVENIADA_PP', 'Int'), ('TP_CONVENIO_PODER_PUBLICO', 'TP_CONVENIO_PODER_PUBLICO', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_EMP', 'FL_MANT_ESCOLA_PRIVADA_EMP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_ONG', 'FL_MANT_ESCOLA_PRIVADA_ONG', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_OSCIP', 'FL_MANT_ESCOLA_PRIVADA_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIV_ONG_OSCIP', 'FL_MANT_ESCOLA_PRIV_ONG_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIND', 'FL_MANT_ESCOLA_PRIVADA_SIND', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIST_S', 'FL_MANT_ESCOLA_PRIVADA_SIST_S', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_S_FINS', 'FL_MANT_ESCOLA_PRIVADA_S_FINS', 'Int'), ('NU_CNPJ_ESCOLA_PRIVADA', 'NR_CNPJ_ESCOLA_PRIVADA', 'String'), ('NU_CNPJ_MANTENEDORA', 'NR_CNPJ_MANTENEDORA', 'String'), ('TP_REGULAMENTACAO', 'TP_REGULAMENTACAO', 'Int'), ('TP_RESPONSAVEL_REGULAMENTACAO', 'TP_RESPONSAVEL_REGULAMENTACAO', 'Int'), ('CO_ESCOLA_SEDE_VINCULADA', 'CO_ESCOLA_SEDE_VINCULADA', 'Int'), ('CO_IES_OFERTANTE', 'CO_IES_OFERTANTE', 'Int'), ('IN_LOCAL_FUNC_PREDIO_ESCOLAR', 'FL_LOCAL_FUNC_PREDIO_ESCOLAR', 'Int'), ('TP_OCUPACAO_PREDIO_ESCOLAR', 'TP_OCUPACAO_PREDIO_ESCOLAR', 'Int'), ('IN_LOCAL_FUNC_SALAS_EMPRESA', 'FL_LOCAL_FUNC_SALAS_EMPRESA', 'Int'), ('IN_LOCAL_FUNC_SOCIOEDUCATIVO', 'FL_LOCAL_FUNC_SOCIOEDUCATIVO', 'Int'), ('IN_LOCAL_FUNC_UNID_PRISIONAL', 'FL_LOCAL_FUNC_UNID_PRISIONAL', 'Int'), ('IN_LOCAL_FUNC_PRISIONAL_SOCIO', 'FL_LOCAL_FUNC_PRISIONAL_SOCIO', 'Int'), ('IN_LOCAL_FUNC_TEMPLO_IGREJA', 'FL_LOCAL_FUNC_TEMPLO_IGREJA', 'Int'), ('IN_LOCAL_FUNC_CASA_PROFESSOR', 'FL_LOCAL_FUNC_CASA_PROFESSOR', 'Int'), ('IN_LOCAL_FUNC_GALPAO', 'FL_LOCAL_FUNC_GALPAO', 'Int'), ('TP_OCUPACAO_GALPAO', 'TP_OCUPACAO_GALPAO', 'Int'), ('IN_LOCAL_FUNC_SALAS_OUTRA_ESC', 'FL_LOCAL_FUNC_SALAS_OUTRA_ESC', 'Int'), ('IN_LOCAL_FUNC_OUTROS', 'FL_LOCAL_FUNC_OUTROS', 'Int'), ('IN_PREDIO_COMPARTILHADO', 'FL_PREDIO_COMPARTILHADO', 'Int'), ('IN_AGUA_FILTRADA', 'FL_AGUA_FILTRADA', 'Int'), ('IN_AGUA_POTAVEL', 'FL_AGUA_POTAVEL', 'Int'), ('IN_AGUA_REDE_PUBLICA', 'FL_AGUA_REDE_PUBLICA', 'Int'), ('IN_AGUA_POCO_ARTESIANO', 'FL_AGUA_POCO_ARTESIANO', 'Int'), ('IN_AGUA_CACIMBA', 'FL_AGUA_CACIMBA', 'Int'), ('IN_AGUA_FONTE_RIO', 'FL_AGUA_FONTE_RIO', 'Int'), ('IN_AGUA_INEXISTENTE', 'FL_AGUA_INEXISTENTE', 'Int'), ('IN_ENERGIA_REDE_PUBLICA', 'FL_ENERGIA_REDE_PUBLICA', 'Int'), ('IN_ENERGIA_GERADOR', 'FL_ENERGIA_GERADOR', 'Int'),  ('IN_ENERGIA_OUTROS', 'FL_ENERGIA_OUTROS', 'Int'),  ('IN_ENERGIA_INEXISTENTE', 'FL_ENERGIA_INEXISTENTE', 'Int'), ('IN_ESGOTO_REDE_PUBLICA', 'FL_ESGOTO_REDE_PUBLICA', 'Int'), ('IN_ESGOTO_FOSSA_SEPTICA', 'FL_ESGOTO_FOSSA_SEPTICA', 'Int'), ('IN_ESGOTO_FOSSA_COMUM', 'FL_ESGOTO_FOSSA_COMUM', 'Int'), ('IN_ESGOTO_FOSSA', 'FL_ESGOTO_FOSSA', 'Int'), ('IN_ESGOTO_INEXISTENTE', 'FL_ESGOTO_INEXISTENTE', 'Int'), ('IN_LIXO_SERVICO_COLETA', 'FL_LIXO_SERVICO_COLETA', 'Int'), ('IN_LIXO_QUEIMA', 'FL_LIXO_QUEIMA', 'Int'), ('IN_LIXO_ENTERRA', 'FL_LIXO_ENTERRA', 'Int'), ('IN_LIXO_DESTINO_FINAL_PUBLICO', 'FL_LIXO_DESTINO_FINAL_PUBLICO', 'Int'), ('IN_LIXO_JOGA_OUTRA_AREA', 'FL_LIXO_JOGA_OUTRA_AREA', 'Int'), ('IN_LIXO_OUTROS', 'FL_LIXO_OUTROS', 'Int'), ('IN_LIXO_RECICLA', 'FL_LIXO_RECICLA', 'Int'), ('IN_TRATAMENTO_LIXO_SEPARACAO', 'FL_TRATAMENTO_LIXO_SEPARACAO', 'Int'), ('IN_TRATAMENTO_LIXO_REUTILIZA', 'FL_TRATAMENTO_LIXO_REUTILIZA', 'Int'), ('IN_TRATAMENTO_LIXO_INEXISTENTE', 'FL_TRATAMENTO_LIXO_INEXISTENTE', 'Int'), ('IN_ALMOXARIFADO', 'FL_ALMOXARIFADO', 'Int'), ('IN_AREA_VERDE', 'FL_AREA_VERDE', 'Int'), ('IN_AUDITORIO', 'FL_AUDITORIO', 'Int'), ('IN_BANHEIRO_FORA_PREDIO', 'FL_BANHEIRO_FORA_PREDIO', 'Int'), ('IN_BANHEIRO_DENTRO_PREDIO', 'FL_BANHEIRO_DENTRO_PREDIO', 'Int'), ('IN_BANHEIRO', 'FL_BANHEIRO', 'Int'), ('IN_BANHEIRO_EI', 'FL_BANHEIRO_EI', 'Int'), ('IN_BANHEIRO_PNE', 'FL_BANHEIRO_PNE', 'Int'), ('IN_BANHEIRO_FUNCIONARIOS', 'FL_BANHEIRO_FUNCIONARIOS', 'Int'), ('IN_BANHEIRO_CHUVEIRO', 'FL_BANHEIRO_CHUVEIRO', 'Int'), ('IN_BERCARIO', 'FL_BERCARIO', 'Int'), ('IN_BIBLIOTECA', 'FL_BIBLIOTECA', 'Int'), ('IN_BIBLIOTECA_SALA_LEITURA', 'FL_BIBLIOTECA_SALA_LEITURA', 'Int'), ('IN_COZINHA', 'FL_COZINHA', 'Int'), ('IN_DESPENSA', 'FL_DESPENSA', 'Int'), ('IN_DORMITORIO_ALUNO', 'FL_DORMITORIO_ALUNO', 'Int'), ('IN_DORMITORIO_PROFESSOR', 'FL_DORMITORIO_PROFESSOR', 'Int'), ('IN_LABORATORIO_CIENCIAS', 'FL_LABORATORIO_CIENCIAS', 'Int'), ('IN_LABORATORIO_INFORMATICA', 'FL_LABORATORIO_INFORMATICA', 'Int'), ('IN_PATIO_COBERTO', 'FL_PATIO_COBERTO', 'Int'), ('IN_PATIO_DESCOBERTO', 'FL_PATIO_DESCOBERTO', 'Int'), ('IN_PARQUE_INFANTIL', 'FL_PARQUE_INFANTIL', 'Int'), ('IN_PISCINA', 'FL_PISCINA', 'Int'), ('IN_QUADRA_ESPORTES', 'FL_QUADRA_ESPORTES', 'Int'), ('IN_QUADRA_ESPORTES_COBERTA', 'FL_QUADRA_ESPORTES_COBERTA', 'Int'), ('IN_QUADRA_ESPORTES_DESCOBERTA', 'FL_QUADRA_ESPORTES_DESCOBERTA', 'Int'), ('IN_REFEITORIO', 'FL_REFEITORIO', 'Int'), ('IN_SALA_ATELIE_ARTES', 'FL_SALA_ATELIE_ARTES', 'Int'), ('IN_SALA_MUSICA_CORAL', 'FL_SALA_MUSICA_CORAL', 'Int'), ('IN_SALA_ESTUDIO_DANCA', 'FL_SALA_ESTUDIO_DANCA', 'Int'), ('IN_SALA_MULTIUSO', 'FL_SALA_MULTIUSO', 'Int'), ('IN_SALA_DIRETORIA', 'FL_SALA_DIRETORIA', 'Int'), ('IN_SALA_LEITURA', 'FL_SALA_LEITURA', 'Int'), ('IN_SALA_PROFESSOR', 'FL_SALA_PROFESSOR', 'Int'), ('IN_SALA_REPOUSO_ALUNO', 'FL_SALA_REPOUSO_ALUNO', 'Int'), ('IN_SECRETARIA', 'FL_SECRETARIA', 'Int'), ('IN_SALA_ATENDIMENTO_ESPECIAL', 'FL_SALA_ATENDIMENTO_ESPECIAL', 'Int'), ('IN_TERREIRAO', 'FL_TERREIRAO', 'Int'), ('IN_VIVEIRO', 'FL_VIVEIRO', 'Int'), ('IN_DEPENDENCIAS_PNE', 'FL_DEPENDENCIAS_PNE', 'Int'), ('IN_LAVANDERIA', 'FL_LAVANDERIA', 'Int'), ('IN_DEPENDENCIAS_OUTRAS', 'FL_DEPENDENCIAS_OUTRAS', 'Int'), ('IN_ACESSIBILIDADE_CORRIMAO', 'FL_ACESSIBILIDADE_CORRIMAO', 'Int'), ('IN_ACESSIBILIDADE_ELEVADOR', 'FL_ACESSIBILIDADE_ELEVADOR', 'Int'), ('IN_ACESSIBILIDADE_PISOS_TATEIS', 'FL_ACESSIBILIDADE_PISOS_TATEIS', 'Int'), ('IN_ACESSIBILIDADE_VAO_LIVRE', 'FL_ACESSIBILIDADE_VAO_LIVRE', 'Int'), ('IN_ACESSIBILIDADE_RAMPAS', 'FL_ACESSIBILIDADE_RAMPAS', 'Int'), ('IN_ACESSIBILIDADE_SINAL_SONORO', 'FL_ACESSIBILIDADE_SINAL_SONORO', 'Int'), ('IN_ACESSIBILIDADE_SINAL_TATIL', 'FL_ACESSIBILIDADE_SINAL_TATIL', 'Int'), ('IN_ACESSIBILIDADE_SINAL_VISUAL', 'FL_ACESSIBILIDADE_SINAL_VISUAL', 'Int'), ('IN_ACESSIBILIDADE_INEXISTENTE', 'FL_ACESSIBILIDADE_INEXISTENTE', 'Int'), ('QT_SALAS_EXISTENTES', 'QT_SALAS_EXISTENTES', 'Int'), ('QT_SALAS_UTILIZADAS_DENTRO', 'QT_SALAS_UTILIZADAS_DENTRO', 'Int'), ('QT_SALAS_UTILIZADAS_FORA', 'QT_SALAS_UTILIZADAS_FORA', 'Int'), ('QT_SALAS_UTILIZADAS', 'QT_SALAS_UTILIZADAS', 'Int'), ('QT_SALAS_UTILIZA_CLIMATIZADAS', 'QT_SALAS_UTILIZA_CLIMATIZADAS', 'Int'), ('QT_SALAS_UTILIZADAS_ACESSIVEIS', 'QT_SALAS_UTILIZADAS_ACESSIVEIS', 'Int'), ('IN_EQUIP_PARABOLICA', 'FL_EQUIP_PARABOLICA', 'Int'), ('IN_COMPUTADOR', 'FL_COMPUTADOR', 'Int'), ('IN_EQUIP_COPIADORA', 'FL_EQUIP_COPIADORA', 'Int'), ('IN_EQUIP_IMPRESSORA', 'FL_EQUIP_IMPRESSORA', 'Int'), ('IN_EQUIP_IMPRESSORA_MULT', 'FL_EQUIP_IMPRESSORA_MULT', 'Int'), ('IN_EQUIP_SCANNER', 'FL_EQUIP_SCANNER', 'Int'), ('IN_EQUIP_DVD', 'FL_EQUIP_DVD', 'Int'), ('QT_EQUIP_DVD', 'QT_EQUIP_DVD', 'Int'), ('IN_EQUIP_SOM', 'FL_EQUIP_SOM', 'Int'), ('QT_EQUIP_SOM', 'QT_EQUIP_SOM', 'Int'), ('IN_EQUIP_TV', 'FL_EQUIP_TV', 'Int'), ('QT_EQUIP_TV', 'QT_EQUIP_TV', 'Int'), ('IN_EQUIP_LOUSA_DIGITAL', 'FL_EQUIP_LOUSA_DIGITAL', 'Int'), ('QT_EQUIP_LOUSA_DIGITAL', 'QT_EQUIP_LOUSA_DIGITAL', 'Int'), ('IN_EQUIP_MULTIMIDIA', 'FL_EQUIP_MULTIMIDIA', 'Int'), ('QT_EQUIP_MULTIMIDIA', 'QT_EQUIP_MULTIMIDIA', 'Int'), ('IN_EQUIP_VIDEOCASSETE', 'FL_EQUIP_VIDEOCASSETE', 'Int'), ('IN_EQUIP_RETROPROJETOR', 'FL_EQUIP_RETROPROJETOR', 'Int'), ('IN_EQUIP_FAX', 'FL_EQUIP_FAX', 'Int'), ('IN_EQUIP_FOTO', 'FL_EQUIP_FOTO', 'Int'), ('QT_EQUIP_VIDEOCASSETE', 'QT_EQUIP_VIDEOCASSETE', 'Int'), ('QT_EQUIP_PARABOLICA', 'QT_EQUIP_PARABOLICA', 'Int'), ('QT_EQUIP_COPIADORA', 'QT_EQUIP_COPIADORA', 'Int'), ('QT_EQUIP_RETROPROJETOR', 'QT_EQUIP_RETROPROJETOR', 'Int'), ('QT_EQUIP_IMPRESSORA', 'QT_EQUIP_IMPRESSORA', 'Int'), ('QT_EQUIP_IMPRESSORA_MULT', 'QT_EQUIP_IMPRESSORA_MULT', 'Int'), ('QT_EQUIP_FAX', 'QT_EQUIP_FAX', 'Int'), ('QT_EQUIP_FOTO', 'QT_EQUIP_FOTO', 'Int'), ('QT_COMP_ALUNO', 'QT_COMP_ALUNO', 'Int'), ('IN_DESKTOP_ALUNO', 'FL_DESKTOP_ALUNO', 'Int'), ('QT_DESKTOP_ALUNO', 'QT_DESKTOP_ALUNO', 'Int'), ('IN_COMP_PORTATIL_ALUNO', 'FL_COMP_PORTATIL_ALUNO', 'Int'), ('QT_COMP_PORTATIL_ALUNO', 'QT_COMP_PORTATIL_ALUNO', 'Int'), ('IN_TABLET_ALUNO', 'FL_TABLET_ALUNO', 'Int'), ('QT_TABLET_ALUNO', 'QT_TABLET_ALUNO', 'Int'), ('QT_COMPUTADOR', 'QT_COMPUTADOR', 'Int'), ('QT_COMP_ADMINISTRATIVO', 'QT_COMP_ADMINISTRATIVO', 'Int'), ('IN_INTERNET', 'FL_INTERNET', 'Int'), ('IN_INTERNET_ALUNOS', 'FL_INTERNET_ALUNOS', 'Int'), ('IN_INTERNET_ADMINISTRATIVO', 'FL_INTERNET_ADMINISTRATIVO', 'Int'), ('IN_INTERNET_APRENDIZAGEM', 'FL_INTERNET_APRENDIZAGEM', 'Int'), ('IN_INTERNET_COMUNIDADE', 'FL_INTERNET_COMUNIDADE', 'Int'), ('IN_ACESSO_INTERNET_COMPUTADOR', 'FL_ACESSO_INTERNET_COMPUTADOR', 'Int'), ('IN_ACES_INTERNET_DISP_PESSOAIS', 'FL_ACES_INTERNET_DISP_PESSOAIS', 'Int'), ('TP_REDE_LOCAL', 'TP_REDE_LOCAL', 'Int'), ('IN_BANDA_LARGA', 'FL_BANDA_LARGA', 'Int'), ('QT_FUNCIONARIOS', 'QT_FUNCIONARIOS', 'Int'), ('QT_PROF_ADMINISTRATIVOS', 'QT_PROF_ADMINISTRATIVOS', 'Int'), ('QT_PROF_SERVICOS_GERAIS', 'QT_PROF_SERVICOS_GERAIS', 'Int'), ('QT_PROF_BIBLIOTECARIO', 'QT_PROF_BIBLIOTECARIO', 'Int'), ('QT_PROF_SAUDE', 'QT_PROF_SAUDE', 'Int'), ('QT_PROF_COORDENADOR', 'QT_PROF_COORDENADOR', 'Int'), ('QT_PROF_FONAUDIOLOGO', 'QT_PROF_FONAUDIOLOGO', 'Int'), ('QT_PROF_NUTRICIONISTA', 'QT_PROF_NUTRICIONISTA', 'Int'), ('QT_PROF_PSICOLOGO', 'QT_PROF_PSICOLOGO', 'Int'), ('QT_PROF_ALIMENTACAO', 'QT_PROF_ALIMENTACAO', 'Int'), ('QT_PROF_PEDAGOGIA', 'QT_PROF_PEDAGOGIA', 'Int'), ('QT_PROF_SECRETARIO', 'QT_PROF_SECRETARIO', 'Int'), ('QT_PROF_SEGURANCA', 'QT_PROF_SEGURANCA', 'Int'), ('QT_PROF_MONITORES', 'QT_PROF_MONITORES', 'Int'), ('IN_ALIMENTACAO', 'FL_ALIMENTACAO', 'Int'), ('IN_SERIE_ANO', 'FL_SERIE_ANO', 'Int'), ('IN_PERIODOS_SEMESTRAIS', 'FL_PERIODOS_SEMESTRAIS', 'Int'), ('IN_FUNDAMENTAL_CICLOS', 'FL_FUNDAMENTAL_CICLOS', 'Int'), ('IN_GRUPOS_NAO_SERIADOS', 'FL_GRUPOS_NAO_SERIADOS', 'Int'), ('IN_MODULOS', 'FL_MODULOS', 'Int'), ('IN_FORMACAO_ALTERNANCIA', 'FL_FORMACAO_ALTERNANCIA', 'Int'), ('IN_MATERIAL_PED_MULTIMIDIA', 'FL_MATERIAL_PED_MULTIMIDIA', 'Int'), ('IN_MATERIAL_PED_INFANTIL', 'FL_MATERIAL_PED_INFANTIL', 'Int'), ('IN_MATERIAL_PED_CIENTIFICO', 'FL_MATERIAL_PED_CIENTIFICO', 'Int'), ('IN_MATERIAL_PED_DIFUSAO', 'FL_MATERIAL_PED_DIFUSAO', 'Int'), ('IN_MATERIAL_PED_MUSICAL', 'FL_MATERIAL_PED_MUSICAL', 'Int'), ('IN_MATERIAL_PED_JOGOS', 'FL_MATERIAL_PED_JOGOS', 'Int'), ('IN_MATERIAL_PED_ARTISTICAS', 'FL_MATERIAL_PED_ARTISTICAS', 'Int'), ('IN_MATERIAL_PED_DESPORTIVA', 'FL_MATERIAL_PED_DESPORTIVA', 'Int'), ('IN_MATERIAL_PED_INDIGENA', 'FL_MATERIAL_PED_INDIGENA', 'Int'), ('IN_MATERIAL_PED_ETNICO', 'FL_MATERIAL_PED_ETNICO', 'Int'), ('IN_MATERIAL_PED_CAMPO', 'FL_MATERIAL_PED_CAMPO', 'Int'), ('IN_MATERIAL_ESP_QUILOMBOLA', 'FL_MATERIAL_ESP_QUILOMBOLA', 'Int'), ('IN_MATERIAL_ESP_INDIGENA', 'FL_MATERIAL_ESP_INDIGENA', 'Int'), ('IN_MATERIAL_ESP_NAO_UTILZA', 'FL_MATERIAL_ESP_NAO_UTILZA', 'Int'), ('IN_EDUCACAO_INDIGENA', 'FL_EDUCACAO_INDIGENA', 'Int'), ('TP_INDIGENA_LINGUA', 'TP_INDIGENA_LINGUA', 'Int'), ('CO_LINGUA_INDIGENA_1', 'CO_LINGUA_INDIGENA_1', 'Int'), ('CO_LINGUA_INDIGENA_2', 'CO_LINGUA_INDIGENA_2', 'Int'), ('CO_LINGUA_INDIGENA_3', 'CO_LINGUA_INDIGENA_3', 'Int'), ('IN_BRASIL_ALFABETIZADO', 'FL_BRASIL_ALFABETIZADO', 'Int'), ('IN_FINAL_SEMANA', 'FL_FINAL_SEMANA', 'Int'), ('IN_EXAME_SELECAO', 'FL_EXAME_SELECAO', 'Int'), ('IN_RESERVA_PPI', 'FL_RESERVA_PPI', 'Int'), ('IN_RESERVA_RENDA', 'FL_RESERVA_RENDA', 'Int'), ('IN_RESERVA_PUBLICA', 'FL_RESERVA_PUBLICA', 'Int'), ('IN_RESERVA_PCD', 'FL_RESERVA_PCD', 'Int'), ('IN_RESERVA_OUTROS', 'FL_RESERVA_OUTROS', 'Int'), ('IN_RESERVA_NENHUMA', 'FL_RESERVA_NENHUMA', 'Int'), ('IN_REDES_SOCIAIS', 'FL_REDES_SOCIAIS', 'Int'), ('IN_ESPACO_ATIVIDADE', 'FL_ESPACO_ATIVIDADE', 'Int'), ('IN_ESPACO_EQUIPAMENTO', 'FL_ESPACO_EQUIPAMENTO', 'Int'), ('IN_ORGAO_ASS_PAIS', 'FL_ORGAO_ASS_PAIS', 'Int'), ('IN_ORGAO_ASS_PAIS_MESTRES', 'FL_ORGAO_ASS_PAIS_MESTRES', 'Int'), ('IN_ORGAO_CONSELHO_ESCOLAR', 'FL_ORGAO_CONSELHO_ESCOLAR', 'Int'), ('IN_ORGAO_GREMIO_ESTUDANTIL', 'FL_ORGAO_GREMIO_ESTUDANTIL', 'Int'), ('IN_ORGAO_OUTROS', 'FL_ORGAO_OUTROS', 'Int'), ('IN_ORGAO_NENHUM', 'FL_ORGAO_NENHUM', 'Int'), ('TP_PROPOSTA_PEDAGOGICA', 'TP_PROPOSTA_PEDAGOGICA', 'Int'), ('TP_AEE', 'TP_AEE', 'Int'), ('TP_ATIVIDADE_COMPLEMENTAR', 'TP_ATIVIDADE_COMPLEMENTAR', 'Int'), ('IN_MEDIACAO_PRESENCIAL', 'FL_MEDIACAO_PRESENCIAL', 'Int'), ('IN_MEDIACAO_SEMIPRESENCIAL', 'FL_MEDIACAO_SEMIPRESENCIAL', 'Int'), ('IN_MEDIACAO_EAD', 'FL_MEDIACAO_EAD', 'Int'), ('IN_ESPECIAL_EXCLUSIVA', 'FL_ESPECIAL_EXCLUSIVA', 'Int'), ('IN_REGULAR', 'FL_REGULAR', 'Int'), ('IN_EJA', 'FL_EJA', 'Int'), ('IN_PROFISSIONALIZANTE', 'FL_PROFISSIONALIZANTE', 'Int'), ('IN_COMUM_CRECHE', 'FL_COMUM_CRECHE', 'Int'), ('IN_COMUM_PRE', 'FL_COMUM_PRE', 'Int'), ('IN_COMUM_FUND_AI', 'FL_COMUM_FUND_AI', 'Int'), ('IN_COMUM_FUND_AF', 'FL_COMUM_FUND_AF', 'Int'), ('IN_COMUM_MEDIO_MEDIO', 'FL_COMUM_MEDIO_MEDIO', 'Int'), ('IN_COMUM_MEDIO_INTEGRADO', 'FL_COMUM_MEDIO_INTEGRADO', 'Int'), ('IN_COMUM_MEDIO_NORMAL', 'FL_COMUM_MEDIO_NORMAL', 'Int'), ('IN_ESP_EXCLUSIVA_CRECHE', 'FL_ESP_EXCLUSIVA_CRECHE', 'Int'), ('IN_ESP_EXCLUSIVA_PRE', 'FL_ESP_EXCLUSIVA_PRE', 'Int'), ('IN_ESP_EXCLUSIVA_FUND_AI', 'FL_ESP_EXCLUSIVA_FUND_AI', 'Int'), ('IN_ESP_EXCLUSIVA_FUND_AF', 'FL_ESP_EXCLUSIVA_FUND_AF', 'Int'), ('IN_ESP_EXCLUSIVA_MEDIO_MEDIO', 'FL_ESP_EXCLUSIVA_MEDIO_MEDIO', 'Int'), ('IN_ESP_EXCLUSIVA_MEDIO_INTEGR', 'FL_ESP_EXCLUSIVA_MEDIO_INTEGR', 'Int'), ('IN_ESP_EXCLUSIVA_MEDIO_NORMAL', 'FL_ESP_EXCLUSIVA_MEDIO_NORMAL', 'Int'), ('IN_COMUM_EJA_FUND', 'FL_COMUM_EJA_FUND', 'Int'), ('IN_COMUM_EJA_MEDIO', 'FL_COMUM_EJA_MEDIO', 'Int'), ('IN_COMUM_EJA_PROF', 'FL_COMUM_EJA_PROF', 'Int'), ('IN_ESP_EXCLUSIVA_EJA_FUND', 'FL_ESP_EXCLUSIVA_EJA_FUND', 'Int'), ('IN_ESP_EXCLUSIVA_EJA_MEDIO', 'FL_ESP_EXCLUSIVA_EJA_MEDIO', 'Int'), ('IN_ESP_EXCLUSIVA_EJA_PROF', 'FL_ESP_EXCLUSIVA_EJA_PROF', 'Int'), ('IN_COMUM_PROF', 'FL_COMUM_PROF', 'Int'), ('IN_ESP_EXCLUSIVA_PROF', 'FL_ESP_EXCLUSIVA_PROF', 'Int'),('ID_LINGUA_INDIGENA', 'FL_LINGUA_INDIGENA', 'string'),('ID_LINGUA_PORTUGUESA', 'FL_LINGUA_PORTUGUESA', 'string' )]

# COMMAND ----------

for column, renamed, _type in transformation:
  df_source = df_source.withColumn(column, col(column).cast(_type))
  df_source = df_source.withColumnRenamed(column, renamed)

# COMMAND ----------

df_source = df_source.join(df_escola,on=['CD_ENTIDADE','NR_ANO_CENSO'], how='left')

# COMMAND ----------

df_source = df_source.join(df_territorio, on='CD_MUNICIPIO', how='left')

# COMMAND ----------

df_source = df_source.join(df_turma, on=['CD_ENTIDADE','NR_ANO_CENSO'], how='left')

# COMMAND ----------

df_source = df_source.dropDuplicates()

# COMMAND ----------

df_source = df_source.withColumn('FL_ENERGIA_GERADOR_FOSSIL',df_source.FL_ENERGIA_GERADOR.cast('Int'))
df_source = df_source.withColumn('FL_ENERGIA_RENOVAVEL',df_source.FL_ENERGIA_OUTROS.cast('Int'))
df_source = df_source.withColumn('FL_LIXO_DESCARTA_OUTRA_AREA',df_source.FL_LIXO_JOGA_OUTRA_AREA.cast('Int'))
df_source = df_source.withColumn('FL_TRATAMENTO_LIXO_RECICLAGEM',df_source.FL_LIXO_RECICLA.cast('Int'))

# COMMAND ----------

df_2009_a_2014 = df_source.filter((df_source.NR_ANO_CENSO >= 2009) & (df_source.NR_ANO_CENSO <= 2014))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_CONVENIO_PODER_PUBLICO',\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 1, 2).\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 2, 1).otherwise(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_LOCAL_FUNC_PREDIO_ESCOLAR',\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_PREDIO_ESCOLAR == 0, 0).otherwise(lit(1)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_OCUPACAO_PREDIO_ESCOLAR',\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_PREDIO_ESCOLAR == 1, 1).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_PREDIO_ESCOLAR == 2, 2).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_PREDIO_ESCOLAR == 3, 3).otherwise(lit(None)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_LOCAL_FUNC_PRISIONAL_SOCIO',\
                  when((df_2009_a_2014.FL_LOCAL_FUNC_SOCIOEDUCATIVO == 1) | (df_2009_a_2014.FL_LOCAL_FUNC_UNID_PRISIONAL == 1), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_LOCAL_FUNC_GALPAO',\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 0, 0).otherwise(lit(1)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_OCUPACAO_GALPAO',\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 1, 1).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 2, 2).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 3, 3).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 4, 9).\
                  when(df_2009_a_2014.FL_LOCAL_FUNC_GALPAO == 9, 9).otherwise(lit(None)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_INDIGENA_LINGUA',\
                  when((df_2009_a_2014.FL_EDUCACAO_INDIGENA == lit(0)), lit(None)).\
                  when((df_2009_a_2014.FL_EDUCACAO_INDIGENA == lit(1)) & (df_2009_a_2014.FL_LINGUA_INDIGENA == lit(1)) &                          (df_2009_a_2014.FL_LINGUA_PORTUGUESA == lit(0)), lit(1)).\
                  when((df_2009_a_2014.FL_EDUCACAO_INDIGENA == lit(1)) & (df_2009_a_2014.FL_LINGUA_INDIGENA == lit(0)) & (df_2009_a_2014.FL_LINGUA_PORTUGUESA == lit(1)), lit(2)).\
                  when((df_2009_a_2014.FL_EDUCACAO_INDIGENA == lit(1)) & (df_2009_a_2014.FL_LINGUA_INDIGENA == lit(1)) & (df_2009_a_2014.FL_LINGUA_PORTUGUESA == lit(1)), lit(3)).otherwise(df_2009_a_2014.TP_INDIGENA_LINGUA))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_BANHEIRO',\
                     when((df_2009_a_2014.FL_BANHEIRO_FORA_PREDIO == 1) | (df_2009_a_2014.FL_BANHEIRO_DENTRO_PREDIO == 1), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_BIBLIOTECA_SALA_LEITURA',\
                     when((df_2009_a_2014.FL_BIBLIOTECA == 1) | (df_2009_a_2014.FL_SALA_LEITURA == 1), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_QUADRA_ESPORTES',\
                     when((df_2009_a_2014.FL_QUADRA_ESPORTES_COBERTA == 1) | (df_2009_a_2014.FL_QUADRA_ESPORTES_DESCOBERTA == 1), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_PARABOLICA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_PARABOLICA >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_PARABOLICA).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_COMPUTADOR',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_COMPUTADOR >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_COMPUTADOR).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_COPIADORA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_COPIADORA >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_COPIADORA).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_IMPRESSORA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_IMPRESSORA >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_IMPRESSORA).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_DVD',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_DVD >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_DVD).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_SOM',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_SOM >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_SOM).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_TV',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_TV >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_TV).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_MULTIMIDIA',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_MULTIMIDIA >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_MULTIMIDIA).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_VIDEOCASSETE',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_VIDEOCASSETE >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_VIDEOCASSETE).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_RETROPROJETOR',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_RETROPROJETOR >= lit(0)), 1).otherwise(when((col('nr_ano_censo') <= lit(2012)), df_2009_a_2014.FL_EQUIP_RETROPROJETOR).otherwise(lit(0))))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_FAX',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_FAX > lit(0)), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_EQUIP_FOTO',\
                  when((df_2009_a_2014.NR_ANO_CENSO.isin([2013,2014])) & (df_2009_a_2014.QT_EQUIP_FOTO > lit(0)), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('QT_COMP_ALUNO',(df_2009_a_2014.QT_DESKTOP_ALUNO + df_2009_a_2014.QT_COMP_PORTATIL_ALUNO))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_COMUM_EJA_PROF',lit(None))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_ESP_EXCLUSIVA_EJA_PROF',lit(None))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('FL_ESPECIAL_EXCLUSIVA',\
                  when(df_2009_a_2014.TP_AEE == 2, 1).otherwise(lit(df_2009_a_2014.FL_ESPECIAL_EXCLUSIVA)))

# COMMAND ----------

df_2015_a_2019 = df_source.filter(df_source.NR_ANO_CENSO >= 2015)

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('FL_DEPENDENCIAS_PNE',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.FL_ACESSIBILIDADE_INEXISTENTE == lit(1)), 0).otherwise(when((col('nr_ano_censo') < lit(2019)), df_2015_a_2019.FL_DEPENDENCIAS_PNE).otherwise(lit(1))))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('FL_BANHEIRO',\
                     when((df_2009_a_2014.NR_ANO_CENSO.isin([2015,2016,2017,2018])) & ((df_2015_a_2019.FL_BANHEIRO_FORA_PREDIO == 1) | (df_2015_a_2019.FL_BANHEIRO_DENTRO_PREDIO == 1)), 1).otherwise(when((col('nr_ano_censo') < lit(2019)), df_2015_a_2019.FL_BANHEIRO).otherwise(lit(0))))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_ADMINISTRATIVOS',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_ADMINISTRATIVOS == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_ADMINISTRATIVOS))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_SERVICOS_GERAIS',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_SERVICOS_GERAIS == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_SERVICOS_GERAIS))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_BIBLIOTECARIO',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_BIBLIOTECARIO == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_BIBLIOTECARIO))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_SAUDE',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_SAUDE == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_SAUDE))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_COORDENADOR',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_COORDENADOR == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_COORDENADOR))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_FONAUDIOLOGO',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_FONAUDIOLOGO == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_FONAUDIOLOGO))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_NUTRICIONISTA',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_NUTRICIONISTA == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_NUTRICIONISTA))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_PSICOLOGO',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_PSICOLOGO == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_PSICOLOGO))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_ALIMENTACAO',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_ALIMENTACAO == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_ALIMENTACAO))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_PEDAGOGIA',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_PEDAGOGIA == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_PEDAGOGIA))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_SECRETARIO',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_SECRETARIO == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_SECRETARIO))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_SEGURANCA',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_SEGURANCA == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_SEGURANCA))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_PROF_MONITORES',\
                  when((df_2015_a_2019.NR_ANO_CENSO == 2019) & (df_2015_a_2019.QT_PROF_MONITORES == lit(88888)), 0).otherwise(df_2015_a_2019.QT_PROF_MONITORES))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('QT_FUNCIONARIOS',(df_2015_a_2019.QT_PROF_ADMINISTRATIVOS + df_2015_a_2019.QT_PROF_SERVICOS_GERAIS + df_2015_a_2019.QT_PROF_BIBLIOTECARIO + df_2015_a_2019.QT_PROF_SAUDE + df_2015_a_2019.QT_PROF_COORDENADOR + df_2015_a_2019.QT_PROF_FONAUDIOLOGO + df_2015_a_2019.QT_PROF_NUTRICIONISTA + df_2015_a_2019.QT_PROF_PSICOLOGO + df_2015_a_2019.QT_PROF_ALIMENTACAO + df_2015_a_2019.QT_PROF_PEDAGOGIA + df_2015_a_2019.QT_PROF_SECRETARIO + df_2015_a_2019.QT_PROF_SEGURANCA + df_2015_a_2019.QT_PROF_MONITORES))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('FL_MATERIAL_ESP_NAO_UTILZA',\
                     when((df_2015_a_2019.NR_ANO_CENSO == 2019) & ((df_2015_a_2019.FL_MATERIAL_PED_ARTISTICAS == 1) | (df_2015_a_2019.FL_MATERIAL_PED_INDIGENA == 1) | (df_2015_a_2019.FL_MATERIAL_PED_ETNICO == 1)), 0).otherwise(when((col('nr_ano_censo') < lit(2019)), df_2015_a_2019.FL_MATERIAL_ESP_NAO_UTILZA).otherwise(lit(1))))

# COMMAND ----------

df = df_2009_a_2014.union(df_2015_a_2019)

# COMMAND ----------

df = df.withColumn('FL_MEDIACAO_PRESENCIAL',\
                  when(df.NR_ANO_CENSO <= 2014,df.FL_MEDIACAO_PRESENCIAL_TURMA).otherwise(df.FL_MEDIACAO_PRESENCIAL))

# COMMAND ----------

df = df.withColumn('FL_MEDIACAO_SEMIPRESENCIAL',\
                  when(df.NR_ANO_CENSO <= 2014,df.FL_MEDIACAO_SEMIPRESENCIAL_TURMA).otherwise(df.FL_MEDIACAO_SEMIPRESENCIAL))

# COMMAND ----------

df = df.withColumn('FL_PROFISSIONALIZANTE',\
                  when(df.NR_ANO_CENSO <= 2014,df.FL_PROFISSIONALIZANTE_TURMA).otherwise(df.FL_PROFISSIONALIZANTE))

# COMMAND ----------

df = df.drop('FL_PROFISSIONALIZANTE_TURMA','FL_MEDIACAO_PRESENCIAL_TURMA','FL_MEDIACAO_SEMIPRESENCIAL_TURMA')

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('nr_ano_censo').save(path=target, format="parquet", mode='overwrite')

# COMMAND ----------

#df.createOrReplaceTempView("tb_trs_escola")