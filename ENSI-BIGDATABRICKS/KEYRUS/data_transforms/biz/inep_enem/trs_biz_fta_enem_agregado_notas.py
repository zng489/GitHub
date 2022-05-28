# Databricks notebook source
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

import crawler.functions as cf

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
#   "path_origin": "inep_enem/microdados_enem",
#   "path_school": "inep_censo_escolar/censo_escolar_escola",
#   "path_base_school": "mtd/corp/base_escolas",
#   "path_enem_school": "inep_enem/enem_escola",
#   "path_eligible": "inep_enem/elegiveis",
#   "path_classes": "inep_enem/turmas",
#   "path_destination": "uniepro/fta_enem_agregado_notas",
#   "destination": "/uniepro/fta_enem_agregado_notas",
#   "databricks": {
#     "notebook": "/biz/inep_enem/trs_biz_fta_enem_agregado_notas"
#   }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err",
#                   "staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/trs","business":"/tmp/dev/biz"}}
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

source = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_origin"])
source

# COMMAND ----------

source_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_school"])
source_school

# COMMAND ----------

source_base_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_base_school"])
source_base_school

# COMMAND ----------

source_enem_school = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_enem_school"])
source_enem_school

# COMMAND ----------

source_elegible = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_eligible"])
source_elegible

# COMMAND ----------

source_classes = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_classes"])
source_classes

# COMMAND ----------

biz_target = "{biz}/{origin}".format(biz=biz, origin=tables["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source = df_source.where((f.col('NR_ANO') > f.lit(2015)) &
                            (f.col('TP_ST_CONCLUSAO') == f.lit(2)) &
                            (f.col('CD_ENTIDADE') > 0) &
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
    
df_source = (df_source
              .withColumn('CD_PERFIL', ((f.col('CD_UF_ESC') * f.lit(10000)) +
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
                                                                                       .otherwise(f.when(f.col('DS_ESCOLAR_MAE') == f.lit('H'), f.lit(0))))))))
              .withColumn('FL_EXCLUI_PERFIL', f.when((f.col('CD_UF_ESC').isNull()) | 
                                                     (f.col('TP_SEXO').isNull()) | 
                                                     (f.col('NR_IDADE').isNull()) | 
                                                     (f.col('TP_COR_RACA').isNull()) | 
                                                     (f.col('DS_ESCOLAR_MAE').isNull()), f.lit(1))
                                               .otherwise(f.lit(0)))
              .withColumn('NR_ANO', f.col('NR_ANO').cast('Int'))
              .withColumn('DT_ANO', f.col('NR_ANO'))
              .withColumn('FL_LAVANDERIA', f.lit(None).cast('Int')))

# COMMAND ----------

df_source_columns = ['CD_ENTIDADE', 'CD_MUNICIPIO_ESC', 'CD_MUNICIPIO_NASCIMENTO', 'CD_MUNICIPIO_PROVA', 'CD_MUNICIPIO_RESIDENCIA', 'CD_PROVA_CH', 'CD_PROVA_CN', 'CD_PROVA_LC', 'CD_PROVA_MT', 'CD_UF_ENTIDADE_CERTIFICACAO', 'CD_UF_ESC', 'CD_UF_NASCIMENTO', 'CD_UF_PROVA', 'CD_UF_RESIDENCIA', 'DS_COR', 'DS_ESCOLAR_MAE', 'FL_ACESSO', 'FL_AMPLIADA_18', 'FL_AMPLIADA_24', 'FL_APOIO_PERNA', 'FL_AUTISMO', 'FL_BAIXA_VISAO', 'FL_BRAILLE', 'FL_CADEIRA_ACOLCHOADA', 'FL_CADEIRA_CANHOTO', 'FL_CADEIRA_ESPECIAL', 'FL_CEGUEIRA', 'FL_CERTIFICADO', 'FL_COMPUTADOR', 'FL_DEFICIENCIA_AUDITIVA', 'FL_DEFICIENCIA_FISICA', 'FL_DEFICIENCIA_MENTAL', 'FL_DEFICIT_ATENCAO', 'FL_DISCALCULIA', 'FL_DISLEXIA', 'FL_ESTUDA_CLASSE_HOSPITALAR', 'FL_GESTANTE', 'FL_GUIA_INTERPRETE', 'FL_IDOSO', 'FL_LACTANTE', 'FL_LAMINA_OVERLAY', 'FL_LEDOR', 'FL_LEITURA_LABIAL', 'FL_LIBRAS', 'FL_MACA', 'FL_MAQUINA_BRAILE', 'FL_MARCA_PASSO', 'FL_MATERIAL_ESPECIFICO', 'FL_MEDICAMENTOS', 'FL_MEDIDOR_GLICOSE', 'FL_MESA_CADEIRA_RODAS', 'FL_MESA_CADEIRA_SEPARADA', 'FL_MOBILIARIO_ESPECIFICO', 'FL_MOBILIARIO_OBESO', 'FL_NOME_SOCIAL', 'FL_OUTRA_DEF', 'FL_PROTETOR_AURICULAR', 'FL_PROVA_DEITADO', 'FL_SABATISTA', 'FL_SALA_ACOMPANHANTE', 'FL_SALA_ESPECIAL', 'FL_SALA_INDIVIDUAL', 'FL_SEM_RECURSO', 'FL_SONDA', 'FL_SOROBAN', 'FL_SURDEZ', 'FL_SURDO_CEGUEIRA', 'FL_TRANSCRICAO', 'FL_TREINEIRO', 'FL_VISAO_MONOCULAR', 'DS_GENERO', 'DS_IDADE', 'NM_ENTIDADE_CERTIFICACAO', 'NM_MUNICIPIO_ESC', 'NM_MUNICIPIO_NASCIMENTO', 'NM_MUNICIPIO_PROVA', 'NM_MUNICIPIO_RESIDENCIA', 'NR_IDADE', 'NR_INSCRICAO', 'NR_NOTA_CH', 'NR_NOTA_CN', 'NR_NOTA_COMP1', 'NR_NOTA_COMP2', 'NR_NOTA_COMP3', 'NR_NOTA_COMP4', 'NR_NOTA_COMP5', 'NR_NOTA_LC', 'NR_NOTA_MT', 'NR_NOTA_REDACAO', 'SG_UF_ENTIDADE_CERTIFICACAO', 'SG_UF_ESC', 'SG_UF_NASCIMENTO', 'SG_UF_PROVA', 'SG_UF_RESIDENCIA', 'TP_ANO_CONCLUIU', 'TP_COR_RACA', 'TP_DEPENDENCIA_ADM_ESC', 'TP_ENSINO', 'TP_ESCOLA', 'TP_ESTADO_CIVIL', 'TP_LINGUA', 'TP_LOCALIZACAO_ESC', 'TP_NACIONALIDADE', 'TP_PRESENCA_CH', 'TP_PRESENCA_CN', 'TP_PRESENCA_LC', 'TP_PRESENCA_MT', 'TP_SEXO', 'TP_SIT_FUNC_ESC', 'TP_ST_CONCLUSAO', 'TP_STATUS_REDACAO', 'TX_GABARITO_CH', 'TX_GABARITO_CN', 'TX_GABARITO_LC', 'TX_GABARITO_MT', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_CN', 'TX_RESPOSTAS_LC', 'TX_RESPOSTAS_MT']

df_source_renamed_columns = [('VL_Q001_NIVEL_ESTUDO_PAI_HOMEM_RESP', 'VL_Q001'), ('VL_Q002_NIVEL_ESTUDO_MAE_MULHER_RESP', 'VL_Q002'), ('VL_Q003_GRP_OCUPACAO_PAI_HOMEM_RESP', 'VL_Q003'), ('VL_Q004_GRP_OCUPACAO_MAE_MULHER_RESP', 'VL_Q004'), ('VL_Q005_QTD_PESSOAS_RESIDENCIA', 'VL_Q005'), ('VL_Q006_RENDA_MENSAL_FAMIILIA', 'VL_Q006'), ('VL_Q007_RESIDENCIA_TEM_EMPREGADO_DOMEST', 'VL_Q007'), ('VL_Q008_RESIDENCIA_POSSUI_BANHEIRO', 'VL_Q008'), ('VL_Q009_RESIDENCIA_POSSUI_QUARTOS', 'VL_Q009'), ('VL_Q010_RESIDENCIA_POSSUI_CARRO', 'VL_Q010'), ('VL_Q011_RESIDENCIA_POSSUI_MOTO', 'VL_Q011'), ('VL_Q012_RESIDENCIA_POSSUI_GELADEIRA', 'VL_Q012'), ('VL_Q013_RESIDENCIA_POSSUI_FREEZER', 'VL_Q013'), ('VL_Q014_RESIDENCIA_POSSUI_MAQ_LAVAR_ROUPA', 'VL_Q014'), ('VL_Q015_RESIDENCIA_POSSUI_MAQ_SECAR', 'VL_Q015'), ('VL_Q016_RESIDENCIA_POSSUI_MICROONDAS', 'VL_Q016'), ('VL_Q017_RESIDENCIA_POSSUI_MAQ_LAVAR_LOUCA', 'VL_Q017'), ('VL_Q018_RESIDENCIA_POSSUI_ASPIRADOR', 'VL_Q018'), ('VL_Q019_RESIDENCIA_POSSUI_TV', 'VL_Q019'), ('VL_Q020_RESIDENCIA_POSSUI_DVD', 'VL_Q020'), ('VL_Q021_RESIDENCIA_POSSUI_TV_ACABO', 'VL_Q021'), ('VL_Q022_RESIDENCIA_POSSUI_CELULAR', 'VL_Q022'), ('VL_Q023_RESIDENCIA_POSSUI_TELEFONE', 'VL_Q023'), ('VL_Q024_RESIDENCIA_POSSUI_COMPUTADOR', 'VL_Q024'), ('VL_Q025_RESIDENCIA_POSSUI_INTERNET', 'VL_Q025'), ('VL_Q026_ENSINO_MEDIO_CONCLUIDO', 'VL_Q026'), ('VL_Q027_TIPO_ESCOLA_ENSINO_MEDIO', 'VL_Q027'), ('VL_Q028_QTD_HORAS_SEMANAIS_TRAB', 'VL_Q028'), ('VL_Q029_IND_MOTIVO_TRABALHO_AJUDAR_PAIS', 'VL_Q029'), ('VL_Q030_IND_MOTIVO_TRABALHO_SUSTENTAR_FAMILIA', 'VL_Q030'), ('VL_Q031_IND_MOTIVO_TRABALHO_GANHO_FINANC_PROPRIO', 'VL_Q031'), ('VL_Q032_IND_MOTIVO_TRABALHO_GANHO_EXPERIENCIA', 'VL_Q032'), ('VL_Q033_IND_MOTIVO_TRABALHO_PAGAR_ESTUDOS', 'VL_Q033'), ('VL_Q034_IND_MOTIVO_ENEM_TESTAR_CONHECIMENTO', 'VL_Q034'), ('VL_Q035_IND_MOTIVO_ENEM_INGRESSO_EDUC_SUP_PUBLICA', 'VL_Q035'), ('VL_Q036_IND_MOTIVO_ENEM_INGRESSO_EDUC_SUP_PRIVADA', 'VL_Q036'), ('VL_Q037_IND_MOTIVO_ENEM_BOLSA_EDUC_SUPERIOR', 'VL_Q037'), ('VL_Q038_IND_MOTIVO_ENEM_FIES_EDUC_SUPERIOR', 'VL_Q038'), ('VL_Q039_IND_MOTIVO_ENEM_CIENCIAS_SEM_FRONTEIRAS', 'VL_Q039'), ('VL_Q040_IND_MOTIVO_ENEM_CONSEGUIR_EMPREGO', 'VL_Q040'), ('VL_Q041_IND_MOTIVO_ENEM_PROGREDIR_EMPREGO', 'VL_Q041'), ('VL_Q042_TIPO_ESCOLA_ENSINO_FUNDAMENTAL', 'VL_Q042'), ('VL_Q043_MODALIDADE_ENSINO_FUNDAMENTAL', 'VL_Q043'), ('VL_Q044_TURNO_ENSINO_FUNDAMENTAL', 'VL_Q044'), ('VL_Q045_FOI_REPROVADO_ABANDONOU_EF', 'VL_Q045'), ('VL_Q048_MODALIDADE_ENSINO_MEDIO', 'VL_Q048'), ('VL_Q049_TURNO_ENSINO_MEDIO', 'VL_Q049'), ('VL_Q050_FOI_REPROVADO_ABANDONOU_EM', 'VL_Q050')]

df_source_renamed_columns = (f.col(old_name).alias(new_name) for old_name, new_name in df_source_renamed_columns)
df_source = df_source.select('CD_PERFIL', 'FL_EXCLUI_PERFIL', 'NR_ANO', 'DT_ANO', 'FL_LAVANDERIA', *df_source_columns, *df_source_renamed_columns)

# COMMAND ----------

df_school = spark.read.parquet(source_school)
df_school = (df_school
             .withColumn('CD_NOME_ENTIDADE', f.col('NO_ENTIDADE'))
             .withColumn('NR_ESCOLAS_W', f.lit(None).cast('Int')))

df_school_columns = ['CD_DISTRITO', 'CD_MUNICIPIO', 'CD_UF', 'FL_AGUA_CACIMBA', 'FL_AGUA_FILTRADA', 'FL_AGUA_FONTE_RIO', 'FL_AGUA_INEXISTENTE', 'FL_AGUA_POCO_ARTESIANO', 'FL_AGUA_REDE_PUBLICA', 'FL_ALIMENTACAO', 'FL_ALMOXARIFADO', 'FL_AREA_VERDE', 'FL_AUDITORIO', 'FL_BANDA_LARGA', 'FL_BANHEIRO_CHUVEIRO', 'FL_BANHEIRO_DENTRO_PREDIO', 'FL_BANHEIRO_EI', 'FL_BANHEIRO_FORA_PREDIO', 'FL_BANHEIRO_PNE', 'FL_BERCARIO', 'FL_BIBLIOTECA', 'FL_BIBLIOTECA_SALA_LEITURA', 'FL_CONVENIADA_PP', 'FL_COZINHA', 'FL_DEPENDENCIAS_OUTRAS', 'FL_DEPENDENCIAS_PNE', 'FL_DESPENSA', 'FL_ENERGIA_GERADOR', 'FL_ENERGIA_INEXISTENTE', 'FL_ENERGIA_OUTROS', 'FL_ENERGIA_REDE_PUBLICA', 'FL_EQUIP_COPIADORA', 'FL_EQUIP_DVD', 'FL_EQUIP_FAX', 'FL_EQUIP_FOTO', 'FL_EQUIP_IMPRESSORA', 'FL_EQUIP_IMPRESSORA_MULT', 'FL_EQUIP_MULTIMIDIA', 'FL_EQUIP_PARABOLICA', 'FL_EQUIP_RETROPROJETOR', 'FL_EQUIP_SOM', 'FL_EQUIP_TV', 'FL_EQUIP_VIDEOCASSETE', 'FL_ESGOTO_FOSSA', 'FL_ESGOTO_INEXISTENTE', 'FL_ESGOTO_REDE_PUBLICA', 'FL_INTERNET', 'FL_LABORATORIO_CIENCIAS', 'FL_LABORATORIO_INFORMATICA', 'FL_LIXO_ENTERRA', 'FL_LIXO_JOGA_OUTRA_AREA', 'FL_LIXO_OUTROS', 'FL_LIXO_QUEIMA', 'FL_LIXO_RECICLA', 'FL_LOCAL_FUNC_CASA_PROFESSOR', 'FL_LOCAL_FUNC_GALPAO', 'FL_LOCAL_FUNC_OUTROS', 'FL_LOCAL_FUNC_PREDIO_ESCOLAR', 'FL_LOCAL_FUNC_PRISIONAL_SOCIO', 'FL_LOCAL_FUNC_SALAS_EMPRESA', 'FL_LOCAL_FUNC_SALAS_OUTRA_ESC', 'FL_LOCAL_FUNC_SOCIOEDUCATIVO', 'FL_LOCAL_FUNC_TEMPLO_IGREJA', 'FL_LOCAL_FUNC_UNID_PRISIONAL', 'FL_MANT_ESCOLA_PRIVADA_EMP', 'FL_MANT_ESCOLA_PRIVADA_ONG', 'FL_MANT_ESCOLA_PRIVADA_SIND', 'FL_MANT_ESCOLA_PRIVADA_SIST_S', 'FL_PARQUE_INFANTIL', 'FL_PATIO_COBERTO', 'FL_PATIO_DESCOBERTO', 'FL_PREDIO_COMPARTILHADO', 'FL_QUADRA_ESPORTES', 'FL_QUADRA_ESPORTES_COBERTA', 'FL_QUADRA_ESPORTES_DESCOBERTA', 'FL_REFEITORIO', 'FL_SALA_ATENDIMENTO_ESPECIAL', 'FL_SALA_DIRETORIA', 'FL_SALA_LEITURA', 'FL_SALA_PROFESSOR', 'FL_SECRETARIA', 'TP_AEE', 'TP_ATIVIDADE_COMPLEMENTAR', 'TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_CONVENIO_PODER_PUBLICO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'TP_OCUPACAO_GALPAO', 'TP_OCUPACAO_PREDIO_ESCOLAR', 'TP_SITUACAO_FUNCIONAMENTO']

df_school_renamed_columns = [('CD_MESORREGIAO_GEOGRAFICA', 'CD_MESORREGIAO'), ('CD_MICRORREGIAO_GEOGRAFICA', 'CD_MICRORREGIAO'), ('CD_REGIAO_GEOGRAFICA', 'CD_REGIAO'), ('QT_COMP_ADMINISTRATIVO', 'NR_COMP_ADMINISTRATIVO'), ('QT_COMP_ALUNO', 'NR_COMP_ALUNO'), ('QT_COMPUTADOR', 'NR_COMPUTADOR'), ('QT_EQUIP_COPIADORA', 'NR_EQUIP_COPIADORA'), ('QT_EQUIP_DVD', 'NR_EQUIP_DVD'), ('QT_EQUIP_FAX', 'NR_EQUIP_FAX'), ('QT_EQUIP_FOTO', 'NR_EQUIP_FOTO'), ('QT_EQUIP_IMPRESSORA', 'NR_EQUIP_IMPRESSORA'), ('QT_EQUIP_IMPRESSORA_MULT', 'NR_EQUIP_IMPRESSORA_MULT'), ('QT_EQUIP_MULTIMIDIA', 'NR_EQUIP_MULTIMIDIA'), ('QT_EQUIP_PARABOLICA', 'NR_EQUIP_PARABOLICA'), ('QT_EQUIP_RETROPROJETOR', 'NR_EQUIP_RETROPROJETOR'), ('QT_EQUIP_SOM', 'NR_EQUIP_SOM'), ('QT_EQUIP_TV', 'NR_EQUIP_TV'), ('QT_EQUIP_VIDEOCASSETE', 'NR_EQUIP_VIDEOCASSETE'), ('QT_FUNCIONARIOS', 'NR_FUNCIONARIOS'), ('QT_SALAS_EXISTENTES', 'NR_SALAS_EXISTENTES'), ('QT_SALAS_UTILIZADAS', 'NR_SALAS_UTILIZADAS')]

df_school_renamed_columns = (f.col(old_name).alias(new_name) for old_name, new_name in df_school_renamed_columns)
df_school = df_school.select('CD_NOME_ENTIDADE','NR_ESCOLAS_W', 
                             *df_school_columns, *df_school_renamed_columns, 'CD_ENTIDADE', f.col('NR_ANO_CENSO').alias('NR_ANO'))

# COMMAND ----------

df_base_school = spark.read.parquet(source_base_school)
df_base_school_columns = [('nr_cep', 'NR_CEP'), ('ds_complemento', 'DS_COMPLEMENTO'), ('ds_endereco', 'DS_ENDERECO'), ('fl_escola_senai', 'FL_ESCOLA_SENAI'), ('fl_escola_sesi', 'FL_ESCOLA_SESI'), ('cd_dep_adm_escola_sesi_senai', 'FL_ESCOLA_SESI_SENAI'), ('nm_bairro', 'NM_BAIRRO'), ('nm_entidade', 'NM_NOME_ESCOLA'), ('nm_nome_unidade', 'NM_NOME_UNIDADE'), ('nm_nomedomunicipio', 'NM_NOMEDOMUNICÍPIO'), ('ds_numeracao', 'NR_NUMERACAO')]

df_base_school_columns = (f.col(old_name).alias(new_name) for old_name, new_name in df_base_school_columns)
df_base_school = df_base_school.select(*df_base_school_columns, 'CD_ENTIDADE')

df_base_school = df_base_school.withColumn('FL_ESCOLA_SESI_SENAI', (f.col('FL_ESCOLA_SESI_SENAI') > 10).cast('Int'))

# COMMAND ----------

df_enem_school = spark.read.parquet(source_enem_school)
df_enem_school = df_enem_school.where(f.col('NR_ANO') > f.lit(2008))

df_enem_school_columns = ['DS_INSE', 'NM_ESCOLA_EDUCACENSO', 'NR_ANO', 'NR_MATRICULAS', 'NR_MEDIA_TOT', 'NR_PARTICIPANTES', 'NR_PARTICIPANTES_NEC_ESP', 'NR_TAXA_ABANDONO', 'NR_TAXA_APROVACAO', 'NR_TAXA_PARTICIPACAO', 'NR_TAXA_PERMANENCIA', 'NR_TAXA_REPROVACAO', 'PC_FORMACAO_DOCENTE', 'DS_PORTE_ESCOLA']

df_enem_school_renamed_columns = [('CD_ESCOLA_EDUCACENSO', 'CD_ENTIDADE'), ('CD_MUNICIPIO_ESCOLA', 'CD_MUNICIPIO_ESC'), ('CD_UF_ESCOLA', 'CD_UF_ESC'), ('NR_ANO', 'DT_ANO'), ('NM_MUNICIPIO_ESCOLA', 'NM_MUNICIPIO_ESC'), ('NR_MEDIA_CH', 'NR_NOTA_CH'), ('NR_MEDIA_CH', 'NR_NOTA_CH_MEAN'), ('NR_MEDIA_CN', 'NR_NOTA_CN'), ('NR_MEDIA_CN', 'NR_NOTA_CN_MEAN'), ('NR_MEDIA_LP', 'NR_NOTA_LC'), ('NR_MEDIA_LP', 'NR_NOTA_LC_MEAN'), ('NR_MEDIA_MT', 'NR_NOTA_MT'), ('NR_MEDIA_MT', 'NR_NOTA_MT_MEAN'), ('NR_MEDIA_RED', 'NR_NOTA_REDACAO'), ('NR_MEDIA_RED', 'NR_NOTA_REDACAO_MEAN'), ('SG_UF_ESCOLA', 'SG_UF_ESC'), ('TP_DEPENDENCIA_ADM_ESCOLA', 'TP_DEPENDENCIA_ADM_ESC'), ('TP_LOCALIZACAO_ESCOLA', 'TP_LOCALIZACAO_ESC')]

df_enem_school_renamed_columns = (f.col(old_name).alias(new_name) for old_name, new_name in df_enem_school_renamed_columns)
df_enem_school = df_enem_school.select(*df_enem_school_columns, *df_enem_school_renamed_columns)

df_enem_school = (df_enem_school
                  .withColumn('NR_NOTA_GERAL', f.round((f.col('NR_NOTA_CH') + f.col('NR_NOTA_CN') + f.col('NR_NOTA_LC') + f.col('NR_NOTA_MT')) / f.lit(4), 5))
                  .withColumn('NR_NOTA_GERAL_REDAC', f
                              .when(f.col('NR_NOTA_REDACAO') > 0, f.round((f.col('NR_NOTA_CH') + f.col('NR_NOTA_CN') + f.col('NR_NOTA_LC') + f.col('NR_NOTA_MT') + f.col('NR_NOTA_REDACAO')) / f.lit(5), 5))
                              .otherwise(f.col('NR_NOTA_GERAL'))))

# COMMAND ----------

df_elegible = spark.read.parquet(source_elegible)
df_elegible = df_elegible.select(f.col('NU_NOTA_GERAL').alias('NR_MEDIA_OBJ'),
                                 f.col('NU_NOTA_GERAL').alias('NR_NOTA_ENEM_GERAL_ESCOLA_SEM_REDAC'),
                                 f.col('NU_NOTA_GERAL').alias('NR_NOTA_GERAL'),
                                 f.col('NU_NOTA_GERAL_REDAC').alias('NR_NOTA_ENEM_GERAL_ESCOLA_COM_REDAC'),
                                 f.col('NU_NOTA_GERAL_REDAC').alias('NR_NOTA_GERAL_REDAC'),
                                 f.col('VL_TOT_ALUNOS_ENEM_ESCOLA').alias('VL_TOT_ALUNOS_ENEM_ESCOLA'),
                                 f.col('CO_ENTIDADE').alias('CD_ENTIDADE'), f.col('NU_ANO').alias('NR_ANO'))

# COMMAND ----------

df_classes = spark.read.parquet(source_classes)
df_classes = (df_classes
              .withColumn('FL_ESCOLA_FORA', f.when(f.col('MATRIC_CONC').isNull(), f.lit(1)).otherwise(f.lit(0)))
              .withColumn('FL_MATRIC10', f.when(f.col('MATRIC_CONC') > f.lit(10), f.lit(1)).otherwise(f.lit(0))))

df_classes_columns = [('COM_3E4', 'VL_COM_3E4'), ('mat_3ano', 'QT_MATRIC_EM3'), ('mat_4ano', 'QT_MATRIC_EM4'), ('NU_ANO_CENSO', 'NR_ANM_CENSO'), ('MATRIC_CONC', 'NR_MATRIC_CONC')]
df_classes_columns = (f.col(old_name).alias(new_name) for old_name, new_name in df_classes_columns)
df_classes = df_classes.select('FL_ESCOLA_FORA', 'FL_MATRIC10', *df_classes_columns, 
                               f.col('co_entidade').alias('CD_ENTIDADE'), f.col('NU_ANO_CENSO').alias('NR_ANO'))

# COMMAND ----------

df = df_source.join(df_elegible, on=['CD_ENTIDADE', 'NR_ANO'], how='left')
df = df.join(df_classes, on=['CD_ENTIDADE', 'NR_ANO'], how='left')

# COMMAND ----------

source_columns_diff = set(df.columns) - set(df_enem_school.columns)
school_columns_diff = set(df_enem_school.columns) - set(df.columns)

source_types = dict(df.dtypes)
school_types = dict(df_enem_school.dtypes)

for na in source_columns_diff:
  df_enem_school = df_enem_school.withColumn(na, f.lit(None).cast(source_types[na]))
  
for na in school_columns_diff:
  df = df.withColumn(na, f.lit(None).cast(school_types[na]))
  
df = df.union(df_enem_school.select(*df.columns))

# COMMAND ----------

df = df.join(df_school, on=['CD_ENTIDADE', 'NR_ANO'], how='left')
df = df.join(df_base_school, on='CD_ENTIDADE', how='left')

df = (df
      .withColumn('NM_NOME_ESCOLA', f.when(f.col('NM_NOME_ESCOLA').isNull(), f.col('NM_ESCOLA_EDUCACENSO')).otherwise(f.col('NM_NOME_ESCOLA')))
      .withColumn('NM_ENTIDADE', f.col('NM_NOME_ESCOLA')))

# COMMAND ----------

w = Window.partitionBy('NR_ANO', 'CD_ENTIDADE')

df = (df
      .withColumn('DT_ANO', f.concat_ws('-', f.col('NR_ANO'), f.lit('01'), f.lit('01')).cast('Date'))
      .withColumn('NR_PARTICIPANTES', f.when((f.col('NR_ANO') >= f.lit(2009)) & (f.col('NR_ANO') < f.lit(2016)), f.col('NR_PARTICIPANTES')).otherwise(f.lit(1)))
      .withColumn('NR_MATRICULAS', f.when((f.col('NR_ANO') >= f.lit(2009)) & (f.col('NR_ANO') < f.lit(2016)), f.col('NR_MATRICULAS')).otherwise(f.col('NR_MATRIC_CONC') / f.sum('NR_PARTICIPANTES').over(w)))
      .withColumn('NR_TAXA_PARTICIPACAO', 
                   f.when((f.col('NR_ANO') >= f.lit(2009)) & (f.col('NR_ANO') < 2016), f.col('NR_TAXA_PARTICIPACAO'))
                   .otherwise(f.col('VL_TOT_ALUNOS_ENEM_ESCOLA') / f.col('NR_MATRIC_CONC') * f.lit(100)))
      .withColumn('FL_ESCOLA_SESI_SENAI', f.when(f.col('FL_ESCOLA_SESI_SENAI').isNull(), f.lit(0)).otherwise(f.col('FL_ESCOLA_SESI_SENAI')))
      .withColumn('FL_ESCOLA_SELECIONADA', f.when((f.col('NR_ANO') >= f.lit(2009)) & (f.col('NR_ANO') < 2016), f.lit(1))
                                            .otherwise(f.when((f.col('VL_TOT_ALUNOS_ENEM_ESCOLA') >= f.lit(10)) & (f.col('NR_TAXA_PARTICIPACAO') >= f.lit(50)), f.lit(1))
                                                        .otherwise(f.lit(0)))))

# COMMAND ----------

ordered_columns = ['CD_DISTRITO', 'CD_ENTIDADE', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'CD_MUNICIPIO', 'CD_MUNICIPIO_ESC', 'CD_MUNICIPIO_NASCIMENTO', 'CD_MUNICIPIO_PROVA', 'CD_MUNICIPIO_RESIDENCIA', 'CD_NOME_ENTIDADE', 'CD_PERFIL', 'CD_PROVA_CH', 'CD_PROVA_CN', 'CD_PROVA_LC', 'CD_PROVA_MT', 'CD_REGIAO', 'CD_UF', 'CD_UF_ENTIDADE_CERTIFICACAO', 'CD_UF_ESC', 'CD_UF_NASCIMENTO', 'CD_UF_PROVA', 'CD_UF_RESIDENCIA', 'NR_CEP', 'VL_COM_3E4', 'DS_COMPLEMENTO', 'DS_COR', 'DS_ENDERECO', 'DT_ANO', 'FL_ESCOLA_FORA', 'FL_ESCOLA_SELECIONADA', 'FL_ESCOLA_SENAI', 'FL_ESCOLA_SESI', 'FL_ESCOLA_SESI_SENAI', 'DS_ESCOLAR_MAE', 'FL_EXCLUI_PERFIL', 'FL_ACESSO', 'FL_AGUA_CACIMBA', 'FL_AGUA_FILTRADA', 'FL_AGUA_FONTE_RIO', 'FL_AGUA_INEXISTENTE', 'FL_AGUA_POCO_ARTESIANO', 'FL_AGUA_REDE_PUBLICA', 'FL_ALIMENTACAO', 'FL_ALMOXARIFADO', 'FL_AMPLIADA_18', 'FL_AMPLIADA_24', 'FL_APOIO_PERNA', 'FL_AREA_VERDE', 'FL_AUDITORIO', 'FL_AUTISMO', 'FL_BAIXA_VISAO', 'FL_BANDA_LARGA', 'FL_BANHEIRO_CHUVEIRO', 'FL_BANHEIRO_DENTRO_PREDIO', 'FL_BANHEIRO_EI', 'FL_BANHEIRO_FORA_PREDIO', 'FL_BANHEIRO_PNE', 'FL_BERCARIO', 'FL_BIBLIOTECA', 'FL_BIBLIOTECA_SALA_LEITURA', 'FL_BRAILLE', 'FL_CADEIRA_ACOLCHOADA', 'FL_CADEIRA_CANHOTO', 'FL_CADEIRA_ESPECIAL', 'FL_CEGUEIRA', 'FL_CERTIFICADO', 'FL_COMPUTADOR', 'FL_CONVENIADA_PP', 'FL_COZINHA', 'FL_DEFICIENCIA_AUDITIVA', 'FL_DEFICIENCIA_FISICA', 'FL_DEFICIENCIA_MENTAL', 'FL_DEFICIT_ATENCAO', 'FL_DEPENDENCIAS_OUTRAS', 'FL_DEPENDENCIAS_PNE', 'FL_DESPENSA', 'FL_DISCALCULIA', 'FL_DISLEXIA', 'FL_ENERGIA_GERADOR', 'FL_ENERGIA_INEXISTENTE', 'FL_ENERGIA_OUTROS', 'FL_ENERGIA_REDE_PUBLICA', 'FL_EQUIP_COPIADORA', 'FL_EQUIP_DVD', 'FL_EQUIP_FAX', 'FL_EQUIP_FOTO', 'FL_EQUIP_IMPRESSORA', 'FL_EQUIP_IMPRESSORA_MULT', 'FL_EQUIP_MULTIMIDIA', 'FL_EQUIP_PARABOLICA', 'FL_EQUIP_RETROPROJETOR', 'FL_EQUIP_SOM', 'FL_EQUIP_TV', 'FL_EQUIP_VIDEOCASSETE', 'FL_ESGOTO_FOSSA', 'FL_ESGOTO_INEXISTENTE', 'FL_ESGOTO_REDE_PUBLICA', 'FL_ESTUDA_CLASSE_HOSPITALAR', 'FL_GESTANTE', 'FL_GUIA_INTERPRETE', 'FL_IDOSO', 'FL_INTERNET', 'FL_LABORATORIO_CIENCIAS', 'FL_LABORATORIO_INFORMATICA', 'FL_LACTANTE', 'FL_LAMINA_OVERLAY', 'FL_LAVANDERIA', 'FL_LEDOR', 'FL_LEITURA_LABIAL', 'FL_LIBRAS', 'FL_LIXO_ENTERRA', 'FL_LIXO_JOGA_OUTRA_AREA', 'FL_LIXO_OUTROS', 'FL_LIXO_QUEIMA', 'FL_LIXO_RECICLA', 'FL_LOCAL_FUNC_CASA_PROFESSOR', 'FL_LOCAL_FUNC_GALPAO', 'FL_LOCAL_FUNC_OUTROS', 'FL_LOCAL_FUNC_PREDIO_ESCOLAR', 'FL_LOCAL_FUNC_PRISIONAL_SOCIO', 'FL_LOCAL_FUNC_SALAS_EMPRESA', 'FL_LOCAL_FUNC_SALAS_OUTRA_ESC', 'FL_LOCAL_FUNC_SOCIOEDUCATIVO', 'FL_LOCAL_FUNC_TEMPLO_IGREJA', 'FL_LOCAL_FUNC_UNID_PRISIONAL', 'FL_MACA', 'FL_MANT_ESCOLA_PRIVADA_EMP', 'FL_MANT_ESCOLA_PRIVADA_ONG', 'FL_MANT_ESCOLA_PRIVADA_SIND', 'FL_MANT_ESCOLA_PRIVADA_SIST_S', 'FL_MAQUINA_BRAILE', 'FL_MARCA_PASSO', 'FL_MATERIAL_ESPECIFICO', 'FL_MEDICAMENTOS', 'FL_MEDIDOR_GLICOSE', 'FL_MESA_CADEIRA_RODAS', 'FL_MESA_CADEIRA_SEPARADA', 'FL_MOBILIARIO_ESPECIFICO', 'FL_MOBILIARIO_OBESO', 'FL_NOME_SOCIAL', 'FL_OUTRA_DEF', 'FL_PARQUE_INFANTIL', 'FL_PATIO_COBERTO', 'FL_PATIO_DESCOBERTO', 'FL_PREDIO_COMPARTILHADO', 'FL_PROTETOR_AURICULAR', 'FL_PROVA_DEITADO', 'FL_QUADRA_ESPORTES', 'FL_QUADRA_ESPORTES_COBERTA', 'FL_QUADRA_ESPORTES_DESCOBERTA', 'FL_REFEITORIO', 'FL_SABATISTA', 'FL_SALA_ACOMPANHANTE', 'FL_SALA_ATENDIMENTO_ESPECIAL', 'FL_SALA_DIRETORIA', 'FL_SALA_ESPECIAL', 'FL_SALA_INDIVIDUAL', 'FL_SALA_LEITURA', 'FL_SALA_PROFESSOR', 'FL_SECRETARIA', 'FL_SEM_RECURSO', 'FL_SONDA', 'FL_SOROBAN', 'FL_SURDEZ', 'FL_SURDO_CEGUEIRA', 'FL_TRANSCRICAO', 'FL_TREINEIRO', 'FL_VISAO_MONOCULAR', 'DS_GENERO', 'DS_IDADE', 'DS_INSE', 'QT_MATRIC_EM3', 'QT_MATRIC_EM4', 'FL_MATRIC10', 'NM_BAIRRO', 'NM_ENTIDADE', 'NM_ENTIDADE_CERTIFICACAO', 'NM_MUNICIPIO_ESC', 'NM_MUNICIPIO_NASCIMENTO', 'NM_MUNICIPIO_PROVA', 'NM_MUNICIPIO_RESIDENCIA', 'NM_NOME_ESCOLA', 'NM_NOME_UNIDADE', 'NM_NOMEDOMUNICÍPIO', 'NR_ANM_CENSO', 'NR_ANO', 'NR_COMP_ADMINISTRATIVO', 'NR_COMP_ALUNO', 'NR_COMPUTADOR', 'NR_EQUIP_COPIADORA', 'NR_EQUIP_DVD', 'NR_EQUIP_FAX', 'NR_EQUIP_FOTO', 'NR_EQUIP_IMPRESSORA', 'NR_EQUIP_IMPRESSORA_MULT', 'NR_EQUIP_MULTIMIDIA', 'NR_EQUIP_PARABOLICA', 'NR_EQUIP_RETROPROJETOR', 'NR_EQUIP_SOM', 'NR_EQUIP_TV', 'NR_EQUIP_VIDEOCASSETE', 'NR_ESCOLAS_W', 'NR_FUNCIONARIOS', 'NR_IDADE', 'NR_INSCRICAO', 'NR_MATRIC_CONC', 'NR_MATRICULAS', 'NR_MEDIA_OBJ', 'NR_MEDIA_TOT', 'NR_NOTA_CH', 'NR_NOTA_CN', 'NR_NOTA_COMP1', 'NR_NOTA_COMP2', 'NR_NOTA_COMP3', 'NR_NOTA_COMP4', 'NR_NOTA_COMP5', 'NR_NOTA_ENEM_GERAL_ESCOLA_COM_REDAC', 'NR_NOTA_ENEM_GERAL_ESCOLA_SEM_REDAC', 'NR_NOTA_GERAL', 'NR_NOTA_GERAL_REDAC', 'NR_NOTA_LC', 'NR_NOTA_MT', 'NR_NOTA_REDACAO', 'NR_NUMERACAO', 'NR_PARTICIPANTES', 'NR_PARTICIPANTES_NEC_ESP', 'NR_SALAS_EXISTENTES', 'NR_SALAS_UTILIZADAS', 'NR_TAXA_ABANDONO', 'NR_TAXA_APROVACAO', 'NR_TAXA_PARTICIPACAO', 'NR_TAXA_PERMANENCIA', 'NR_TAXA_REPROVACAO', 'PC_FORMACAO_DOCENTE', 'DS_PORTE_ESCOLA', 'SG_UF_ENTIDADE_CERTIFICACAO', 'SG_UF_ESC', 'SG_UF_NASCIMENTO', 'SG_UF_PROVA', 'SG_UF_RESIDENCIA', 'VL_TOT_ALUNOS_ENEM_ESCOLA', 'TP_AEE', 'TP_ANO_CONCLUIU', 'TP_ATIVIDADE_COMPLEMENTAR', 'TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_CONVENIO_PODER_PUBLICO', 'TP_COR_RACA', 'TP_DEPENDENCIA', 'TP_DEPENDENCIA_ADM_ESC', 'TP_ENSINO', 'TP_ESCOLA', 'TP_ESTADO_CIVIL', 'TP_LINGUA', 'TP_LOCALIZACAO', 'TP_LOCALIZACAO_ESC', 'TP_NACIONALIDADE', 'TP_OCUPACAO_GALPAO', 'TP_OCUPACAO_PREDIO_ESCOLAR', 'TP_PRESENCA_CH', 'TP_PRESENCA_CN', 'TP_PRESENCA_LC', 'TP_PRESENCA_MT', 'TP_SEXO', 'TP_SIT_FUNC_ESC', 'TP_SITUACAO_FUNCIONAMENTO', 'TP_ST_CONCLUSAO', 'TP_STATUS_REDACAO', 'TX_GABARITO_CH', 'TX_GABARITO_CN', 'TX_GABARITO_LC', 'TX_GABARITO_MT', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_CN', 'TX_RESPOSTAS_LC', 'TX_RESPOSTAS_MT', 'VL_Q001', 'VL_Q002', 'VL_Q003', 'VL_Q004', 'VL_Q005', 'VL_Q006', 'VL_Q007', 'VL_Q008', 'VL_Q009', 'VL_Q010', 'VL_Q011', 'VL_Q012', 'VL_Q013', 'VL_Q014', 'VL_Q015', 'VL_Q016', 'VL_Q017', 'VL_Q018', 'VL_Q019', 'VL_Q020', 'VL_Q021', 'VL_Q022', 'VL_Q023', 'VL_Q024', 'VL_Q025', 'VL_Q026', 'VL_Q027', 'VL_Q028', 'VL_Q029', 'VL_Q030', 'VL_Q031', 'VL_Q032', 'VL_Q033', 'VL_Q034', 'VL_Q035', 'VL_Q036', 'VL_Q037', 'VL_Q038', 'VL_Q039', 'VL_Q040', 'VL_Q041', 'VL_Q042', 'VL_Q043', 'VL_Q044', 'VL_Q045', 'VL_Q048', 'VL_Q049', 'VL_Q050']

df = df.select(*ordered_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.repartition(20).write.partitionBy('NR_ANO').parquet(target, mode='overwrite')