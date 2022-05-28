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
# MAGIC Processo	trs_biz_fta_matricula
# MAGIC Tabela/Arquivo Origem	/biz/uniepro/fta_matricula
# MAGIC Tabela/Arquivo Destino	/biz/uniepro/fta_matricula_kpi
# MAGIC Particionamento Tabela/Arquivo Destino	nr_ano_censo
# MAGIC Descrição Tabela/Arquivo Destino	Base de matrículas da Educação Profissional com as descrições
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

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES

# tables =  {
#   "path_origin": "uniepro/fta_matricula/",
#   "path_destination": "uniepro/fta_matricula_kpi/",
#   "destination": "/uniepro/fta_matricula_kpi/",
#   "databricks": {
#     "notebook": "/biz/inep_censo_escolar/biz_biz_fta_matricula_kpi"
#   }
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}

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

biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_destination"])
target

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Loading dataframes

# COMMAND ----------

df_fta_matricula = spark.read.parquet(source)
df_fta_matricula = (df_fta_matricula
                    .select('nr_ano_censo', 'cd_municipio_escola', 'cd_localizacao', 'cd_entidade', 'fl_mant_escola_privada_sist_s', 'tp_etapa_ensino', 'cd_curso_educacao_profissional',
                            'fl_eja', 'cd_mediacao_didatico_pedago', 'cd_sexo', 'nr_idade_censo', 'qt_matricula', 'fl_necessidade_especial', 'fl_profissionalizante', 'cd_municipio_residencia','cd_dep_adm_escola_sesi_senai','cd_dependencia_adm_escola'))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumnRenamed('nr_idade_censo', 'nr_idade')

# COMMAND ----------

uri = var_adls_uri + '/biz/corporativo/dim_estrutura_territorial/'
df_dim_estrutura_territorial = spark.read.parquet(uri)
df_dim_estrutura_territorial = (df_dim_estrutura_territorial
                               .select('cd_regiao_geografica', 'nm_uf', 'nm_mesorregiao_geografica', 'nm_municipio', 'cd_municipio'))

columns_renamed_territory = {
  'cd_regiao_geografica': 'Regiao',
  'nm_uf': 'UF_Nome',
  'nm_mesorregiao_geografica': 'Meso',
  'nm_municipio': 'NomeMunicipio',
  'cd_municipio': 'cd_municipio_escola'
}
for column, renamed in columns_renamed_territory.items():
  df_dim_estrutura_territorial = df_dim_estrutura_territorial.withColumnRenamed(column, renamed)

# COMMAND ----------

uri = var_adls_uri + '/biz/corporativo/dim_estrutura_territorial/'
df_dim_estrutura_territorial_student = spark.read.parquet(uri)
df_dim_estrutura_territorial_student = (df_dim_estrutura_territorial_student
                                         .select('nm_regiao_geografica', 'nm_uf', 'nm_mesorregiao_geografica', 'nm_municipio', 'cd_municipio'))

columns_renamed_territory_student = {
  'nm_regiao_geografica': 'Regiao_aluno',
  'nm_uf': 'UF_Nome_aluno',
  'nm_mesorregiao_geografica': 'Meso_aluno',
  'nm_municipio': 'NomeMunicAluno',
  'cd_municipio': 'cd_municipio_residencia'
}
for column, renamed in columns_renamed_territory_student.items():
  df_dim_estrutura_territorial_student = df_dim_estrutura_territorial_student.withColumnRenamed(column, renamed)

# COMMAND ----------

uri = var_adls_uri + '/biz/corporativo/dim_curso_educacao_profissional/'
df_dim_curso_educacao_profissional = spark.read.parquet(uri)
df_dim_curso_educacao_profissional = df_dim_curso_educacao_profissional.select('cd_eixo', 'nm_curso_educacao_profissional', 'cd_curso_educacao_profissional')

# COMMAND ----------

uri = var_adls_uri + '/biz/corporativo/dim_escola_unificada/'
df_dim_escola_unificada = spark.read.parquet(uri)
df_dim_escola_unificada = df_dim_escola_unificada.select('nm_unidade_atendimento_inep', 'cd_entidade')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Applying rules individually

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('etapa_prof_tableau', (f.when(f.col('tp_etapa_ensino').isin([30, 31, 32, 33, 34]), f.lit(1))
                                                                       .otherwise(f.when(f.col('tp_etapa_ensino').isin([35, 36, 37, 38]), f.lit(2))
                                                                                   .otherwise(f.when(f.col('tp_etapa_ensino') == f.lit(39), f.lit(3))
                                                                                               .otherwise(f.when(f.col('tp_etapa_ensino') == f.lit(40), f.lit(4))
                                                                                                           .otherwise(f.when(f.col('tp_etapa_ensino') == f.lit(65), f.lit(5))
                                                                                                                       .otherwise(f.when(f.col('tp_etapa_ensino').isin([67, 68, 73]), f.lit(6))
                                                                                                                                   .otherwise(f.when(f.col('tp_etapa_ensino') == f.lit(74), f.lit(7))))))))))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('TP_EAD_AGR', f.when(f.col('cd_mediacao_didatico_pedago') == f.lit(1), f.lit(1)).otherwise(f.lit(2)))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('Idade_Agrupada', (f.when(f.col('nr_idade') < f.lit(14), f.lit(1))
                                                                    .otherwise(f.when((f.col('nr_idade') > f.lit(13)) & (f.col('nr_idade') < f.lit(18)), f.lit(2))
                                                                                .otherwise(f.when((f.col('nr_idade') > f.lit(17)) & (f.col('nr_idade') < f.lit(25)), f.lit(3))
                                                                                            .otherwise(f.when((f.col('nr_idade') > f.lit(24)) & (f.col('nr_idade') < f.lit(30)), f.lit(4))
                                                                                                        .otherwise(f.when((f.col('nr_idade') > f.lit(29)) & (f.col('nr_idade') < f.lit(40)), f.lit(5))
                                                                                                                     .otherwise(f.lit(6))))))))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('Idade_Agrupada2', (f.when(f.col('nr_idade') < f.lit(18), f.lit(1))
                                                                    .otherwise(f.when((f.col('nr_idade') > f.lit(17)) & (f.col('nr_idade') < f.lit(25)), f.lit(2))
                                                                                .otherwise(f.when((f.col('nr_idade') > f.lit(24)) & (f.col('nr_idade') < f.lit(40)), f.lit(3))
                                                                                            .otherwise(f.when((f.col('nr_idade') > f.lit(39)) & (f.col('nr_idade') < f.lit(100)), f.lit(4))
                                                                                                        .otherwise(f.lit(5)))))))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('Publico_Privado', f.when(f.col('cd_dependencia_adm_escola') < f.lit(4), f.lit(0)).otherwise(f.lit(1)))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('escola_senai', f.when(f.col('cd_dep_adm_escola_sesi_senai') == f.lit(12), f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('escola_sesi', f.when(f.col('cd_dep_adm_escola_sesi_senai') == f.lit(11), f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

df_fta_matricula = df_fta_matricula.withColumn('Sesi_Senai', f.when(f.col('cd_dep_adm_escola_sesi_senai').isin([11, 12,13]), f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Joining dataframes

# COMMAND ----------

df = df_fta_matricula.join(df_dim_estrutura_territorial, on='cd_municipio_escola', how='left')

# COMMAND ----------

df = df.join(df_dim_curso_educacao_profissional, on='cd_curso_educacao_profissional', how='left')

# COMMAND ----------

df = df.join(df_dim_escola_unificada, on='cd_entidade', how='left')

# COMMAND ----------

df = df.join(df_dim_estrutura_territorial_student, on='cd_municipio_residencia', how='left')

# COMMAND ----------

columns_renamed = {
    'nr_ano_censo': 'Ano',
    'cd_municipio_escola': 'Municipio_Escola',
    'cd_localizacao': 'Localizacao',
    'cd_entidade': 'Codigo_Escola',
    'cd_dep_adm_escola_sesi_senai': 'Dep_Administrativa',
    'fl_mant_escola_privada_sist_s': 'Mant_Sistema_S',
    'cd_eixo': 'AREA_PROF',
    'cd_curso_educacao_profissional': 'Curso_Prof',
    'nm_curso_educacao_profissional': 'Curso',
    'fl_eja': 'EJA',
    'cd_mediacao_didatico_pedago': 'TP_EAD',
    'cd_municipio_residencia': 'Municipio_Aluno',
    'cd_sexo': 'Sexo',
    'nm_unidade_atendimento_inep': 'NOME_ESCOLA',
    'qt_matricula': 'Total_Matriculas',
    'fl_necessidade_especial': 'IN_NECESSIDADE_ESPECIAL',
    'fl_profissionalizante': 'IN_PROFISSIONALIZANTE'
}

# COMMAND ----------

for column, renamed in columns_renamed.items():
  df = df.withColumnRenamed(column, renamed)

# COMMAND ----------

reorder_columns = ['Ano', 'Regiao', 'UF_Nome', 'Meso', 'Municipio_Escola', 'NomeMunicipio', 'Localizacao', 'Codigo_Escola', 'Dep_Administrativa', 'Mant_Sistema_S',
                   'Publico_Privado', 'etapa_prof_tableau', 'AREA_PROF', 'Curso_Prof', 'Curso', 'EJA', 'TP_EAD', 'TP_EAD_AGR', 'Regiao_aluno', 'UF_Nome_aluno', 'Meso_aluno', 
                   'Municipio_Aluno', 'NomeMunicAluno', 'Sexo', 'Idade_Agrupada', 'Idade_Agrupada2', 'NOME_ESCOLA', 'escola_senai', 
                   'escola_sesi', 'Sesi_Senai', 'Total_Matriculas', 'IN_NECESSIDADE_ESPECIAL', 'IN_PROFISSIONALIZANTE']

# COMMAND ----------

df = df.select(*reorder_columns)

# COMMAND ----------

df = df.withColumn('AREA_PROF_desc', f.when(f.col('AREA_PROF') == f.lit(1), f.lit('Ambiente e saúde'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(2), f.lit('Desenvolvimento educacional e social'))       
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(3), f.lit('Controle e processos industriais'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(4), f.lit('Gestão e negócios'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(5), f.lit('Turismo, hospitalidade e lazer'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(6), f.lit('Informação e Comunicação'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(7), f.lit('Infraestrutura'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(8), f.lit('Militar'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(9), f.lit('Produção alimentícia'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(10), f.lit('Produção cultural e design'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(11), f.lit('Produção industrial'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(12), f.lit('Recursos naturais'))
                                      .otherwise(
                                     f.when(f.col('AREA_PROF') == f.lit(13), f.lit('Segurança do trabalho'))
                                      .otherwise(f.lit('Outros não-técnico')))))))))))))))

# COMMAND ----------

df = df.withColumn('Dep_Administrativa_desc', f.when(f.col('DEP_ADMINISTRATIVA') == f.lit(1), f.lit('Federal'))
                                               .otherwise(
                                              f.when(f.col('DEP_ADMINISTRATIVA') == f.lit(2), f.lit('Estadual'))       
                                               .otherwise(
                                              f.when(f.col('DEP_ADMINISTRATIVA') == f.lit(3), f.lit('Municipal'))
                                               .otherwise(
                                              f.when(f.col('DEP_ADMINISTRATIVA') == f.lit(4), f.lit('Privada'))
                                               .otherwise(f.col('DEP_ADMINISTRATIVA'))))))

# COMMAND ----------

df = df.withColumn('DependenciaAdmSenai', f.when(f.col('Sesi_Senai') == f.lit(1), f.lit(5)).otherwise(f.col('DEP_ADMINISTRATIVA')))
df = df.withColumn('DependenciaAdmSenai_desc', f.when(f.col('DependenciaAdmSenai') == f.lit(1), f.lit('Federal'))
                                                .otherwise(
                                               f.when(f.col('DependenciaAdmSenai') == f.lit(2), f.lit('Estadual'))       
                                                .otherwise(
                                               f.when(f.col('DependenciaAdmSenai') == f.lit(3), f.lit('Municipal'))
                                                .otherwise(
                                               f.when(f.col('DependenciaAdmSenai') == f.lit(4), f.lit('Outras Privadas'))
                                                .otherwise(
                                               f.when(f.col('DependenciaAdmSenai') == f.lit(5), f.lit('Senai')))))))

# COMMAND ----------

df = df.withColumn('etapa_prof_tableau_desc', f.when(f.col('etapa_prof_tableau') == f.lit(1), f.lit('Curso Técnico - Integrado'))
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(2), f.lit('Ensino Médio Normal/Magistério'))       
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(3), f.lit('Curso Técnico - Concomitante'))
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(4), f.lit('Curso Técnico - Subsequente'))
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(5), f.lit('EJA Ensino Fundamental Projovem Urbano'))
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(6), f.lit('Curso FIC'))
                                                .otherwise(
                                               f.when(f.col('etapa_prof_tableau') == f.lit(7), f.lit('Curso Técnico - Integrado EJA'))
                                                .otherwise(f.lit(None)))))))))

# COMMAND ----------

df = df.withColumn('FaltaInfoAluno', f.col('Municipio_Aluno').isNull().cast('Int'))
df = df.withColumn('FaltaInfoAluno_desc', f.when(f.col('FaltaInfoAluno') == f.lit(0), f.lit('Com dados de local')).otherwise(f.lit('Sem local de residência')))

# COMMAND ----------

df = df.withColumn('Idade_Agrupada2_desc', f.when(f.col('Idade_Agrupada2') == f.lit(1), f.lit('Até 17 anos'))
                                            .otherwise(
                                           f.when(f.col('Idade_Agrupada2') == f.lit(2), f.lit('18 a 24 anos'))       
                                            .otherwise(
                                           f.when(f.col('Idade_Agrupada2') == f.lit(3), f.lit('25 a 39 anos'))
                                            .otherwise(
                                           f.when(f.col('Idade_Agrupada2') == f.lit(4), f.lit('40 anos ou +'))
                                            .otherwise(
                                           f.when(f.col('Idade_Agrupada2') == f.lit(5), f.lit('5')))))))

# COMMAND ----------

df = df.withColumn('Publico_Privado_desc', f.when(f.col('Publico_Privado') == f.lit(0), f.lit('Pública')).otherwise(f.lit('Privada')))
df = df.withColumn('Sesi_Senai_desc', f.when(f.col('Sesi_Senai') == f.lit(0), f.lit('Outras redes')).otherwise(f.lit('Senai')))
df = df.withColumn('Sexo_desc', f.when(f.col('Sexo') == f.lit(1), f.lit('Masculino')).otherwise(f.lit('Feminino')))
df = df.withColumn('TP_EAD_AGR_desc', f.when(f.col('TP_EAD_AGR') == f.lit(1), f.lit('Presencial')).otherwise(f.lit('EAD')))
df = df.withColumn('Localizacao_desc', f.when(f.col('Localizacao') == f.lit(1), f.lit('Urbana')).otherwise(f.lit('Rural')))

# COMMAND ----------

df = df.withColumn('Regiao_desc', f.when(f.col('Regiao') == f.lit(1), f.lit('Norte'))
                                   .otherwise(
                                  f.when(f.col('Regiao') == f.lit(2), f.lit('Nordeste'))       
                                   .otherwise(
                                  f.when(f.col('Regiao') == f.lit(3), f.lit('Sudeste'))
                                   .otherwise(
                                  f.when(f.col('Regiao') == f.lit(4), f.lit('Sul'))
                                   .otherwise(
                                  f.when(f.col('Regiao') == f.lit(5), f.lit('Centro-Oeste')))))))

# COMMAND ----------

df = df.withColumn('Regiao_Sigla_desc', f.when(f.col('Regiao') == f.lit(1), f.lit('NO'))
                                         .otherwise(
                                        f.when(f.col('Regiao') == f.lit(2), f.lit('NE'))
                                         .otherwise(
                                        f.when(f.col('Regiao') == f.lit(3), f.lit('SD'))
                                         .otherwise(
                                        f.when(f.col('Regiao') == f.lit(4), f.lit('SU'))
                                         .otherwise(
                                        f.when(f.col('Regiao') == f.lit(5), f.lit('CO')))))))

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.repartition(5).write.parquet(target, mode='overwrite')