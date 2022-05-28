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

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES
# tables =  {
#   "path_origin": "crw/inep_censo_escolar/matriculas/",
#   "path_escola": "mtd/corp/base_escolas/",
#   "path_destination": "inep_censo_escolar/matriculas/"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

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

source_escola = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_escola"])
source_escola

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df_source = spark.read.parquet(source)

# COMMAND ----------

df_source = df_source.withColumnRenamed('CO_ENTIDADE','cd_entidade')

# COMMAND ----------

df_2009_a_2014 = df_source.filter((df_source.NU_ANO_CENSO >= 2009) & (df_source.NU_ANO_CENSO <= 2014))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('IN_ESPECIAL_EXCLUSIVA',\
                  when(df_2009_a_2014.FK_COD_MOD_ENSINO == 2, 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('IN_REGULAR',\
                  when((df_2009_a_2014.FK_COD_MOD_ENSINO.isin([1,2])) & (df_2009_a_2014.TP_ETAPA_ENSINO.isin([1,2,4,5,6,7,8,9,10,11,14, 15,16,17,18,19,20,21,41,25,26,27,28,29,30,31,32,33,34,35,36,37,38])), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('IN_EJA',\
                  when((df_2009_a_2014.FK_COD_MOD_ENSINO.isin([2,3])) & (df_2009_a_2014.TP_ETAPA_ENSINO.isin([65,67,69,70,71,73,74])), 1).otherwise(lit(0)))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('IN_PROFISSIONALIZANTE',\
                  when((df_2009_a_2014.FK_COD_MOD_ENSINO.isin([1,2,3])) & (df_2009_a_2014.TP_ETAPA_ENSINO.isin([30,31,32,33,34, 35,36,37,38,39,40,60,61,62,63,65,67,68,73,74])), 1).otherwise(lit(0))) 

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('TP_CONVENIO_PODER_PUBLICO',\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 1, 2).\
                  when(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO == 2, 1).otherwise(df_2009_a_2014.TP_CONVENIO_PODER_PUBLICO))

# COMMAND ----------

df_2009_a_2014 = df_2009_a_2014.withColumn('tp_mediacao_didatico_pedago', when(col('TP_ETAPA_ENSINO').isin([47,48,61,63]), lit(2)).otherwise(lit(1)))

# COMMAND ----------

df_2015_a_2019 = df_source.filter((df_source.NU_ANO_CENSO >= 2015))

# COMMAND ----------

df_2015_a_2019 = df_2015_a_2019.withColumn('FK_COD_MOD_ENSINO',\
                  when((df_2015_a_2019.IN_REGULAR == 1) & (df_2015_a_2019.IN_ESPECIAL_EXCLUSIVA == 0), 1).\
                  when(df_2015_a_2019.IN_ESPECIAL_EXCLUSIVA == 1, 2).\
                  when((df_2015_a_2019.IN_EJA == 1) & (df_2015_a_2019.IN_ESPECIAL_EXCLUSIVA == 0), 3).otherwise(df_2015_a_2019.FK_COD_MOD_ENSINO))

# COMMAND ----------

df = df_2009_a_2014.union(df_2015_a_2019)

# COMMAND ----------

df = df.withColumnRenamed('nu_ano_censo','nr_ano_censo')

# COMMAND ----------

df_escola = spark.read.parquet(source_escola).select('cd_entidade', 'fl_escola_sesi', 'fl_escola_senai')

# COMMAND ----------

df = df.join(df_escola,on='CD_ENTIDADE', how='left')

# COMMAND ----------

df = df.withColumn('TP_SEXO', when((col('TP_SEXO') == 'M'), lit(1)).otherwise(when((col('TP_SEXO') == 'F'), lit(2)).otherwise(df.TP_SEXO)))

# COMMAND ----------

df = df.withColumnRenamed('nu_ano_censo','nr_ano_censo')
df = df.withColumnRenamed('id_matricula','id_matricula')
df = df.withColumnRenamed('id_aluno','id_aluno')
df = df.withColumnRenamed('nu_dia','nr_dia_nasc')
df = df.withColumnRenamed('nu_mes','nr_mes_nasc')
df = df.withColumnRenamed('nu_ano','nr_ano_nasc')
df = df.withColumnRenamed('nu_idade','nr_idade')
df = df.withColumnRenamed('tp_sexo','cd_sexo')
df = df.withColumnRenamed('tp_cor_raca','tp_cor_raca')
df = df.withColumnRenamed('tp_nacionalidade','tp_nacionalidade')
df = df.withColumnRenamed('co_pais_origem','cd_pais_origem')
df = df.withColumnRenamed('co_uf_nasc','cd_uf_nasc')
df = df.withColumnRenamed('sgl_uf_nascimento','sg_uf_nascimento')
df = df.withColumnRenamed('co_municipio_nasc','cd_municipio_nasc')
df = df.withColumnRenamed('co_uf_end','cd_uf_end')
df = df.withColumnRenamed('sigla_end','sg_end')
df = df.withColumnRenamed('co_municipio_end','cd_municipio_residencia')
df = df.withColumnRenamed('tp_zona_residencial','tp_zona_residencial')
df = df.withColumnRenamed('tp_outro_local_aula','tp_outro_local_aula')
df = df.withColumnRenamed('in_transporte_publico','fl_transporte_publico')
df = df.withColumnRenamed('tp_responsavel_transporte','tp_responsavel_transporte')
df = df.withColumnRenamed('in_necessidade_especial','fl_necessidade_especial')
df = df.withColumnRenamed('in_cegueira','fl_cegueira')
df = df.withColumnRenamed('in_baixa_visao','fl_baixa_visao')
df = df.withColumnRenamed('in_surdez','fl_surdez')
df = df.withColumnRenamed('in_def_auditiva','fl_def_auditiva')
df = df.withColumnRenamed('in_surdocegueira','fl_surdocegueira')
df = df.withColumnRenamed('in_def_fisica','fl_def_fisica')
df = df.withColumnRenamed('in_def_intelectual','fl_def_intelectual')
df = df.withColumnRenamed('in_def_multipla','fl_def_multipla')
df = df.withColumnRenamed('in_autismo','fl_autismo')
df = df.withColumnRenamed('in_sindrome_asperger','fl_sindrome_asperger')
df = df.withColumnRenamed('in_sindrome_rett','fl_sindrome_rett')
df = df.withColumnRenamed('in_transtorno_di','fl_transtorno_di')
df = df.withColumnRenamed('in_superdotacao','fl_superdotacao')
df = df.withColumnRenamed('fk_cod_mod_ensino','fk_cod_mod_ensino')
df = df.withColumnRenamed('tp_etapa_ensino','tp_etapa_ensino')
df = df.withColumnRenamed('id_turma','id_turma')
df = df.withColumnRenamed('co_curso_educ_profissional','cd_curso_educacao_profissional')
df = df.withColumnRenamed('tp_unificada','tp_unificada')
df = df.withColumnRenamed('tp_tipo_turma','tp_tipo_turma')
df = df.withColumnRenamed('co_uf','cd_uf')
df = df.withColumnRenamed('sigla_escola','sg_escola')
df = df.withColumnRenamed('co_municipio', 'cd_municipio_escola')
df = df.withColumnRenamed('tp_localizacao','cd_localizacao')
df = df.withColumnRenamed('tp_dependencia','cd_dependencia_adm_escola')
df = df.withColumnRenamed('tp_categoria_escola_privada','tp_categoria_escola_privada')
df = df.withColumnRenamed('in_conveniada_pp','fl_conveniada_pp')
df = df.withColumnRenamed('tp_convenio_poder_publico','tp_convenio_poder_publico')
df = df.withColumnRenamed('in_mant_escola_privada_emp','fl_mant_escola_privada_emp')
df = df.withColumnRenamed('in_mant_escola_privada_ong','fl_mant_escola_privada_ong')
df = df.withColumnRenamed('in_mant_escola_privada_sind','fl_mant_escola_privada_sind')
df = df.withColumnRenamed('in_mant_escola_privada_s_fins','fl_mant_escola_privada_s_fins')
df = df.withColumnRenamed('tp_regulamentacao','tp_regulamentacao')
df = df.withColumnRenamed('tp_localizacao_diferenciada','tp_localizacao_diferenciada')
df = df.withColumnRenamed('in_educacao_indigena','fl_educacao_indigena')
df = df.withColumnRenamed('nu_idade_referencia','nr_idade_censo')
df = df.withColumnRenamed('co_pais_residencia','cd_pais_residencia')
df = df.withColumnRenamed('tp_local_resid_diferenciada','tp_local_resid_diferenciada')
df = df.withColumnRenamed('in_recurso_ledor','fl_recurso_ledor')
df = df.withColumnRenamed('in_recurso_transcricao','fl_recurso_transcricao')
df = df.withColumnRenamed('in_recurso_interprete','fl_recurso_interprete')
df = df.withColumnRenamed('in_recurso_libras','fl_recurso_libras')
df = df.withColumnRenamed('in_recurso_labial','fl_recurso_labial')
df = df.withColumnRenamed('in_recurso_ampliada_18','fl_recurso_ampliada_18')
df = df.withColumnRenamed('in_recurso_ampliada_16','fl_recurso_ampliada_16')
df = df.withColumnRenamed('in_recurso_ampliada_20','fl_recurso_ampliada_20')
df = df.withColumnRenamed('in_recurso_ampliada_24','fl_recurso_ampliada_24')
df = df.withColumnRenamed('in_recurso_cd_audio','fl_recurso_cd_audio')
df = df.withColumnRenamed('in_recurso_prova_portugues','fl_recurso_prova_portugues')
df = df.withColumnRenamed('in_recurso_video_libras','fl_recurso_video_libras')
df = df.withColumnRenamed('in_recurso_braille','fl_recurso_braille')
df = df.withColumnRenamed('in_recurso_nenhum','fl_recurso_nenhum')
df = df.withColumnRenamed('in_aee_libras','fl_aee_libras')
df = df.withColumnRenamed('in_aee_lingua_portuguesa','fl_aee_lingua_portuguesa')
df = df.withColumnRenamed('in_aee_informatica_acessivel','fl_aee_informatica_acessivel')
df = df.withColumnRenamed('in_aee_braille','fl_aee_braille')
df = df.withColumnRenamed('in_aee_caa','fl_aee_caa')
df = df.withColumnRenamed('in_aee_soroban','fl_aee_soroban')
df = df.withColumnRenamed('in_aee_vida_autonoma','fl_aee_vida_autonoma')
df = df.withColumnRenamed('in_aee_opticos_nao_opticos','fl_aee_opticos_nao_opticos')
df = df.withColumnRenamed('in_aee_enriq_curricular','fl_aee_enriq_curricular')
df = df.withColumnRenamed('in_aee_desen_cognitivo','fl_aee_desen_cognitivo')
df = df.withColumnRenamed('in_aee_mobilidade','fl_aee_mobilidade')
df = df.withColumnRenamed('in_transp_bicicleta','fl_transp_bicicleta')
df = df.withColumnRenamed('in_transp_micro_onibus','fl_transp_micro_onibus')
df = df.withColumnRenamed('in_transp_onibus','fl_transp_onibus')
df = df.withColumnRenamed('in_transp_tr_animal','fl_transp_tr_animal')
df = df.withColumnRenamed('in_transp_vans_kombi','fl_transp_vans_kombi')
df = df.withColumnRenamed('in_transp_outro_veiculo','fl_transp_outro_veiculo')
df = df.withColumnRenamed('in_transp_embar_ate5','fl_transp_embar_ate5')
df = df.withColumnRenamed('in_transp_embar_5a15','fl_transp_embar_5a15')
df = df.withColumnRenamed('in_transp_embar_15a35','fl_transp_embar_15a35')
df = df.withColumnRenamed('in_transp_embar_35','fl_transp_embar_35')
df = df.withColumnRenamed('in_transp_trem_metro','fl_transp_trem_metro')
df = df.withColumnRenamed('tp_ingresso_federais','tp_ingresso_federais')
df = df.withColumnRenamed('in_especial_exclusiva','fl_especial_exclusiva')
df = df.withColumnRenamed('in_regular','fl_regular')
df = df.withColumnRenamed('in_eja','fl_eja')
df = df.withColumnRenamed('in_profissionalizante','fl_profissionalizante')
df = df.withColumnRenamed('tp_mediacao_didatico_pedago','cd_mediacao_didatico_pedago')
df = df.withColumnRenamed('nu_duracao_turma','nr_duracao_turma')
df = df.withColumnRenamed('nu_dur_ativ_comp_mesma_rede','nr_dur_ativ_comp_mesma_rede')
df = df.withColumnRenamed('nu_dur_ativ_comp_outras_redes','nr_dur_ativ_comp_outras_redes')
df = df.withColumnRenamed('nu_dur_aee_mesma_rede','nr_dur_aee_mesma_rede')
df = df.withColumnRenamed('nu_dur_aee_outras_redes','nr_dur_aee_outras_redes')
df = df.withColumnRenamed('nu_dias_atividade','nr_dias_atividade')
df = df.withColumnRenamed('tp_tipo_atendimento_turma','tp_tipo_atendimento_turma')
df = df.withColumnRenamed('tp_tipo_local_turma','tp_tipo_local_turma')
df = df.withColumnRenamed('co_regiao','cd_regiao')
df = df.withColumnRenamed('co_mesorregiao','cd_mesorregiao')
df = df.withColumnRenamed('co_microrregiao','cd_microrregiao')
df = df.withColumnRenamed('co_distrito','cd_distrito')
df = df.withColumnRenamed('in_mant_escola_privada_oscip','fl_mant_escola_privada_oscip')
df = df.withColumnRenamed('in_mant_escola_priv_ong_oscip','fl_mant_escola_priv_ong_oscip')
df = df.withColumnRenamed('in_mant_escola_privada_sist_s','fl_mant_escola_privada_sist_s')
df = df.withColumnRenamed('dh_arq_in', 'dh_ultima_atualizacao_oltp')

# COMMAND ----------

df = df.withColumn('cd_dep_adm_escola_sesi_senai', when((col('fl_escola_sesi') == lit(1)) & (col('fl_escola_senai') == lit(0)), lit(11)).\
                   when((col('fl_escola_sesi') == lit(0)) & (col('fl_escola_senai') == lit(1)), lit(12)).\
                   when((col('fl_escola_sesi') == lit(1)) & (col('fl_escola_senai') == lit(1)), lit(13)).otherwise(col('cd_dependencia_adm_escola')))

# COMMAND ----------

df = df.drop("fl_escola_sesi","fl_escola_senai")

# COMMAND ----------

df = df.withColumn('tp_tipo_atendimento_turma',\
                  when(df.tp_tipo_turma.isin([0,1,3]),lit(1)).\
                  when(df.tp_tipo_turma == 4, 3).\
                  when(df.tp_tipo_turma == 5, 4).otherwise(lit(None)))

# COMMAND ----------

df = df.withColumn('tp_tipo_local_turma',\
                  when(df.tp_tipo_turma == 0, 0).\
                  when(df.tp_tipo_turma == 2, 2).\
                  when(df.tp_tipo_turma == 3, 3).otherwise(lit(None)))

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"nr_ano_censo" : "int",                    
                   "id_aluno": "varchar(250)",
                   "id_matricula" : "int",
                   "id_turma" : "int",
                   "cd_mediacao_didatico_pedago" : "int",
                   "cd_curso_educacao_profissional" : "int",
                   "cd_entidade" : "int",
                   "cd_municipio_escola" : "int",
                   "nr_idade" : "int",
                   "cd_sexo":"int",
                   "cd_municipio_residencia" : "int",
                   "tp_etapa_ensino" : "int",
                   "dh_ultima_atualizacao_oltp": "timestamp",
                   "cd_dependencia_adm_escola" : "int",
                   "cd_dep_adm_escola_sesi_senai" : "int",
                   "nr_dia_nasc" : "int",
                   "nr_mes_nasc" : "int",
                   "nr_ano_nasc" : "int",
                   "nr_idade_censo" : "int",
                   "nr_idade" : "int",
                   "cd_municipio_residencia" : "int",
                   "cd_localizacao" : "int",
                   "fl_eja" : "int",
                   "fl_necessidade_especial" : "int",
                   "fl_profissionalizante" : "int",
                   "fl_mant_escola_privada_sist_s" : "int"}
for c in column_type_map:
  df = df.withColumn(c, df[c].cast(column_type_map[c]))

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.repartition(60).write.partitionBy('nr_ano_censo').save(path=target, format="parquet", mode='overwrite')