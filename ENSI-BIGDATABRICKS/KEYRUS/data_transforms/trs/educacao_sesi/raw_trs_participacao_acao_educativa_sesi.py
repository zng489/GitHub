# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_participacao_acao_educativa_sesi			
# MAGIC Tabela/Arquivo Origem	"Principais:
# MAGIC /raw/bdo/scae/matricula
# MAGIC Relacionadas:
# MAGIC /raw/bdo/scae/matricula_situacao
# MAGIC /raw/bdo/scae/ciclo_matricula
# MAGIC /raw/bdo/scae/ciclo
# MAGIC /raw/bdo/scae/turma
# MAGIC /raw/bdo/corporativo/instituicao
# MAGIC /raw/bdo/scae/oferta_curso
# MAGIC /raw/bdo/scae/curso
# MAGIC /raw/bdo/scae/produto_servico"			
# MAGIC Tabela/Arquivo Destino	/trs/evt/participacao_acao_educativa_sesi			/trs/evt/participacao_acao_educativa_sesi_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp)			YEAR(dh_ultima_atualizacao_oltp)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações de identificação adesões de participantes em ações educativas oferecidas pelo SESI que geram produção e que são contabilizados em termos de consolidação dos dados de produção SESI			Tabela satélite com campos que necessitam acompanhamento de aterações no tempo
# MAGIC Tipo Atualização	U = update (insert/update)			V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou sobrescreve o registro caso a chave cd_participante_acao_educativa_sesi seja encontrada.			Carga incremental por dat_registro e cod_matricula, que insere ou versiona o registro caso a chave cd_participante_acao_educativa_sesi seja encontrada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00			
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import json
import re

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/scae/vw_matricula", "/bdo/scae/turma", "/bdo/scae/oferta_curso", "/bdo/scae/curso", "/bdo/scae/produto_servico", "/bdo/corporativo/instituicao", "/bdo/scae/matricula_situacao"], "destination": ["/evt/participacao_acao_educativa_sesi", "/evt/participacao_acao_educativa_sesi_caracteristica"]}

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/tmp/dev/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_trs_matricula_educacao_sesi',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }  
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_mt, src_tu, src_oc, src_cs, src_ps, src_in, src_ms = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_mt, src_tu, src_oc, src_cs, src_ps, src_in, src_ms)

# COMMAND ----------

sink_paes, sink_paesc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["destination"]]
print(sink_paes, sink_paesc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, year, max, min, current_timestamp, from_utc_timestamp, col, trim, substring, count, concat_ws, when, greatest, desc, current_timestamp, asc, broadcast, coalesce, lag, lead, concat_ws
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get max date of dh_ultima_atualizacao_oltp from last load

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  df_trs_participacao_acao_educativa_sesi = spark.read.parquet(sink_paes).coalesce(6)
  df_trs_participacao_acao_educativa_sesi_caracteristica = spark.read.parquet(sink_paesc).coalesce(6)
  var_max_dh_ultima_atualizacao_oltp = df_trs_participacao_acao_educativa_sesi.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]  
except AnalysisException:
  first_load = True

# COMMAND ----------

print("first_load: {}, var_max_dh_ultima_atualizacao_oltp: {}".format(first_load, var_max_dh_ultima_atualizacao_oltp))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 0

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /*Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) 
# MAGIC  from participacao_acao_educativa_sesi */
# MAGIC /*Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC 
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 0: Seleciona todas as matriculas situacao onde status = 17 (participantes) com flag de excluído = 1 (excluído fisicamente)
# MAGIC   Esses registros serão utilizados para garantir que nenhum participante excluído será contabilizado.
# MAGIC   ==============================================================================================================================*/
# MAGIC    ---- Incluido em 10/05/2021
# MAGIC  (SELECT cod_matricula 
# MAGIC  FROM  matricula_situacao 
# MAGIC  WHERE fl_excluido = 1
# MAGIC  AND   cod_situacao = 17 AND YEAR(dat_movimentacao) >= YEAR(#var_max_dh_ultima_atualizacao_oltp)
# MAGIC  ) "PARTICIPANTE_EXCLUIDO"
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

"""
This table has ~2mi records at first load. We can keep it in 12 partitions.
"""

matricula_situacao_useful_columns = ["cod_matricula", "dat_movimentacao", "cod_situacao", "fl_excluido"]

df_matricula_situacao = spark.read.parquet(src_ms)\
.select(*matricula_situacao_useful_columns)\
.filter((col("cod_situacao") == 17 )&\
        (year(col("dat_movimentacao")) >= year(lit(var_max_dh_ultima_atualizacao_oltp).cast("date"))))\
.coalesce(12)

# COMMAND ----------

df_participante_excluido = df_matricula_situacao.filter(col("fl_excluido")==1)\
.select(col("cod_matricula").alias("cod_matricula_excluida"))\
.distinct()\
.coalesce(1)\
.cache()

df_participante_excluido.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 and 2
# MAGIC #### (Steps 1 to 3 refers to raw_trs_participacao_acao_educativa_sesi and raw_trs_participacao_acao_educativa_sesi_caracteristica)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /*Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) 
# MAGIC  from participacao_acao_educativa_sesi */
# MAGIC /*Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC 
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 1: Obter o registro original/inicial da matrícula situacao onde status = 17 (participante) para em seguida, 
# MAGIC   RETIRADO - PASSO 2: verificar se é válida, isto é, toda a matrícula tem que estar atrelada a um ciclo para ser considerada nas análises 
# MAGIC   ==============================================================================================================================*/
# MAGIC (  
# MAGIC    SELECT
# MAGIC    s.cod_matricula,
# MAGIC    TO_DATE(MIN(s.dat_movimentacao)) AS dat_movimentacao
# MAGIC    FROM matricula_situacao s
# MAGIC    INNER JOIN vw_matricula m ON m.cod_matricula = s.cod_matricula
# MAGIC    LEFT JOIN "PARTICIPANTE_EXCLUIDO" n ON m.cod_matricula = n.cod_matricula   ---- Incluido em 10/05/2021
# MAGIC    WHERE s.cod_situacao = 17 AND YEAR(s.dat_movimentacao) >= 2020 --YEAR(#var_max_dh_ultima_atualizacao_oltp)
# MAGIC    AND s.fl_excluido = 0  ---- Incluido em 10/05/2021
# MAGIC    AND n.cod_matricula IS NULL   ---- Incluido em 10/05/2021
# MAGIC    GROUP BY s.cod_matricula
# MAGIC ) "PARTICIPANTE"
# MAGIC /*
# MAGIC --retirado em 26/01/2021
# MAGIC ( 
# MAGIC SELECT cm.cod_matricula, MAX(cm.dat_alteracao) AS dat_alteracao_ciclo
# MAGIC FROM ciclo_matricula cm
# MAGIC INNER JOIN "PARTICIPANTE" p ON p.cod_matricula = cm.cod_matricula
# MAGIC INNER JOIN ciclo c ON c.cod_ciclo = cm.cod_ciclo
# MAGIC WHERE YEAR(c.dat_inicio_exercicio) >= 2013
# MAGIC GROUP BY cm.cod_matricula
# MAGIC ) "PARTICIPANTE_VALIDO"
# MAGIC */
# MAGIC </pre>

# COMMAND ----------

"""
This table has ~14mi records in first load. We can keep it to 24 partitions, maybe cache won't help us.
"""

matricula_useful_columns = ["cod_matricula", "cod_matricula_dr", "cod_turma",  "cod_pessoa", "cod_estudante", "cod_financiamento", "ind_ebep", "ind_tempo_integral", "ind_viravida", "ind_exclusao", "dat_registro", "dat_atualizacao"]

df_mat = spark.read.parquet(src_mt)\
.select(*matricula_useful_columns)\
.distinct()\
.coalesce(24)

# COMMAND ----------

"""
This table has ~2mi records and ~ 16mb in mem cache at first load. We can cache it and keep it in 12 partitions. 
"""

df_participante = df_matricula_situacao\
.join(df_mat.select("cod_matricula"), ["cod_matricula"], "inner")\
.join(df_participante_excluido, 
      df_matricula_situacao.cod_matricula == df_participante_excluido.cod_matricula_excluida,
      "left")\
.filter((col("fl_excluido")==0) &\
        (col("cod_matricula_excluida").isNull()))\
.groupBy("cod_matricula")\
.agg(min("dat_movimentacao").cast("date").alias("dat_movimentacao"))\
.coalesce(12)

# COMMAND ----------

df_participante_excluido = df_participante_excluido.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC */
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 3: Obter os detalhes da participacao em ações educativas nos registros de matrícula correspondentes 
# MAGIC   identificados nos passoa anteriores que tenham sido atualizados (incremental), com todas as fotografias no período
# MAGIC   ==============================================================================================================================*/
# MAGIC ( 
# MAGIC    SELECT
# MAGIC    M.cod_matricula, 
# MAGIC    M.cod_matricula_dr, 
# MAGIC    P.dt_participacao,
# MAGIC    M.cod_turma, 
# MAGIC    M.cod_pessoa, 
# MAGIC    M.cod_estudante, 
# MAGIC    M.cod_financiamento, 
# MAGIC    NVL(M.ind_ebep,0)                    AS fl_ebep, 
# MAGIC    NVL(M.ind_tempo_integral,0)          AS fl_tempo_integral, 
# MAGIC    NVL(M.ind_viravida,0)                AS fl_viravida, 
# MAGIC    NVL(M.ind_exclusao,0)                AS fl_excluido_oltp, 
# MAGIC    --GREATEST(M.dat_registro, C.dat_alteracao_ciclo) AS dh_ultima_atualizacao_oltp, --retirado em 26/01/2021
# MAGIC    M.dat_atualizacao AS dh_ultima_atualizacao_oltp, -- incluído em 26/01/2021
# MAGIC    ROW_NUMBER() OVER(PARTITION BY M.cod_matricula ORDER BY M.dat_atualizacao DESC) AS SQ, -- troca de campo dat_registro em 26/01/2021
# MAGIC    M.dat_registro
# MAGIC    
# MAGIC    FROM       vw_matricula             M
# MAGIC    INNER JOIN "PARTICIPANTE"        P ON M.cod_matricula = P.cod_matricula
# MAGIC    --INNER JOIN "PARTICIPANTE_VALIDO" C ON C.cod_matricula = P.cod_matricula --retirado em 26/01/2021
# MAGIC    
# MAGIC    -- Filtros carga incremental, ciclo_matricula pode ter sido criado a posteriori e a matricula havia entrado e a sua dat_alteracao pode ter se mantido inalterada
# MAGIC    WHERE     TO_DATE(M.dat_atualizacao)        > #var_max_dh_ultima_atualizacao_oltp  -- troca de campo dat_registro em 26/01/2021
# MAGIC    --retirado em 26/01/2021
# MAGIC   --         OR TO_DATE(C.dat_alteracao_ciclo) > #var_max_dh_ultima_atualizacao_oltp )
# MAGIC 		   
# MAGIC ) "PARTICIPACAO"  
# MAGIC -- Traz todos os SQs para uso abaixo em características
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

# If there's no new data, then just let it die.
if first_load == False:
  if df_mat.filter(col("dat_atualizacao") > var_max_dh_ultima_atualizacao_oltp).count() == 0:
    dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Tables
# MAGIC And filter by var_max_dh_ultima_atualizacao_oltp
# MAGIC Note: it can't be done in the begging while reading because it's an optional ("or") condition

# COMMAND ----------

"""
This table has ~2mi records and ~ 140mb in mem cache at first load. We can cache it and keep it in 12 partitions. 
"""

w = Window.partitionBy("cod_matricula").orderBy(desc("dat_atualizacao"))


df_participacao = df_mat\
.join(df_participante, ["cod_matricula"], "inner")\
.filter(col("dat_atualizacao") > var_max_dh_ultima_atualizacao_oltp)\
.withColumn("sq", row_number().over(w))\
.withColumnRenamed("dat_atualizacao", "dh_ultima_atualizacao_oltp")\
.withColumnRenamed("dat_movimentacao", "dt_participacao")\
.withColumnRenamed("ind_ebep", "fl_ebep")\
.withColumnRenamed("ind_tempo_integral", "fl_tempo_integral")\
.withColumnRenamed("ind_viravida", "fl_viravida")\
.withColumnRenamed("ind_exclusao", "fl_excluido_oltp")\
.coalesce(12)\
.cache()

#activate cache
df_participacao.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 
# MAGIC ####(In this step we have the development of raw_trs_participacao_acao_educativa_sesi)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC  /*==============================================================================================================================
# MAGIC   PASSO 4: Atualizar a tabela "participacao_acao_educativa_sesi" (insert/update)com as informações obtidas nos respectivos PASSOs
# MAGIC   1 a 3 para SQ = 1 (versão mais atual) complementada com informações de turma, curso, unidade de atendimento e centro de responsabilidade  
# MAGIC   ==============================================================================================================================*/
# MAGIC 
# MAGIC  SELECT 
# MAGIC    P.cod_matricula       AS cd_participacao_acao_educativa, 
# MAGIC    P.cod_matricula_dr    AS cd_participacao_acao_educativa_dr, 
# MAGIC    P.dt_participacao,
# MAGIC    P.cod_turma           AS cd_turma_acao_educativa_sesi, 
# MAGIC    C.cod_curso           AS cd_acao_educativa_sesi, 
# MAGIC    TRIM(SUBSTRING(PS.cod_centro_responsabilidade, 3, 9)) AS cd_centro_responsabilidade, -- corresponde ao nível 5 do CR
# MAGIC    T.cod_unidade         AS cd_unidade_atendimento_dr,                          -- trs unidade_atendimento
# MAGIC    I.cod_instituicao_pai AS cd_entidade_regional,                               -- trs entidade_regional
# MAGIC    P.cod_pessoa          AS cd_pessoa_particip_educacao_sesi, 
# MAGIC    P.cod_estudante       AS cd_pessoa_particip_educacao_sesi_dr, 
# MAGIC    P.cod_financiamento, 
# MAGIC    P.fl_ebep, 
# MAGIC    P.fl_tempo_integral, 
# MAGIC    P.fl_viravida, 
# MAGIC    P.fl_excluido_oltp, 
# MAGIC    P.dh_ultima_atualizacao_oltp
# MAGIC    
# MAGIC    FROM "PARTICIPACAO" AS  P
# MAGIC    
# MAGIC    -- para obter turma, curso, unidade de atendimento e centro de responsabilidade  
# MAGIC    INNER JOIN (SELECT DISTINCT cod_turma, cod_unidade, cod_oferta_curso FROM turma) T ON P.cod_turma = T.cod_turma
# MAGIC    INNER JOIN (SELECT DISTINCT cod_oferta_curso, cod_curso FROM oferta_curso) OC ON T.cod_oferta_curso = OC.cod_oferta_curso
# MAGIC    INNER JOIN (SELECT cod_curso, cod_produto_servico,
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_curso ORDER BY dat_registro DESC) AS SQ 
# MAGIC                FROM curso) C 
# MAGIC          ON OC.cod_curso = C.cod_curso AND C.SQ = 1 
# MAGIC    INNER JOIN (SELECT DISTINCT cod_produto_servico, cod_centro_responsabilidade FROM produto_servico) PS 
# MAGIC          ON C.cod_produto_servico = PS.cod_produto_servico 
# MAGIC    
# MAGIC    -- para obter a entidade regional (DR) a partir da unidade de atendimento
# MAGIC    INNER JOIN (SELECT DISTINCT cod_instituicao, cod_instituicao_pai FROM instituicao) I ON I.cod_instituicao = T.cod_unidade
# MAGIC 
# MAGIC WHERE P.SQ = 1 
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

"""
This table has ~440k records at test period. We can keep it in 2 partitions.
"""

turma_useful_columns = ["cod_turma", "cod_unidade", "cod_oferta_curso"]

df_turma = spark.read.parquet(src_tu)\
.select(*turma_useful_columns)\
.distinct()\
.coalesce(2)

# COMMAND ----------

"""
This table has ~36k records at test period. We can keep it in 1 partition.
"""

oferta_curso_useful_columns = ["cod_oferta_curso", "cod_curso"]
df_oferta_curso = spark.read.parquet(src_oc)\
.select(*oferta_curso_useful_columns)\
.distinct()\
.coalesce(1)

# COMMAND ----------

"""
This table has ~31k records at test period. We can keep it in 1 partition.
"""

curso_useful_columns = ["cod_curso", "cod_produto_servico", "dat_registro"]
w = Window.partitionBy("cod_curso").orderBy(desc("dat_registro"))
df_curso = spark.read.parquet(src_cs)\
.select(*curso_useful_columns)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro", "row_number")\
.coalesce(1)

# COMMAND ----------

"""
This table has ~130 records at test period. We can keep it in 1 partition.
"""

produto_servico_useful_columns = ["cod_produto_servico", "cod_centro_responsabilidade"]
df_produto_servico = spark.read.parquet(src_ps)\
.select(*produto_servico_useful_columns)\
.distinct()\
.coalesce(1)

# COMMAND ----------

"""
This table has ~206k records at test period. We can keep it in 2 partitions.
"""

instituicao_useful_columns = ["cod_instituicao", "cod_instituicao_pai"]
df_instituicao = spark.read.parquet(src_in)\
.select(*instituicao_useful_columns)\
.distinct()\
.withColumnRenamed("cod_instituicao", "cod_unidade")\
.coalesce(1)

# COMMAND ----------

"""
This table has ~2mi records and ~ 175mb in mem cache at first load. We can cache it and keep it in 12 partitions. 

This will be used for participacao_acao_educativa_sesi, keeping only the latest version
"""

w = Window.partitionBy("cod_matricula").orderBy(desc("dat_registro"))

df_paes = df_participacao\
.filter(col("sq")==1)\
.join(df_turma, ["cod_turma"], "inner")\
.join(df_oferta_curso, ["cod_oferta_curso"], "inner")\
.join(df_curso, ["cod_curso"], "inner")\
.join(df_produto_servico, ["cod_produto_servico"], "inner")\
.join(df_instituicao, ["cod_unidade"], "inner")\
.withColumn("cod_centro_responsabilidade", trim(substring(col("cod_centro_responsabilidade"), 3, 9)))\
.drop("cod_produto_servico", "cod_oferta_curso", "dat_registro", "sq")\
.coalesce(12)\
.cache()

#activate cache
df_paes.count()

# COMMAND ----------

"""
This table has ~2mi records and ~ 45mb in mem cache at first load. We can cache it and keep it in 12 partitions. 

This will be used for participacao_acao_educativa_sesi_caracteristica, keeping all the available versions
"""

df_paesc = df_participacao.select("cod_matricula",
                                  "cod_financiamento",
                                  "fl_ebep",
                                  "fl_excluido_oltp",
                                  "dh_ultima_atualizacao_oltp")\
.coalesce(12)\
.cache()

#activate cache
df_paesc.count()

# COMMAND ----------

df_participacao = df_participacao.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing the new data of participacao_acao_educativa_sesi

# COMMAND ----------

df_participacao_acao_educativa_sesi = df_paes\
.fillna(0, subset=["fl_ebep",
                   "fl_tempo_integral",
                   "fl_viravida",
                   "fl_excluido_oltp"])

# COMMAND ----------

column_name_mapping = {'cod_matricula': 'cd_participacao_acao_educativa',
                       'cod_matricula_dr': 'cd_participacao_acao_educativa_dr',
                       'cod_turma': 'cd_turma_acao_educativa_sesi',
                       'cod_curso': 'cd_acao_educativa_sesi',
                       'cod_centro_responsabilidade': 'cd_centro_responsabilidade',
                       'cod_unidade': 'cd_unidade_atendimento',
                       'cod_instituicao_pai': 'cd_entidade_regional',                       
                       'cod_pessoa': 'cd_pessoa_particip_educacao_sesi',
                       'cod_estudante': 'cd_pessoa_particip_educacao_sesi_dr',                       
                       'cod_financiamento': 'cd_tipo_financiamento'}
  
for key in column_name_mapping:
  df_participacao_acao_educativa_sesi =  df_participacao_acao_educativa_sesi.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

trim_columns = ["cd_participacao_acao_educativa_dr"]

for c in trim_columns:
  df_participacao_acao_educativa_sesi = df_participacao_acao_educativa_sesi.withColumn(c, trim(df_participacao_acao_educativa_sesi[c]))  

# COMMAND ----------

column_type_map = {'cd_participacao_acao_educativa':'int',
                   'cd_participacao_acao_educativa_dr':'string',
                   'cd_turma_acao_educativa_sesi':'int',
                   'cd_acao_educativa_sesi':'int',
                   'cd_centro_responsabilidade':'string',
                   'cd_unidade_atendimento':'int',
                   'cd_entidade_regional':'int',
                   'dt_participacao':'date',
                   'cd_pessoa_particip_educacao_sesi':'int',
                   'cd_pessoa_particip_educacao_sesi_dr':'int',
                   'cd_tipo_financiamento':'int',
                   'fl_ebep':'int',
                   'fl_tempo_integral':'int',
                   'fl_viravida':'int',
                   'fl_excluido_oltp':'int',
                   'dh_ultima_atualizacao_oltp':'timestamp'}

for c in column_type_map:
  df_participacao_acao_educativa_sesi = df_participacao_acao_educativa_sesi.withColumn(c, df_participacao_acao_educativa_sesi[c].cast(column_type_map[c]))

# COMMAND ----------

#add control fields from trusted_control_field egg
df_participacao_acao_educativa_sesi = tcf.add_control_fields(df_participacao_acao_educativa_sesi, var_adf)\
.withColumn("cd_ano_atualizacao", year(col("dh_ultima_atualizacao_oltp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing the new data of participacao_acao_educativa_sesi_caracteristica

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cod_financiamento, fl_ebep and fl_excluido_oltp for each cod_matricula compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns = ["cod_matricula", "cod_financiamento", "fl_ebep", "fl_excluido_oltp"]
var_key = ["cod_matricula"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_paesc\
.withColumn("row_hash", concat_ws("@", *hash_columns))\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp")))).dropDuplicates()\
.withColumn("delete", when(col("row_hash") == col("lag_row_hash"), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop("delete", "row_hash", "lag_row_hash")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Schema preparation for versioning.
# MAGIC 
# MAGIC For the new records available in raw:
# MAGIC 
# MAGIC * fl_corrente = 1
# MAGIC * dh_inicio_vigencia = dat_registro
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

w = Window.partitionBy('cod_matricula')
df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.select('*', count('cod_matricula').over(w).alias('count_cd'))

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("dt_lead", lead("dh_ultima_atualizacao_oltp").over(w.orderBy("dh_ultima_atualizacao_oltp")))\
.withColumnRenamed("dh_ultima_atualizacao_oltp", "dh_inicio_vigencia")\
.withColumn("dh_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(col("dt_lead")))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dh_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
df_participacao_acao_educativa_sesi_caracteristica = tcf.add_control_fields(df_participacao_acao_educativa_sesi_caracteristica, var_adf)

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica.fillna(0, subset=['fl_ebep', 'fl_excluido_oltp'])

# COMMAND ----------

column_name_mapping = {"cod_matricula": "cd_participacao_acao_educativa", 
                       "cod_financiamento": "cd_tipo_financiamento"
                      }
for key in column_name_mapping:
  df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_participacao_acao_educativa": "int", 
                   "cd_tipo_financiamento": "int", 
                   "fl_ebep": "int", 
                   "fl_excluido_oltp": "int",
                   "dh_inicio_vigencia": "timestamp",
                   "dh_fim_vigencia": "timestamp",
                   "fl_corrente": "int"}

for c in column_type_map:
  df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  df_participacao_acao_educativa_sesi.coalesce(6).write.partitionBy("cd_ano_atualizacao").save(path=sink_paes, format="parquet", mode="overwrite")
  df_participacao_acao_educativa_sesi_caracteristica.coalesce(6).write.partitionBy("cd_ano_inicio_vigencia").save(path=sink_paesc, format="parquet", mode="overwrite")
  df_paes = df_paes.unpersist()
  df_paesc = df_paesc.unpersist()  
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### If it is not the first load:
# MAGIC * df_participacao_acao_educativa_sesi - Update
# MAGIC * df_participacao_acao_educativa_sesi_caracteristica - Versioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 
# MAGIC #### From df_participacao_acao_educativa_sesi

# COMMAND ----------

df_trs_participacao_acao_educativa_sesi = df_trs_participacao_acao_educativa_sesi\
.union(df_participacao_acao_educativa_sesi\
       .select(df_trs_participacao_acao_educativa_sesi.columns))

# COMMAND ----------

"""
Keep only last refresh date by cd_participacao_acao_educativa and get changed years
"""

w = Window.partitionBy("cd_participacao_acao_educativa").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs_participacao_acao_educativa_sesi = df_trs_participacao_acao_educativa_sesi.withColumn("rank", row_number().over(w))

# COMMAND ----------

years_with_change = df_participacao_acao_educativa_sesi.select("cd_ano_atualizacao").distinct()
if first_load == False:
  years_with_change = years_with_change.union(df_trs_participacao_acao_educativa_sesi.filter(col("rank") > 1)\
                                              .select("cd_ano_atualizacao")\
                                              .distinct()).dropDuplicates()

# COMMAND ----------

df_trs_participacao_acao_educativa_sesi = df_trs_participacao_acao_educativa_sesi\
.filter(col("rank") == 1)\
.drop("rank")

# COMMAND ----------

"""
Keep only changed years and write as year overwrite partition
"""

df_trs_participacao_acao_educativa_sesi = df_trs_participacao_acao_educativa_sesi.join(years_with_change, 
                                                                                       ["cd_ano_atualizacao"],
                                                                                       "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write in sink
# MAGIC #### From participacao_acao_educativa_sesi

# COMMAND ----------

df_trs_participacao_acao_educativa_sesi.coalesce(6).write.partitionBy("cd_ano_atualizacao").save(path=sink_paes, format="parquet", mode="overwrite")

df_paes = df_paes.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning 
# MAGIC #### From participacao_acao_educativa_sesi_caracteristica

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC  /*==============================================================================================================================
# MAGIC   PASSO 5: 03/09/2020: Atualizar a tabela "participacao_acao_educativa_sesi_caracteristica" (versionada)
# MAGIC   registrar as diferentes versões das características das informações no tempo, caso tenham existido correções nos campos abaixo
# MAGIC   ==============================================================================================================================*/
# MAGIC   SELECT 
# MAGIC   cod_matricula, 
# MAGIC   cod_financiamento, 
# MAGIC   fl_ebep, 
# MAGIC   fl_excluido_oltp,
# MAGIC   dh_ultima_atualizacao_oltp -- troca de campo dat_registro em 26/01/2021
# MAGIC   FROM "PARTICIPACAO"
# MAGIC   ORDER BY cod_matricula ASC, dh_ultima_atualizacao_oltp ASC -- troca de campo dat_registro em 26/01/2021
# MAGIC 
# MAGIC   /* Versionar participacao_acao_educativa_sesi_caracteristica*/  
# MAGIC   Para cada dh_ultima_atualizacao_oltp encontrada -- troca de campo dat_registro em 26/01/2021
# MAGIC     Verifica se existe na trs participacao_acao_educativa_sesi_caracteristica: raw.cod_matricula = trs.cd_participacao_acao_educativa com fl_corrente = 1
# MAGIC 
# MAGIC         Se existe, 
# MAGIC             Se houve alteração:
# MAGIC 	       raw.cod_financiamento          <> trs.cd_tipo_financiamento OU
# MAGIC 	       raw.fl_ebep                    <> trs.fl_ebep OU
# MAGIC  	       raw.fl_excluido_oltp           <> trs.fl_excluido_oltp
# MAGIC 						  
# MAGIC                UPDATE trs participacao_acao_educativa_sesi_caracteristica (existente)
# MAGIC                       set trs.dh_fim_vigencia = raw.dh_ultima_atualizacao_oltp, trs.fl_corrente = 0 -- troca de campo dat_registro em 26/01/2021
# MAGIC 					  
# MAGIC                INSERT trs participacao_acao_educativa_sesi_caracteristica (nova)	
# MAGIC                       trs.cd_participacao_acao_educativa = raw.cod_matricula
# MAGIC                       trs.cd_tipo_financiamento          = raw.cod_financiamento
# MAGIC                       trs.fl_ebep                        = raw.fl_ebep
# MAGIC                       trs.fl_excluido_oltp               = raw.fl_excluido_oltp
# MAGIC                       trs.dh_inicio_vigencia             = raw.dh_ultima_atualizacao_oltp -- troca de campo dat_registro em 26/01/2021
# MAGIC                       trs.dh_fim_vigencia                = NULL
# MAGIC                       trs.fl_corrente                    = 1
# MAGIC 					  
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC 			
# MAGIC         Senão, NÃO existe: INSERT trs participacao_acao_educativa_sesi_caracteristica (nova)	
# MAGIC  
# MAGIC </pre>

# COMMAND ----------

var_key = list(["cd_participacao_acao_educativa"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = df_trs_participacao_acao_educativa_sesi_caracteristica\
.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare = old_data_active. \
join(df_participacao_acao_educativa_sesi_caracteristica.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active. \
join(df_participacao_acao_educativa_sesi_caracteristica\
     .select(var_key)\
     .dropDuplicates(),
     var_key, "leftanti") 

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dh_fim_vigencia"

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.union(old_data_to_compare\
       .select(df_participacao_acao_educativa_sesi_caracteristica.columns))

# COMMAND ----------

hash_columns = var_key + ["cd_tipo_financiamento", "fl_ebep", "fl_excluido_oltp"]

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_inicio_vigencia")))).dropDuplicates()

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.withColumn("lead_dh_inicio_vigencia", lead("dh_inicio_vigencia", 1).over(var_window.orderBy(asc("dh_inicio_vigencia"))))

# COMMAND ----------

# When lead_dh_inicio_vigencia is not NULL, then we must close the period with "lead_dh_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica. \
withColumn("dh_fim_vigencia", when(col("lead_dh_inicio_vigencia").isNotNull(), col("lead_dh_inicio_vigencia"))). \
withColumn("fl_corrente", when(col("lead_dh_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dh_inicio_vigencia")

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"

old_data_inactive_to_rewrite = df_trs_participacao_acao_educativa_sesi_caracteristica. \
filter(col("fl_corrente") == 0). \
join(df_participacao_acao_educativa_sesi_caracteristica.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite. \
join(df_participacao_acao_educativa_sesi_caracteristica.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica = df_participacao_acao_educativa_sesi_caracteristica\
.union(old_data_to_rewrite.select(df_participacao_acao_educativa_sesi_caracteristica.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write in sink
# MAGIC #### From matricula_educacao_sesi_caracteristica

# COMMAND ----------

df_participacao_acao_educativa_sesi_caracteristica.coalesce(6).write.partitionBy("cd_ano_inicio_vigencia").save(path=sink_paesc, format="parquet", mode="overwrite")

df_paesc = df_paesc.unpersist()

# COMMAND ----------

