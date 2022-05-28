# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type update
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_matricula_educacao_sesi
# MAGIC Tabela/Arquivo Origem	
# MAGIC Principais: /raw/bdo/scae/matricula 
# MAGIC Relacionadas: 
# MAGIC /raw/bdo/scae/ciclo_matricula 
# MAGIC /raw/bdo/scae/ciclo 
# MAGIC /raw/bdo/scae/turma 
# MAGIC /raw/bdo/corporativo/instituicao
# MAGIC /raw/bdo/scae/oferta_curso
# MAGIC /raw/bdo/scae/curso 
# MAGIC /raw/bdo/scae/produto_servico
# MAGIC Tabela/Arquivo Destino	/trs/evt/matricula_educacao_sesi
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações de identificação (ESTÁVEIS) das matrículas em educação sesi que geram produção e que são contabilizados em termos de consolidação dos dados de produção SESI OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou sobrescreve o registro caso a chave cd_matricula_educacao_sesi seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC 
# MAGIC Dev: Tiago Shin / Marcela
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
var_tables =  {"origins": ["/bdo/scae/vw_matricula", "/bdo/scae/turma", "/bdo/scae/oferta_curso", "/bdo/scae/curso", "/bdo/scae/produto_servico", "/bdo/corporativo/instituicao"], "destination": ["/evt/matricula_educacao_sesi", "/evt/matricula_educacao_sesi_caracteristica"]}

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

src_mt, src_tu, src_oc, src_cs, src_ps, src_in= ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_mt, src_tu, src_oc, src_cs, src_ps, src_in)

# COMMAND ----------

sink_mes, sink_mesc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["destination"]]
print(sink_mes, sink_mesc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, year, max, current_timestamp, from_utc_timestamp, col, trim, substring, count, concat_ws, when, greatest, desc, current_timestamp, asc, broadcast, coalesce, lag, lead, concat_ws
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
  df_trs_matricula_educacao_sesi = spark.read.parquet(sink_mes).coalesce(6)
  df_trs_matricula_educacao_sesi_caracteristica = spark.read.parquet(sink_mesc).coalesce(6)
  var_max_dh_ultima_atualizacao_oltp = df_trs_matricula_educacao_sesi.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))
except AnalysisException:
  first_load = True

# COMMAND ----------

print(first_load, var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1
# MAGIC #### (This step refers to matricula_educacao_sesi and matricula_educacao_sesi_caracteristica)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /*Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from matricula_educacao_sesi */
# MAGIC /*Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 1: Obter matrículas ou ciclo que foram alterados no período. Um ciclo_matricula pode ter sido criado a posteriori à 
# MAGIC   criação de uma matricula cuja dat_alteracao pode ter se mantido antiga e não seria mais trazida, neste caso é preciso trazê-la.
# MAGIC   toda a matrícula tem que estar atrelada a um ciclo para ser considerada nas análises
# MAGIC   **** Todos os SQ ****
# MAGIC   ==============================================================================================================================*/
# MAGIC (
# MAGIC    SELECT M.*, ROW_NUMBER() OVER(PARTITION  M.cod_matricula ORDER BY M.dat_atualizacao DESC) as SQ,
# MAGIC    --GREATEST(M.dat_atualizacao, C.dat_alteracao) AS dh_ultima_atualizacao_oltp, -- retitado em 26/01/2021
# MAGIC    M.dat_atualizacao AS dh_ultima_atualizacao_oltp,    -- incluído em 26/01/2021
# MAGIC  
# MAGIC    FROM vw_matricula M 
# MAGIC    /*
# MAGIC    -- retitado em 26/01/2021
# MAGIC    INNER JOIN (SELECT CM.cod_matricula, MAX(CM.dat_alteracao) AS dat_atualizacao 
# MAGIC                FROM ciclo_matricula CL INNER JOIN ciclo CL ON C.cod_ciclo = CM.cod_ciclo
# MAGIC                WHERE YEAR(CL.dat_inicio_exercicio) >= 2013
# MAGIC                GROUP BY CM.cod_matricula) C
# MAGIC    ON M.cod_matricula = CC.cod_matricula
# MAGIC    */
# MAGIC    -- Filtros carga incremental 
# MAGIC    WHERE M.dat_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC    --OR    C.dat_atualizacao > #var_max_dh_ultima_atualizacao_oltp -- retitado em 26/01/2021
# MAGIC ) "MATRICULA"
# MAGIC </pre>

# COMMAND ----------

matricula_useful_columns = ["cod_matricula", "cod_matricula_dr", "cod_turma",  "cod_pessoa", "cod_estudante", "cod_escolaridade", "cod_financiamento", "ind_ebep", "ind_tempo_integral", "ind_viravida", "ind_mat_dentro_faixa_etaria", "ind_exclusao", "dat_atualizacao"]

# COMMAND ----------

"""
This table has ~12mi records and almost ~1gb in mem cache in test period. We can keep it to 24 partitions, maybe cache won't help us.
"""

w = Window.partitionBy("cod_matricula").orderBy(desc("dat_atualizacao"))

df_mat = spark.read.parquet(src_mt)\
.select(*matricula_useful_columns)\
.filter(col("dat_atualizacao") > var_max_dh_ultima_atualizacao_oltp)\
.withColumn("sq", row_number().over(w))\
.withColumnRenamed("dat_atualizacao", "dh_ultima_atualizacao_oltp")\
.distinct()\
.coalesce(24)

# COMMAND ----------

# If there's no new data, then just let it die.
if df_mat.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2
# MAGIC #### (This step refers to matricula_educacao_sesi and matricula_educacao_sesi_caracteristica)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 2: Obter detalhes das matrículas selecionadas no PASSO 1 para ser uma matrícula válida
# MAGIC   ==============================================================================================================================*/
# MAGIC (   
# MAGIC    SELECT
# MAGIC    M.cod_matricula, 
# MAGIC    M.cod_matricula_dr, 
# MAGIC    M.cod_turma, 
# MAGIC    C.cod_curso, 
# MAGIC    TRIM(SUBSTRING(PS.cod_centro_responsabilidade, 3, 15)) AS cd_centro_responsabilidade, -- corresponde ao n5 na trs centro_responsabilidade
# MAGIC    T.cod_unidade                  AS cd_unidade_atendimento_dr,                          -- trs unidade_atendimento
# MAGIC    I.cod_instituicao_pai          AS cd_entidade_regional,                               -- trs entidade_regional
# MAGIC    T.dat_inicio                   AS dt_entrada_prevista,
# MAGIC    T.dat_termino                  AS dt_saida_prevista,
# MAGIC    M.cod_pessoa, 
# MAGIC    M.cod_estudante, 
# MAGIC    M.cod_escolaridade, 
# MAGIC    M.cod_financiamento, 
# MAGIC    NVL(M.ind_ebep,0)                    AS fl_ebep, 
# MAGIC    NVL(M.ind_tempo_integral,0)          AS fl_tempo_integral, 
# MAGIC    NVL(M.ind_viravida,0)                AS fl_viravida, 
# MAGIC    NVL(M.ind_mat_dentro_faixa_etaria,0) AS fl_mat_dentro_faixa_etaria, 
# MAGIC    NVL(M.ind_exclusao,0)                AS fl_excluido_oltp, 
# MAGIC    M.dh_ultima_atualizacao_oltp 
# MAGIC    
# MAGIC    FROM  "MATRICULA" M 
# MAGIC    
# MAGIC    -- para obter a entrada e saída prevista da matrícula a partir das datas de inicio e fim da turma em que está matriculado 
# MAGIC    INNER JOIN (SELECT cod_turma, cod_unidade, cod_oferta_curso, dat_inicio, dat_termino,
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_turma ORDER BY dat_registro DESC) AS SQ
# MAGIC                FROM turma) T 
# MAGIC          ON M.cod_turma = T.cod_turma AND T.SQ = 1
# MAGIC    
# MAGIC    -- para obter o centro de responsabilidade  
# MAGIC    INNER JOIN (SELECT DISTINCT cod_oferta_curso, cod_curso FROM oferta_curso) OC ON T.cod_oferta_curso = OC.cod_oferta_curso
# MAGIC    INNER JOIN (SELECT cod_curso, cod_produto_servico,
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_curso ORDER BY dat_registro DESC) AS SQ 
# MAGIC                FROM curso) C 
# MAGIC          ON OC.cod_curso = C.cod_curso AND C.SQ = 1 
# MAGIC    INNER JOIN (SELECT DISTINCT cod_produto_servico, cod_centro_responsabilidade FROM produto_servico) PS 
# MAGIC          ON C.cod_produto_servico = PS.cod_produto_servico 
# MAGIC    
# MAGIC    -- para obter a unidade de atendimento e entidade regional (DR)
# MAGIC    INNER JOIN (SELECT DISTINCT cod_instituicao, cod_instituicao_pai FROM instituicao) I ON I.cod_instituicao = T.cod_unidade
# MAGIC    
# MAGIC    -- Filtros Matrículas em Educação SESI    
# MAGIC    WHERE (SUBSTRING(PS.cod_centro_responsabilidade, 3, 5) IN ('30301', '30302', '30303', '30304') -- EDUCACAO BASICA, EDUCACAO CONTINUADA, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC           OR SUBSTRING(PS.cod_centro_responsabilidade, 3, 9) = '305010103')                       -- FORMACAO CULTURAL (na verdade é um curso em EDUCACAO CONTINUADA)
# MAGIC    AND   SUBSTRING(PS.cod_centro_responsabilidade, 3, 7) <> '3030202'                             -- não contabilizar como matrícula: Eventos em EDUCACAO CONTINUADA
# MAGIC  ) "MATRICULA_DETALHE"  
# MAGIC  
# MAGIC </pre>

# COMMAND ----------

"""
This table has ~420k records at test period. We can keep it in 2 partitions.
"""

turma_useful_columns = ["cod_turma", "cod_unidade", "cod_oferta_curso", "dat_inicio", "dat_termino", "dat_registro"]
w = Window.partitionBy("cod_turma").orderBy(desc("dat_registro"))
df_turma = spark.read.parquet(src_tu)\
.select(*turma_useful_columns)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro", "row_number")\
.coalesce(1)

# COMMAND ----------

"""
This table has ~35k records at test period. We can keep it in 1 partition.
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
This table has ~92 records at test period. We can keep it in 1 partition.
"""

produto_servico_useful_columns = ["cod_produto_servico", "cod_centro_responsabilidade"]
df_produto_servico = spark.read.parquet(src_ps)\
.select(*produto_servico_useful_columns)\
.filter(((substring(col("cod_centro_responsabilidade"), 3, 5).isin(['30301', '30302', '30303', '30304'])) |\
        (substring(col("cod_centro_responsabilidade"), 3, 9) == '305010103')) &\
        (substring(col("cod_centro_responsabilidade"), 3, 7) != '3030202'))\
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

# MAGIC %md
# MAGIC #### Join Tables
# MAGIC And filter by var_max_dh_ultima_atualizacao_oltp
# MAGIC Note: it can't be done in the begging while reading because it's an optional ("or") condition

# COMMAND ----------

df_matricula_detalhe = df_mat\
.join(df_turma, ["cod_turma"], "inner")\
.join(df_instituicao, ["cod_unidade"], "inner")\
.join(df_oferta_curso, ["cod_oferta_curso"], "inner")\
.join(df_curso, ["cod_curso"], "inner")\
.join(df_produto_servico, ["cod_produto_servico"], "inner")\
.withColumn("cod_centro_responsabilidade", trim(substring(col("cod_centro_responsabilidade"), 3, 15)))\
.drop("cod_oferta_curso", "cod_produto_servico")\
.coalesce(4)\
.cache()

#df_matricula = df_matricula.coalesce(4).cache() if first_load is False else df_matricula.coalesce(12).cache()

#activate cache
df_matricula_detalhe.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 
# MAGIC #### (Removed step 3 and 4 in 11/05/2021)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /*==============================================================================================================================
# MAGIC   PASSO 5: Atualizar a tabela "matricula_educacao_sesi" (insert/update)
# MAGIC     ******* Filtrar SQ = 1 da MATRICULA_DETALHE ********
# MAGIC   ==============================================================================================================================*/
# MAGIC   SELECT 
# MAGIC    M.cod_matricula, 
# MAGIC    M.cod_matricula_dr, 
# MAGIC    M.cod_turma, 
# MAGIC    M.cod_curso, 
# MAGIC    M.d_centro_responsabilidade, 
# MAGIC    M.cd_unidade_atendimento_dr,                         
# MAGIC    M.cd_entidade_regional,                              
# MAGIC    M.dt_entrada_prevista,
# MAGIC    M.dt_saida_prevista,
# MAGIC    M.cod_pessoa, 
# MAGIC    M.cod_estudante, 
# MAGIC    M.cod_escolaridade, 
# MAGIC    M.cod_financiamento, 
# MAGIC    M.fl_ebep, 
# MAGIC    M.fl_tempo_integral, 
# MAGIC    M.fl_viravida, 
# MAGIC    M.fl_mat_dentro_faixa_etaria, 
# MAGIC    M.fl_excluido_oltp, 
# MAGIC    M.dh_ultima_atualizacao_oltp
# MAGIC    
# MAGIC    FROM "MATRICULA_DETALHE" M
# MAGIC    
# MAGIC    WHERE M.SQ = 1 -- versão mais recente da matrícula
# MAGIC </pre>

# COMMAND ----------

df_mes = df_matricula_detalhe\
.filter(col("sq") == 1)\
.drop("sq")\
.coalesce(4)\
.cache()

#activate cache
df_mes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC *==============================================================================================================================
# MAGIC   PASSO 6: Atualizar a tabela "matricula_educacao_sesi_caracteristica" (versionada)
# MAGIC   registrar as diferentes versões das características das matrícula no tempo, caso tenham existido correções nos campos abaixo
# MAGIC   ==============================================================================================================================*/
# MAGIC  SELECT 
# MAGIC   M.cod_matricula, 
# MAGIC   M.cod_financiamento, 
# MAGIC   M.fl_ebep, 
# MAGIC   M.fl_excluido_oltp,
# MAGIC   M.dh_ultima_atualizacao_oltp,
# MAGIC   FROM "MATRICULA_DETALHE" M
# MAGIC   ORDER BY M.cod_matricula ASC, M.dh_ultima_atualizacao_oltp ASC
# MAGIC    
# MAGIC </pre>

# COMMAND ----------

df_mesc = df_matricula_detalhe\
.select("cod_matricula", "cod_financiamento", col("ind_ebep").alias("fl_ebep"), col("ind_exclusao").alias("fl_excluido_oltp"), "dh_ultima_atualizacao_oltp")\
.coalesce(2)\
.cache()

#activate cache
df_mesc.count()

# COMMAND ----------

df_matricula_detalhe = df_matricula_detalhe.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing the new data of matricula_educacao_sesi

# COMMAND ----------

df_matricula_educacao_sesi = df_mes

# COMMAND ----------

column_name_mapping = {'cod_matricula': 'cd_matricula_educacao_sesi',
                       'cod_matricula_dr': 'cd_matricula_educacao_sesi_dr',
                       'cod_turma': 'cd_turma_educacao_sesi',
                       'cod_curso': 'cd_curso_educacao_sesi',
                       'cod_centro_responsabilidade': 'cd_centro_responsabilidade',
                       'cod_unidade': 'cd_unidade_atendimento',
                       'cod_instituicao_pai': 'cd_entidade_regional',
                       'dat_inicio': 'dt_entrada_prevista',
                       'dat_termino': 'dt_saida_prevista',
                       'cod_pessoa': 'cd_pessoa_aluno_educacao_sesi',
                       'cod_estudante': 'cd_aluno_educacao_sesi_dr',
                       'cod_escolaridade': 'cd_tipo_nivel_escolaridade',
                       'cod_financiamento': 'cd_tipo_financiamento',
                       'ind_ebep': 'fl_ebep',
                       'ind_tempo_integral': 'fl_tempo_integral',
                       'ind_viravida': 'fl_viravida',
                       'ind_mat_dentro_faixa_etaria': 'fl_mat_dentro_faixa_etaria',
                       'ind_exclusao': 'fl_excluido_oltp'}
  
for key in column_name_mapping:
  df_matricula_educacao_sesi =  df_matricula_educacao_sesi.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

trim_columns = ["cd_matricula_educacao_sesi_dr", "cd_aluno_educacao_sesi_dr"]

for c in trim_columns:
  df_matricula_educacao_sesi = df_matricula_educacao_sesi.withColumn(c, trim(df_matricula_educacao_sesi[c]))  

# COMMAND ----------

column_type_map = {'cd_matricula_educacao_sesi': 'int', 
                   'cd_matricula_educacao_sesi_dr': 'string',
                   'cd_turma_educacao_sesi': 'int',
                   'cd_curso_educacao_sesi': 'int',
                   'cd_centro_responsabilidade': 'string',
                   'cd_unidade_atendimento': 'int',
                   'cd_entidade_regional': 'int',                   
                   'dt_entrada_prevista': 'date',
                   'dt_saida_prevista': 'date',
                   'cd_pessoa_aluno_educacao_sesi': 'int',
                   'cd_aluno_educacao_sesi_dr': 'int',
                   'cd_tipo_nivel_escolaridade': 'int',
                   'cd_tipo_financiamento': 'int',
                   'fl_ebep': 'int',
                   'fl_tempo_integral': 'int',
                   'fl_viravida': 'int',
                   'fl_mat_dentro_faixa_etaria': 'int',
                   'fl_excluido_oltp': 'int',
                   'dh_ultima_atualizacao_oltp': 'timestamp',}

for c in column_type_map:
  df_matricula_educacao_sesi = df_matricula_educacao_sesi.withColumn(c, df_matricula_educacao_sesi[c].cast(column_type_map[c]))

# COMMAND ----------

df_matricula_educacao_sesi = df_matricula_educacao_sesi.fillna(0, subset=['fl_ebep', 'fl_tempo_integral', 'fl_viravida', 'fl_mat_dentro_faixa_etaria', 'fl_excluido_oltp'])

# COMMAND ----------

#add control fields from trusted_control_field egg
df_matricula_educacao_sesi = tcf.add_control_fields(df_matricula_educacao_sesi, var_adf)\
.withColumn("cd_ano_atualizacao", year(col("dh_ultima_atualizacao_oltp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing the new data of matricula_educacao_sesi_caracteristica

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_mesc

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cod_financiamento, fl_ebep and fl_excluido_oltp for each cod_matricula compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns = ["cod_matricula", "cod_financiamento", "fl_ebep", "fl_excluido_oltp"]
var_key = ["cod_matricula"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
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
# MAGIC * dh_inicio_vigencia = dh_ultima_atualizacao_oltp
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

w = Window.partitionBy('cod_matricula')
df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.select('*', count('cod_matricula').over(w).alias('count_cd'))

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("dt_lead", lead("dh_ultima_atualizacao_oltp").over(w.orderBy("dh_ultima_atualizacao_oltp")))\
.withColumnRenamed("dh_ultima_atualizacao_oltp", "dh_inicio_vigencia")\
.withColumn("dh_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(col("dt_lead")))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dh_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
df_matricula_educacao_sesi_caracteristica = tcf.add_control_fields(df_matricula_educacao_sesi_caracteristica, var_adf)

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica.fillna(0, subset=['fl_ebep', 'fl_excluido_oltp'])

# COMMAND ----------

column_name_mapping = {"cod_matricula": "cd_matricula_educacao_sesi", 
                       "cod_financiamento": "cd_tipo_financiamento"
                      }
for key in column_name_mapping:
  df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_matricula_educacao_sesi": "int", 
                   "cd_tipo_financiamento": "int", 
                   "fl_ebep": "int", 
                   "fl_excluido_oltp": "int",                   
                   "dh_inicio_vigencia": "timestamp",
                   "dh_fim_vigencia": "timestamp",
                   "fl_corrente": "int"}

for c in column_type_map:
  df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  df_matricula_educacao_sesi.coalesce(6).write.partitionBy("cd_ano_atualizacao").save(path=sink_mes, format="parquet", mode="overwrite")
  df_matricula_educacao_sesi_caracteristica.coalesce(6).write.partitionBy("cd_ano_inicio_vigencia").save(path=sink_mesc, format="parquet", mode="overwrite")
  df_mes = df_mes.unpersist()
  df_mesc = df_mesc.unpersist()  
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### If it is not the first load:
# MAGIC * df_matricula_educacao_sesi - Update
# MAGIC * df_matricula_educacao_sesi_caracteristica - Versioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 
# MAGIC #### From matricula_educacao_sesi

# COMMAND ----------

df_trs_matricula_educacao_sesi = df_trs_matricula_educacao_sesi\
.union(df_matricula_educacao_sesi\
       .select(df_trs_matricula_educacao_sesi.columns))

# COMMAND ----------

"""
Keep only last refresh date by cd_matricula_educacao_sesi and get changed years
"""

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs_matricula_educacao_sesi = df_trs_matricula_educacao_sesi.withColumn("rank", row_number().over(w))

# COMMAND ----------

years_with_change = df_matricula_educacao_sesi.select("cd_ano_atualizacao").distinct()
if first_load == False:
  years_with_change = years_with_change.union(df_trs_matricula_educacao_sesi.filter(col("rank") > 1)\
                                              .select("cd_ano_atualizacao")\
                                              .distinct()).dropDuplicates()

# COMMAND ----------

df_trs_matricula_educacao_sesi = df_trs_matricula_educacao_sesi\
.filter(col("rank") == 1)\
.drop("rank")

# COMMAND ----------

"""
Keep only changed years and write as year overwrite partition
"""

df_trs_matricula_educacao_sesi = df_trs_matricula_educacao_sesi.join(years_with_change, 
                                                                     ["cd_ano_atualizacao"],
                                                                     "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write in sink
# MAGIC #### From matricula_educacao_sesi

# COMMAND ----------

df_trs_matricula_educacao_sesi.coalesce(6).write.partitionBy("cd_ano_atualizacao").save(path=sink_mes, format="parquet", mode="overwrite")

df_mes = df_mes.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning 
# MAGIC #### From matricula_educacao_sesi_caracteristica

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC    /* Versionar matricula_educacao_sesi_caracteristica*/  
# MAGIC   Para cada dh_ultima_atualizacao_oltp encontrada
# MAGIC     Verifica se existe na trs matricula_educacao_sesi_caracteristica: raw.cod_matricula = trs.cd_matricula_educacao_sesi com fl_corrente = 1
# MAGIC 
# MAGIC         Se existe, 
# MAGIC             Se houve alteração:
# MAGIC 	       raw.cod_financiamento          <> trs.cd_tipo_financiamento OU
# MAGIC 	       raw.fl_ebep                    <> trs.fl_ebep OU
# MAGIC  	       raw.fl_excluido_oltp           <> trs.fl_excluido_oltp
# MAGIC 		   
# MAGIC 						  
# MAGIC                UPDATE trs matricula_educacao_sesi_caracteristica (existente)
# MAGIC                       set trs.dh_fim_vigencia = raw.dh_ultima_atualizacao_oltp, trs.fl_corrente = 0
# MAGIC 					  
# MAGIC                INSERT trs matricula_educacao_sesi_caracteristica (nova)	
# MAGIC                       trs.cd_matricula_educacao_sesi = raw.cod_matricula
# MAGIC                       trs.cd_tipo_financiamento      = raw.cod_financiamento
# MAGIC                       trs.fl_ebep                    = raw.fl_ebep
# MAGIC                       trs.fl_excluido_oltp           = raw.fl_excluido_oltp
# MAGIC                       trs.dh_inicio_vigencia         = raw.dh_ultima_atualizacao_oltp
# MAGIC                       trs.dh_fim_vigencia            = NULL
# MAGIC                       trs.fl_corrente                = 1
# MAGIC 			
# MAGIC 					  
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC 			
# MAGIC         Senão, NÃO existe: INSERT trs matricula_educacao_sesi_caracteristica (nova)	
# MAGIC   
# MAGIC </pre>

# COMMAND ----------

var_key = list(["cd_matricula_educacao_sesi"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = df_trs_matricula_educacao_sesi_caracteristica\
.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare = old_data_active. \
join(df_matricula_educacao_sesi_caracteristica.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active. \
join(df_matricula_educacao_sesi_caracteristica\
     .select(var_key)\
     .dropDuplicates(),
     var_key, "leftanti") 

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dh_fim_vigencia"

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.union(old_data_to_compare\
       .select(df_matricula_educacao_sesi_caracteristica.columns))

# COMMAND ----------

hash_columns = var_key + ["cd_tipo_financiamento", "fl_ebep", "fl_excluido_oltp"]

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_inicio_vigencia")))).dropDuplicates()

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.withColumn("lead_dh_inicio_vigencia", lead("dh_inicio_vigencia", 1).over(var_window.orderBy(asc("dh_inicio_vigencia"))))

# COMMAND ----------

# When lead_dh_inicio_vigencia is not NULL, then we must close the period with "lead_dh_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica. \
withColumn("dh_fim_vigencia", when(col("lead_dh_inicio_vigencia").isNotNull(), col("lead_dh_inicio_vigencia"))). \
withColumn("fl_corrente", when(col("lead_dh_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dh_inicio_vigencia")

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"

old_data_inactive_to_rewrite = df_trs_matricula_educacao_sesi_caracteristica. \
filter(col("fl_corrente") == 0). \
join(df_matricula_educacao_sesi_caracteristica.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite. \
join(df_matricula_educacao_sesi_caracteristica.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica = df_matricula_educacao_sesi_caracteristica\
.union(old_data_to_rewrite.select(df_matricula_educacao_sesi_caracteristica.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write in sink
# MAGIC #### From matricula_educacao_sesi_caracteristica

# COMMAND ----------

df_matricula_educacao_sesi_caracteristica.coalesce(6).write.partitionBy("cd_ano_inicio_vigencia").save(path=sink_mesc, format="parquet", mode="overwrite")

df_mesc = df_mesc.unpersist()