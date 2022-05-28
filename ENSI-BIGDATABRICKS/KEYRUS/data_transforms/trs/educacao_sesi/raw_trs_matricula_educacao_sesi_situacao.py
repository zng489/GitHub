# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_matricula_educacao_sesi_situacao
# MAGIC Tabela/Arquivo Origem	"Principais:
# MAGIC /raw/bdo/scae/matricula_situacao
# MAGIC Relacionadas:
# MAGIC /raw/bdo/scae/matricula
# MAGIC /raw/bdo/scae/turma
# MAGIC /raw/bdo/scae/oferta_curso
# MAGIC /raw/bdo/scae/curso
# MAGIC /raw/bdo/scae/produto_servico"
# MAGIC Tabela/Arquivo Destino	/trs/evt/matricula_educacao_sesi_situacao
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre a situação da matrícula em curso em educação sesi, que geram produção e que são contabilizados em termos de consolidação dos dados de produção SESI. O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e < que data de fim e a posição atual com versão corrente informada = 1.
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dat_atualizacao e cod_matricula_situacao, que insere ou versiona o registro caso a chave cd_matric_educacao_sesi seja encontrada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Marcela Crozara
# MAGIC Manutenção: 
# MAGIC   2020/04/23 - Thomaz Moreira
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC There are many huge tables and joins in this execution, so we'll lower Treshold for broadcastjoin and increase timeout to avoid errors

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1048576)
spark.conf.set("spark.sql.broadcastTimeout", '3000000ms')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import re
import json

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSE
"""
var_tables =  {"origins": ["/bdo/scae/matricula_situacao", "/bdo/scae/vw_matricula", "/bdo/scae/turma", "/bdo/scae/oferta_curso", "/bdo/scae/curso", "/bdo/scae/produto_servico"], "destination": "/evt/matricula_educacao_sesi_situacao"}

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/raw", 
    "trusted": "/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_trs_matricula_educacao_sesi_situacao',
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

src_ms, src_mt, src_tu, src_oc, src_cs, src_ps = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_ms, src_mt, src_tu, src_oc, src_cs, src_ps)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max,min, to_date, year, month, current_date, dense_rank, asc, desc, greatest, trim, substring, when, lit, col, concat_ws, lead, lag, row_number, sum, date_format, rank
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC  
# MAGIC <pre>
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) 
# MAGIC    from matricula_educacao_sesi_situacao */
# MAGIC /* Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC 
# MAGIC SELECT 
# MAGIC S.cod_matricula,
# MAGIC S.cod_matricula_situacao,
# MAGIC S.cod_situacao,
# MAGIC S.dat_movimentacao,
# MAGIC S.fl_excluido,  ---- alterado em 16/04/2021
# MAGIC S.dh_insercao_raw,  ---- alterado em 16/04/2021
# MAGIC CASE S.dat_atualizacao >= #var_dh_processo THEN #var_dh_processo ELSE S.dat_atualizacao END AS dat_atualizacao 
# MAGIC FROM matricula_situacao S
# MAGIC INNER JOIN
# MAGIC 
# MAGIC -- filtrando apenas matrícula em EDUCACAO BASICA, EDUCACAO CONTINUADA (sem eventos) + FORMACAO CULTURAL, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC (
# MAGIC    SELECT DISTINCT M.cod_matricula FROM vw_matricula M
# MAGIC    
# MAGIC  /* --retirado em 26/01/2021
# MAGIC     -- qualidade: toda a matrícula tem que estar atrelada a um ciclo para ser considerada nas análises (ciclos a partir de 2013) 
# MAGIC    INNER JOIN (SELECT ciclo_matricula.cod_matricula, MAX(ciclo_matricula.dat_alteracao) as dat_alteracao 
# MAGIC       FROM ciclo_matricula INNER JOIN ciclo ON ciclo.cod_ciclo = ciclo_matricula.cod_ciclo
# MAGIC       WHERE YEAR(ciclo.dat_inicio_exercicio) >= 2013
# MAGIC       GROUP BY ciclo_matricula.cod_matricula) CC ON M.cod_matricula = CC.cod_matricula
# MAGIC  */  
# MAGIC    -- para obter o centro de responsabilidade   
# MAGIC    INNER JOIN (SELECT cod_turma, cod_unidade, cod_oferta_curso, dat_inicio, dat_termino,
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_turma ORDER BY dat_registro DESC) as SQ 
# MAGIC 			   FROM turma) T 
# MAGIC          ON M.cod_turma = T.cod_turma AND T.SQ = 1
# MAGIC    INNER JOIN (SELECT DISTINCT cod_oferta_curso, cod_curso FROM oferta_curso) OC 
# MAGIC          ON T.cod_oferta_curso = OC.cod_oferta_curso
# MAGIC    INNER JOIN (SELECT cod_curso, cod_produto_servico, 
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_curso ORDER BY dat_registro DESC) as SQ 
# MAGIC 			   FROM curso) C 
# MAGIC          ON OC.cod_curso = C.cod_curso AND C.SQ = 1
# MAGIC    INNER JOIN (SELECT DISTINCT cod_produto_servico, cod_centro_responsabilidade FROM produto_servico) PS 
# MAGIC          ON C.cod_produto_servico = PS.cod_produto_servico   
# MAGIC 
# MAGIC    WHERE (SUBSTRING(PS.cod_centro_responsabilidade, 3, 5) IN ('30301', '30302', '30303', '30304')  -- EDUCACAO BASICA, EDUCACAO CONTINUADA, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC           OR SUBSTRING(PS.cod_centro_responsabilidade, 3, 9) = '305010103') -- FORMACAO CULTURAL (na verdade é um curso em EDUCACAO CONTINUADA)
# MAGIC    AND SUBSTRING(PS.cod_centro_responsabilidade, 3, 7) <> '3030202' -- não contabilizar como matrícula: Eventos em EDUCACAO CONTINUADA
# MAGIC ) MT
# MAGIC ON S.cod_matricula = MT.cod_matricula
# MAGIC 
# MAGIC WHERE S.dat_atualizacao > #var_max_dh_ultima_atualizacao_oltp 
# MAGIC ORDER BY S.dat_movimentacao, S.dat_atualizacao, S.cod_matricula_situacao -- alterado em 06/07/2020
# MAGIC /* Pode existir mais de um registro de um registro de atualização no período */
# MAGIC 
# MAGIC /* Versionar */  ---- alterado em 16/0/2021 
# MAGIC Versionar para cada raw.cod_matricula, raw.cod_situacao encontrada 
# MAGIC     Ordenado ascendente raw.cod_matricula, raw.cod_situacao, raw.cod_matricula_situacao, raw.dat_atualizacao
# MAGIC 
# MAGIC     Verifica se existe na trs matricula_educacao_sesi_situacao raw.cod_matricula = trs.cd_matricula_educacao_sesi and raw.cod_situacao = trs.cd_tipo_situacao_matricula
# MAGIC 	com fl_corrente = 1
# MAGIC  
# MAGIC        Se existe
# MAGIC 	       Se fl_excluido = 1
# MAGIC 		       UPDATE trs matricula_educacao_sesi_situacao (existente)
# MAGIC                SET    trs.dh_fim_vigencia = raw.dh_insercao_raw, trs.fl_excluido = 1, trs.dh_ultima_atualizacao_oltp = raw.dh_insercao_raw, trs.fl_corrente = 0
# MAGIC            Else
# MAGIC 		      se a raw.dat_movimentacao > trs.dh_inicio_vigencia 
# MAGIC 				Sim
# MAGIC 	                UPDATE trs matricula_educacao_sesi_situacao (existente)
# MAGIC 		               SET trs.dh_fim_vigencia_oltp = raw.dat_movimentacao, 
# MAGIC 						trs.dh_ultima_atualizacao_oltp = raw.dat_atualizacao, trs.fl_corrente = 0
# MAGIC 
# MAGIC 				    INSERT trs matricula_educacao_sesi_situacao (nova)	
# MAGIC 					    trs.cd_matricula_educacao_sesi = raw.cod_matricula
# MAGIC 						trs.id_matricula_educacao_sesi_situacao_oltp = raw.cod_matricula_situacao
# MAGIC 						trs.cd_tipo_situacao_matricula = raw.cod_situacao
# MAGIC 	                    trs.dh_inicio_vigencia = raw.dat_movimentacao
# MAGIC 						trs.dh_fim_vigencia = NULL
# MAGIC 						trs.dh_ultima_atualizacao_oltp = raw.dat_atualizacao
# MAGIC 						trs.dh_primeira_atualizacao_oltp = raw.dat_atualizacao
# MAGIC 					    trs.fl_corrente = 1
# MAGIC 			  else
# MAGIC 			      se a raw.dat_movimentacao = trs.dh_inicio_vigencia 
# MAGIC 					  Sim
# MAGIC 					  UPDATE trs matricula_educacao_sesi_situacao (existente)
# MAGIC 							SET	trs.dh_ultima_atualizacao_oltp = raw.dat_atualizacao
# MAGIC                   else
# MAGIC 					  Ignora registro da raw e passa para o próximo
# MAGIC 				  end
# MAGIC 		      end
# MAGIC 		   end
# MAGIC 
# MAGIC         Senão, NÃO existe: INSERT trs matricula_educacao_sesi_situacao (nova)	
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get max date of dh_ultima_atualizacao_oltp from last load

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink).coalesce(6).cache()
  var_max_dh_ultima_atualizacao_oltp = trusted_data.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))
except AnalysisException:
  first_load = True

# COMMAND ----------

print(first_load, var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering

# COMMAND ----------

matricula_situacao_useful_columns = ["cod_matricula", "dat_movimentacao", "cod_matricula_situacao", "cod_situacao", "dat_atualizacao", "dh_insercao_raw", "fl_excluido"]

df_matricula_situacao = spark.read.parquet(src_ms).select(*matricula_situacao_useful_columns)\
.coalesce(16)\
.filter((col("dat_atualizacao") > var_max_dh_ultima_atualizacao_oltp))

# COMMAND ----------

#add control fields from trusted_control_field egg
df_matricula_situacao = tcf.add_control_fields(df_matricula_situacao, var_adf)\
.withColumn("dat_atualizacao", when(col("dat_atualizacao") > col("dh_insercao_trs"), 
                                    col("dh_insercao_trs")).otherwise(col("dat_atualizacao")))

# COMMAND ----------

# If there's no new data, then just let it die.
if df_matricula_situacao.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC From turma, select distinct by cod_turma, cod_unidade, cod_oferta_curso
# MAGIC </pre>

# COMMAND ----------

turma_useful_columns = ["cod_turma", "cod_unidade", "cod_oferta_curso", "dat_inicio", "dat_termino", "dat_registro"]
w = Window.partitionBy("cod_turma").orderBy(desc("dat_registro"))

df_turma = spark.read.parquet(src_tu).select(*turma_useful_columns)\
.coalesce(4)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro", "row_number")

# COMMAND ----------

#df_turma.count(), df_turma.select("cod_turma").distinct().count()
#421484

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC From oferta curso select distinct by cod_oferta_curso, cod_curso
# MAGIC </pre>

# COMMAND ----------

oferta_curso_useful_columns = ["cod_oferta_curso", "cod_curso"]
df_oferta_curso = spark.read.parquet(src_oc).select(*oferta_curso_useful_columns)\
.coalesce(1)\
.dropDuplicates()

# COMMAND ----------

#df_oferta_curso.count(), df_oferta_curso.select(*oferta_curso_unique_cols).distinct().count()
#35343

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC From curso, select distinct by cod_curso, cod_produto_servico
# MAGIC </pre>

# COMMAND ----------

curso_useful_columns = ["cod_curso", "cod_produto_servico", "dat_registro"]
w = Window.partitionBy("cod_curso").orderBy(desc("dat_registro"))

df_curso = spark.read.parquet(src_cs).select(*curso_useful_columns)\
.coalesce(4)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro", "row_number")

# COMMAND ----------

#df_curso.count(), df_curso.select("cod_curso").distinct().count()
#30883

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC From produto servico, select distinct by cod_produto_servico, cod_centro_responsabilidade
# MAGIC </pre>

# COMMAND ----------

produto_servico_useful_columns = ["cod_produto_servico", "cod_centro_responsabilidade"]

df_produto_servico = spark.read.parquet(src_ps).select(*produto_servico_useful_columns)\
.filter(
  (
    (substring(col("cod_centro_responsabilidade"), 3, 5)).isin(['30301', '30302', '30303', '30304']) | \
     (substring(col("cod_centro_responsabilidade"), 3, 9) == '305010103') \
  ) & \
  (substring(col("cod_centro_responsabilidade"), 3, 7) != '3030202'))\
.coalesce(1)\
.dropDuplicates()

# COMMAND ----------

#df_produto_servico.count(), df_produto_servico.select(*produto_servico_unique_cols).distinct().count()
#92

# COMMAND ----------

# MAGIC %md
# MAGIC #### Joining all tables

# COMMAND ----------

matricula_useful_columns = ["cod_matricula", "cod_turma"]

df_intermediate = spark.read.parquet(src_mt).select(*matricula_useful_columns). \
join(df_turma, ["cod_turma"], "inner"). \
join(df_oferta_curso, ["cod_oferta_curso"], "inner"). \
join(df_curso, ["cod_curso"], "inner"). \
join(df_produto_servico, ["cod_produto_servico"], "inner"). \
select("cod_matricula"). \
dropDuplicates()

# COMMAND ----------

#df_intermediate.count(), df_intermediate.distinct().count()
#7417555

# COMMAND ----------

new_data = df_matricula_situacao.join(df_intermediate, ["cod_matricula"], "inner")

# COMMAND ----------

#new_data.count(), new_data.select("cod_matricula").distinct().count()
#(12850463, 7387552)

# COMMAND ----------

del df_turma
del df_oferta_curso
del df_curso
del df_produto_servico
del df_matricula_situacao
del df_intermediate

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming columns and adjusting types

# COMMAND ----------

column_name_mapping = {'cod_matricula': 'cd_matricula_educacao_sesi',
                       'cod_matricula_situacao': 'id_matricula_educacao_sesi_situacao_oltp',
                       'cod_situacao': 'cd_tipo_situacao_matricula',
                       'dat_movimentacao': 'dh_inicio_vigencia',
                       'dat_atualizacao': 'dh_primeira_atualizacao_oltp'
                       }
  
for key in column_name_mapping:
  new_data =  new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {'cd_matricula_educacao_sesi': 'int',
                   'id_matricula_educacao_sesi_situacao_oltp': 'int',
                   'cd_tipo_situacao_matricula': 'int',
                   'dh_inicio_vigencia': 'timestamp',
                   'dh_primeira_atualizacao_oltp': 'timestamp' }
for c in column_type_map:
  new_data = new_data.withColumn(c, new_data[c].cast(column_type_map[c]))

# COMMAND ----------

new_data = new_data.withColumn("dh_ultima_atualizacao_oltp", col("dh_primeira_atualizacao_oltp")) \
                   .withColumn("dh_fim_vigencia", lit(None).cast("timestamp")) \
                   .withColumn("fl_corrente", lit(1).cast("int")) \
                   .withColumn("cd_ano_atualizacao", year(col("dh_primeira_atualizacao_oltp"))) \
                   .select("cd_matricula_educacao_sesi", 
                           "dh_inicio_vigencia", 
                           "id_matricula_educacao_sesi_situacao_oltp", 
                           "cd_tipo_situacao_matricula", 
                           "dh_primeira_atualizacao_oltp",
                           "dh_ultima_atualizacao_oltp",
                           "dh_fim_vigencia",
                           "fl_corrente", 
                           "fl_excluido",
                           "dh_insercao_raw",
                           "dh_insercao_trs", 
                           "kv_process_control", 
                           "cd_ano_atualizacao")

# COMMAND ----------

if first_load is True:
  # If it is the first load, create an empty DataFrame of the same schema that corresponds to trusted_data
  trusted_data = spark.createDataFrame([], schema=new_data.schema)
else:
  trusted_data = trusted_data.withColumn("dh_insercao_raw", lit(None).cast("timestamp"))\
  .select("cd_matricula_educacao_sesi", 
          "dh_inicio_vigencia", 
          "id_matricula_educacao_sesi_situacao_oltp", 
          "cd_tipo_situacao_matricula", 
          "dh_primeira_atualizacao_oltp",
          "dh_ultima_atualizacao_oltp",
          "dh_fim_vigencia",
          "fl_corrente", 
          "fl_excluido",
          "dh_insercao_raw",
          "dh_insercao_trs", 
          "kv_process_control", 
          "cd_ano_atualizacao")

# COMMAND ----------

# MAGIC %md
# MAGIC Versioning
# MAGIC 
# MAGIC From all the old data available in trusted, the intersection of *keys_from_new_data* and *trusted_data* ACTIVE RECORDS will give us what has changed. Only active records can change!
# MAGIC 
# MAGIC What changes in old_data_to_change is that fl_corrente = 0

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = trusted_data.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available on trusted that needs update.
old_data_to_update = old_data_active.join(new_data.select("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula").dropDuplicates(), 
                                          ["cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula"], 
                                          "inner")

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active.join(new_data.select("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula").dropDuplicates(), 
                                                  ["cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula"],
                                                  "leftanti") 

# COMMAND ----------

new_data = new_data.withColumn("is_new", lit(1).cast("int"))
old_data_to_update = old_data_to_update.withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

new_data = new_data.union(old_data_to_update.select(new_data.columns))

# COMMAND ----------

new_data = new_data\
.withColumn("max_atualizacao", 
            max("dh_ultima_atualizacao_oltp")\
            .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula", 
                                     "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp", "fl_excluido")))\
.withColumn("min_atualizacao", 
            min("dh_primeira_atualizacao_oltp")\
            .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula",
                                     "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp", "fl_excluido")))\
.withColumn("row_number", row_number() \
            .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula",
                                     "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp", "fl_excluido")\
                  .orderBy(asc("dh_primeira_atualizacao_oltp"))))\
.filter(col("row_number")==1)\
.withColumn("dh_primeira_atualizacao_oltp", col("min_atualizacao"))\
.withColumn("dh_ultima_atualizacao_oltp", col("max_atualizacao"))\
.drop("row_number", "min_atualizacao", "max_atualizacao")


# COMMAND ----------

new_data = new_data\
.withColumn("lead_fl_excluido", lead("fl_excluido", 1)\
            .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula", "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp")\
                  .orderBy(asc("fl_excluido"))))\
.withColumn("lead_dh_insercao_raw", lead("dh_insercao_raw", 1)\
            .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula", "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp")\
                  .orderBy(asc("fl_excluido"))))\
.filter(col("fl_excluido")==0)\
.withColumn("dh_fim_vigencia", when(col("lead_fl_excluido")==1, col("lead_dh_insercao_raw")).otherwise(col("dh_fim_vigencia")))\
.withColumn("dh_ultima_atualizacao_oltp", when(col("lead_fl_excluido")==1, col("lead_dh_insercao_raw")).otherwise(col("dh_ultima_atualizacao_oltp")))\
.withColumn("fl_excluido", when(col("lead_fl_excluido")==1, lit(1)).otherwise(col("fl_excluido")))\
.withColumn("fl_corrente", when(col("lead_fl_excluido")==1, lit(0)).otherwise(col("fl_corrente")))\
.drop("lead_fl_excluido", "lead_dh_insercao_raw")

# COMMAND ----------

#identifies the last occurrence by the field dh_inicio_vigencia (for the new records the date 1800-01-01 is considered)
new_data = new_data\
.withColumn("ultima_ocorrencia", 
            when((col("is_new")==0) & (col("fl_excluido")==0), 
                 col("dh_inicio_vigencia"))\
            .otherwise(lit(datetime.datetime(1800, 1, 1,0,0,0))))\
.withColumn("ultima_ocorrencia", max("ultima_ocorrencia").over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula")))

# COMMAND ----------

#Identify the new records to delete (Verifica se a raw.dat_movimentacao >= trs.dh_inicio_vigencia e raw.cod_situacao <> trs.cd_tipo_situacao_matricula).Remove all recrods marked "delete" = 1. Also, we don't need this column anymore.
new_data = new_data\
.withColumn("delete",
            when((col("dh_inicio_vigencia") >= col("ultima_ocorrencia")), 0)\
            .otherwise(1))\
.filter(col("delete") == 0)\
.drop(*["delete", "is_new", "ultima_ocorrencia"])

# COMMAND ----------

# Now, case we've got more than one record to add, we've go to balance all the old one from trusted and also the intermediate ones.
new_data = new_data.withColumn("row_number", row_number() \
                               .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula", "fl_excluido") \
                                     .orderBy(asc("id_matricula_educacao_sesi_situacao_oltp"), asc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

# Now I'll just have to add who's max("row_number") so it's possible to know what records are old and needs update except for the newest one.
new_data = new_data.withColumn("max_row_number", max("row_number").over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula")))
# Counting after, no records are lost

# COMMAND ----------

#Getting what is the dh_inicio_vigencia of the subsequent record
new_data = new_data.withColumn("lead_dh_inicio_vigencia", lead("dh_inicio_vigencia", 1)\
                               .over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_tipo_situacao_matricula", "fl_excluido")\
                                     .orderBy(asc("id_matricula_educacao_sesi_situacao_oltp"),asc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

# Now for the old active records and the new records, we apply:
new_data = new_data\
.withColumn("fl_corrente", 
            when((col("max_row_number") != col("row_number")) & (col("fl_excluido")==0), 0)\
            .otherwise(col("fl_corrente"))) \
.withColumn("dh_fim_vigencia", 
            when(col("fl_excluido")==0, col("lead_dh_inicio_vigencia"))\
            .otherwise(col("dh_fim_vigencia")))

# COMMAND ----------

new_data = new_data.drop(*["row_number", "max_row_number", "lead_dh_inicio_vigencia"])

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "cd_ano_atualizacao"
old_data_inactive_to_rewrite = trusted_data. \
filter(col("fl_corrente") == 0). \
join(new_data.select("cd_ano_atualizacao").dropDuplicates(), ["cd_ano_atualizacao"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite.join(new_data.select("cd_ano_atualizacao").dropDuplicates(), ["cd_ano_atualizacao"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite.select(old_data_inactive_to_rewrite.columns))

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns))

# COMMAND ----------

new_data = new_data.drop("dh_insercao_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(10).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Unpersisting cached data

# COMMAND ----------

trusted_data.unpersist()

# COMMAND ----------

