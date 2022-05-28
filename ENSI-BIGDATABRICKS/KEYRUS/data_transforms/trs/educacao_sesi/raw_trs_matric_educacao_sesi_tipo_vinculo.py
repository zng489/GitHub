# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type versionamento (insert + update)
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_matric_educacao_sesi_tipo_vinculo
# MAGIC Tabela/Arquivo Origem	Principais: /raw/bdo/scae/matricula Relacionadas: /raw/bdo/scae/ciclo_matricula /raw/bdo/scae/ciclo /raw/bdo/scae/matricula /raw/bdo/scae/turma /raw/bdo/scae/oferta_curso /raw/bdo/scae/curso /raw/bdo/scae/produto_servico
# MAGIC Tabela/Arquivo Destino	/trs/evt/matric_educacao_sesi_tipo_vinculo
# MAGIC Particionamento Tabela/Arquivo Destino	fl_corrente/DATE(dh_inicio_vigencia)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre o tipo de vínculo da matrícula em cursos de educação do SESI, que geram produção e que são contabilizados em termos de consolidação dos dados de produção. O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e < que data de fim e a posição atual com versão corrente informada = 1.
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dat_atualizacao e cod_matricula, que insere ou versiona o registro caso a chave cd_matricula_educacao_sesi seja encontrada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC 
# MAGIC Dev: Tiago Shin
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
var_tables =  {"origins": ["/bdo/scae/vw_matricula", "/bdo/scae/turma", "/bdo/scae/oferta_curso", "/bdo/scae/curso", "/bdo/scae/produto_servico"], "destination": "/evt/matric_educacao_sesi_tipo_vinculo"}

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
       'adf_pipeline_name': 'raw_trs_matric_educacao_sesi_tipo_vinculo',
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

src_mt, src_tu, src_oc, src_cs, src_ps = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_mt, src_tu, src_oc, src_cs, src_ps)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

from pyspark.sql.functions import substring, trim, lit, asc, desc, row_number, col, when, concat, length, upper, lower, dense_rank, count, from_utc_timestamp, current_timestamp, max, date_sub, year, isnull, lag, date_add, min, lead, array, to_date, concat_ws, first, coalesce
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import IntegerType, LongType, StringType
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC <b> FROM DOCUMENTATION </b>:
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from matricula_educacao_sesi_tipo_vinculo */
# MAGIC /* Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC SELECT
# MAGIC M.cod_matricula, 
# MAGIC M.cod_vinculo, 
# MAGIC M.num_cnpj_vinculo, 
# MAGIC M.dat_registro
# MAGIC FROM vw_matricula M
# MAGIC 
# MAGIC /*
# MAGIC --retirado em 26/01/2021
# MAGIC -- qualidade: toda a matrícula tem que estar atrelada a um ciclo para ser considerada nas análises (ciclos a partir de 2013) 
# MAGIC INNER JOIN (SELECT ciclo_matricula.cod_matricula, MAX(ciclo_matricula.dat_alteracao) as dat_alteracao 
# MAGIC             FROM ciclo_matricula INNER JOIN ciclo ON ciclo.cod_ciclo = ciclo_matricula.cod_ciclo
# MAGIC             WHERE YEAR(ciclo.dat_inicio_exercicio) >= 2013
# MAGIC             GROUP BY ciclo_matricula.cod_matricula) CC 
# MAGIC 	  ON M.cod_matricula = CC.cod_matricula
# MAGIC */
# MAGIC  
# MAGIC -- para obter o centro de responsabilidade   
# MAGIC INNER JOIN (SELECT cod_turma, cod_unidade, cod_oferta_curso, dat_inicio, dat_termino,
# MAGIC             ROW_NUMBER() OVER(PARTITION BY cod_turma ORDER BY dat_registro DESC) as SQ 
# MAGIC    	        FROM turma) T 
# MAGIC       ON M.cod_turma = T.cod_turma AND T.SQ = 1
# MAGIC INNER JOIN (SELECT DISTINCT cod_oferta_curso, cod_curso FROM oferta_curso) OC 
# MAGIC       ON T.cod_oferta_curso = OC.cod_oferta_curso
# MAGIC INNER JOIN (SELECT cod_curso, cod_produto_servico, 
# MAGIC             ROW_NUMBER() OVER(PARTITION BY cod_curso ORDER BY dat_registro DESC) as SQ 
# MAGIC             FROM curso) C 
# MAGIC       ON OC.cod_curso = C.cod_curso AND C.SQ = 1
# MAGIC INNER JOIN (SELECT DISTINCT cod_produto_servico, cod_centro_responsabilidade FROM produto_servico) PS 
# MAGIC       ON C.cod_produto_servico = PS.cod_produto_servico   
# MAGIC 
# MAGIC WHERE (SUBSTRING(PS.cod_centro_responsabilidade, 3, 5) IN ('30301', '30302', '30303', '30304')  -- EDUCACAO BASICA, EDUCACAO CONTINUADA, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC        OR SUBSTRING(PS.cod_centro_responsabilidade, 3, 9) = '305010103') -- FORMACAO CULTURAL (na verdade é um curso em EDUCACAO CONTINUADA)
# MAGIC AND SUBSTRING(PS.cod_centro_responsabilidade, 3, 7) <> '3030202' -- não contabilizar como matrícula: Eventos em EDUCACAO CONTINUADA
# MAGIC AND    M.dat_registro > #var_max_dh_ultima_atualizacao_oltp 
# MAGIC  
# MAGIC /* Como resultado, pode existir mais de um registro de atualização no período para o mesmo cd_atendimento caso exista gap de execução entre a car4ga raw e a trs*/
# MAGIC 
# MAGIC /* Versionar se e somente se houve alteração em cd_tipo_vinculo ou cd_cnpj_vinculo_matricula */
# MAGIC Para cada dat_registro encontrada
# MAGIC     Verifica se existe na trs matricula_educacao_sesi_tipo_vinculo: raw.cod_matricula = trs.cd_matricula_educacao_sesi e trs.fl_corrente = 1
# MAGIC         Se existe 
# MAGIC             Se houve alteração: raw.cod_vinculo <> trs.cd_tipo_vinculo ou 
# MAGIC             raw.num_cnpj_vinculo (sem formatacao) <> trs.cd_cnpj_vinculo_matricula
# MAGIC 
# MAGIC                 UPDATE trs matricula_educacao_sesi_tipo_vinculo (existente)
# MAGIC                     set trs.dh_fim_vigencia = dat_registro, trs.fl_corrente = 0, 
# MAGIC                         trs.dh_ultima_atualizacao_oltp = raw.dat_registro --inserido em 16/12/2020
# MAGIC 
# MAGIC                INSERT trs matricula_educacao_sesi_tipo_vinculo (nova)	
# MAGIC                     trs.cd_matricula_educacao_sesi = raw.cod_matricula
# MAGIC                     trs.cd_tipo_vinculo_matricula = raw.cod_vinculo
# MAGIC                     trs.cd_cnpj_vinculo_matricula = raw.num_cnpj_vinculo
# MAGIC                     trs.dh_inicio_vigencia = raw.dat_registro
# MAGIC                     trs.dh_fim_vigencia = NULL
# MAGIC                     trs.fl_corrente = 1
# MAGIC 		    trs.dh_ultima_atualizacao_oltp = raw.dat_registro --inserido em 16/12/2020
# MAGIC 
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs matricula_educacao_sesi_tipo_vinculo (nova)	
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting max date of dh_inicio_vigencia from last load

# COMMAND ----------

first_load = False
var_max_dh_inicio_vigencia = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  df_trs = spark.read.parquet(sink).coalesce(12)
  var_max_dh_inicio_vigencia = df_trs.select(max(col("dh_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)
print("Max dt inicio vigencia", var_max_dh_inicio_vigencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading raws and filtering

# COMMAND ----------

useful_columns_matricula = ["COD_MATRICULA", "COD_VINCULO", "NUM_CNPJ_VINCULO", "DAT_REGISTRO", "COD_TURMA"]
df_mat = spark.read.parquet(src_mt)\
.select(*useful_columns_matricula)\
.filter(col("DAT_REGISTRO") > var_max_dh_inicio_vigencia)\
.distinct()

df_mat = df_mat.coalesce(4) if first_load is False else df_mat.coalesce(24)

# COMMAND ----------

# If there's no new data, then just let it die.
if df_mat.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC Reading and filtering the others raws

# COMMAND ----------

useful_columns_turma = ["COD_TURMA", "COD_UNIDADE", "COD_OFERTA_CURSO", "DAT_INICIO", "DAT_TERMINO", "DAT_REGISTRO"]
w = Window.partitionBy("COD_TURMA").orderBy(desc("DAT_REGISTRO"))
df_t = spark.read.parquet(src_tu)\
.select(*useful_columns_turma)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("DAT_REGISTRO", "row_number")\
.distinct()\
.coalesce(4)

# COMMAND ----------

useful_columns_oferta_curso = ["COD_OFERTA_CURSO", "COD_CURSO"]
df_oc = spark.read.parquet(src_oc)\
.select(*useful_columns_oferta_curso)\
.distinct()\
.coalesce(1)

# COMMAND ----------

useful_columns_curso = ["COD_CURSO", "COD_PRODUTO_SERVICO", "DAT_REGISTRO"]
w = Window.partitionBy("COD_CURSO").orderBy(desc("DAT_REGISTRO"))
df_c = spark.read.parquet(src_cs)\
.select(*useful_columns_curso)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("DAT_REGISTRO", "row_number")\
.distinct()\
.coalesce(4)

# COMMAND ----------

useful_columns_produto_servico = ["COD_PRODUTO_SERVICO", "COD_CENTRO_RESPONSABILIDADE"]
df_ps = spark.read.parquet(src_ps).select(*useful_columns_produto_servico)\
.distinct()\
.filter(((substring(col("COD_CENTRO_RESPONSABILIDADE"), 3, 5).isin(['30301', '30302', '30303', '30304'])) |\
        (substring(col("COD_CENTRO_RESPONSABILIDADE"), 3, 9) == '305010103')) &\
        (substring(col("COD_CENTRO_RESPONSABILIDADE"), 3, 7) != '3030202'))\
.coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering with joins

# COMMAND ----------

df_raw = df_mat\
.join(df_t, ["COD_TURMA"], "inner")\
.join(df_oc, ["COD_OFERTA_CURSO"], "inner")\
.join(df_c, ["COD_CURSO"], "inner")\
.join(df_ps, ["COD_PRODUTO_SERVICO"], "inner")\
.select("COD_MATRICULA", "COD_VINCULO", "NUM_CNPJ_VINCULO", "DAT_REGISTRO")

# COMMAND ----------

#df_raw.count() #7987263

# COMMAND ----------

# MAGIC %md
# MAGIC Drop if there are more than 1 dat_registro for the same cod_matricula. It would break the logic, so we won't let it happen

# COMMAND ----------

df_raw = df_raw.dropDuplicates(["COD_MATRICULA", "DAT_REGISTRO"])\
.withColumn("dh_ultima_atualizacao_oltp", col("DAT_REGISTRO"))

# COMMAND ----------

#df_raw.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changing column names and types

# COMMAND ----------

column_name_mapping = {"COD_MATRICULA" : "cd_matricula_educacao_sesi", \
                       "COD_VINCULO" : "cd_tipo_vinculo_matricula", \
                       "NUM_CNPJ_VINCULO" : "cd_cnpj_vinculo_matricula", \
                       "DAT_REGISTRO" : "dat_registro"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

column_type_map = {"cd_matricula_educacao_sesi" : "int", \
                   "cd_tipo_vinculo_matricula" : "int", \
                   "cd_cnpj_vinculo_matricula" : "string"}

for c in column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifying changes in raw compared to the loaded trusted
# MAGIC - Hypothesis: Only cases with fl_corrente = 1 (active flag) are possibly changeable
# MAGIC - We'll ignore from raw those records that exists in trusted but does not have change and keep only the ones that doesn't exist in trusted and the ones that have changed
# MAGIC - For the first load, as we don't have a trusted loaded yet, we keep with the raw

# COMMAND ----------

# MAGIC %md
# MAGIC Drop records that didn't change cd_tipo_vinculo_matricula or cd_cnpj_vinculo_matricula for each cd_matricula_educacao_sesi compared to the last record
# MAGIC 
# MAGIC <pre>
# MAGIC WARNING NOTES
# MAGIC 
# MAGIC Why are we using concat with "|" to separate non-null values?
# MAGIC Because if we don't separate values by a string (in this case "|"), we could have misunderstandings between sequential columns with similar values. Ex: Imagine a dataframe with 2 records and 2 columns: rec1 = {col1: "10102", col2: "101"}, rec2 = {col1 = "1010", col2: "2101"}. Note that if we don't put a string between columns, the concatenation will be: 10102101 for both even if they have different values! 
# MAGIC Why don't use the pyspark sql function concat_ws? 
# MAGIC Because it doesn't put strings between the columns when the values as are null. So in this cases, we would have trouble.
# MAGIC 
# MAGIC Why don't use dropDuplicates or dense_rank instead of lag?
# MAGIC This question is a bit more tricky. Our goal is to do versioning if, and only if, there is change in any information. Remember that after generating the "unique_id", we have the "unique_id" and the "dat_registro"
# MAGIC You could imagine that if we use dropDuplicates, we'll only have 1 unique_id for each dat_registro and that it's enough. But, in this case, even if we accomplish the requirement "only if", we we'll NOT capture every change. Because if the same "unique_id" happens in distinct periods of time, we would be chosing only 1 arbitrarily to keep with. That could lead to problems when we run the script again to do versioning as we have this random characteristic.
# MAGIC So why don't use dense_rank ordered by dat_registro?
# MAGIC It's true that using dense rank we'll avoid the random nature of the operation, but we still would keep only the last "unique_id" for the same "cd_matricula_educacao_sesi". If the same "unique_id" happens more than 1 time and represents a change, it shouldn't be ignored.
# MAGIC So using lag enable us to compare the "unique_id" with the last one and keep only the records that had some change compared to the last "dat_registro"
# MAGIC </pre>

# COMMAND ----------

id_columns = ["cd_matricula_educacao_sesi", "cd_tipo_vinculo_matricula", "cd_cnpj_vinculo_matricula"]

# COMMAND ----------

def custom_concat(*cols):
    return concat(*[concat(coalesce(c, lit("|")), lit("@")) for c in cols])

# COMMAND ----------

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dat_registro").asc())
df_raw = df_raw.withColumn("unique_id", custom_concat(*id_columns))\
.withColumn("changed", (col("unique_id") != lag('unique_id', 1, 0).over(w)).cast("int"))\
.filter(col("changed") == 1)\
.drop("unique_id", "changed")\
.coalesce(8)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert records (active) & Update records with many ids

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b>FROM DOCUMENTATION</b>:
# MAGIC INSERT trs raw_trs_matric_educacao_sesi_tipo_vinculo (nova)	
# MAGIC                     trs.cd_matricula_educacao_sesi = raw.cod_matricula
# MAGIC                     trs.cd_tipo_vinculo_matricula = raw.cod_vinculo
# MAGIC                     trs.cd_cnpj_vinculo_matricula = raw.num_cnpj_vinculo
# MAGIC                     trs.dh_inicio_vigencia = raw.dat_registro
# MAGIC                     trs.dh_fim_vigencia = NULL
# MAGIC                     trs.fl_corrente = 1
# MAGIC                     trs.dh_ultima_atualizacao_oltp = raw.dat_registro --inserido em 16/12/2020
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC </pre>

# COMMAND ----------

w = Window.partitionBy('cd_matricula_educacao_sesi')
df_raw = df_raw.select('*', count('cd_matricula_educacao_sesi').over(w).alias('count_cd'))

# COMMAND ----------

new_records = df_raw\
.withColumn("dt_lead", lead("dat_registro").over(w.orderBy("dat_registro")))\
.withColumn("dh_inicio_vigencia", col("dat_registro"))\
.withColumn("dh_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(col("dt_lead")))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dh_inicio_vigencia")))\
.drop("dt_lead", "count_cd")

# COMMAND ----------

#add control fields from trusted_control_field egg
new_records = tcf.add_control_fields(new_records, var_adf)

# COMMAND ----------

#display(new_records)

# COMMAND ----------

# MAGIC %md
# MAGIC If first load, save it now!

# COMMAND ----------

if first_load is True:
  new_records = new_records.drop("dat_registro")
  # Partitioning as specified by the documentation
  new_records.coalesce(4).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b> FROM DOCUMENTATION </b>:
# MAGIC Verifica se existe na trs raw_trs_matric_educacao_sesi_tipo_vinculo: raw.cod_matricula = trs.cd_matricula_educacao_sesi e trs.fl_corrente = 1
# MAGIC     Se existe 
# MAGIC         Se houve alteração: raw.cod_vinculo <> trs.cd_tipo_vinculo ou 
# MAGIC         raw.num_cnpj_vinculo (sem formatacao) <> trs.cd_cnpj_vinculo_matricula
# MAGIC </pre>

# COMMAND ----------

#Add unique_id to raw and trusted that performs a contenation between the columns stated on id_columns and filter trusted by the active flag.
new_records = new_records.withColumn("unique_id", custom_concat(*id_columns))

df_trs_active = df_trs.filter(col("fl_corrente") == 1)\
.withColumn("unique_id", custom_concat(*id_columns))\
.cache()

#active cache
df_trs_active.count()

# COMMAND ----------

#Performing an "leftanti join" by unique_id means selecting only the records that are present in raw and not present in trs, that is, the new records or the ones the have some change between the columns in id_columns.
new_records = new_records.join(df_trs_active, ["unique_id"], how="leftanti")\
.drop("unique_id")\
.cache()

#active cache
new_records.count()

# COMMAND ----------

df_trs_active = df_trs_active.unpersist()

# COMMAND ----------

#Just drop "unique_id" as it won't be used anymore and change type of dt_congelamento_matricula to date to conform with the type in documentation.
df_trs_active = df_trs_active.drop("unique_id")

# COMMAND ----------

#Rename all columns of trusted adding "trs" prefix, except for the column cd_matricula_educacao_sesi
columns_trs_mapping = {}

for key in df_trs_active.columns:
  columns_trs_mapping["{}".format(key)] = "trs_" + key
del columns_trs_mapping["cd_matricula_educacao_sesi"]

for key in columns_trs_mapping:
  df_trs_active = df_trs_active.withColumnRenamed(key, columns_trs_mapping[key])

# COMMAND ----------

df_trs_active_2 = df_trs_active.cache()

df_trs_active_2.count()

# COMMAND ----------

#To get the changed records with both data from trusted and the new data, perform an "inner join"  between new_records and df_trs_active by the cd_matricula_educacao_sesi column 
changed_records = new_records.join(df_trs_active_2, ['cd_matricula_educacao_sesi'], 'inner')\
.cache()

changed_records.count()

# COMMAND ----------

#In trusted, we won't keep the column "dat_registro". Drop it here because we needed it to define "changed_records"
new_records_2 = new_records.drop("dat_registro")\
.cache()

new_records_2.count()

# COMMAND ----------

new_records = new_records.unpersist()

# COMMAND ----------

#new_records.count() , changed_records.count() , df_trs_active.count() #(213206, 0, 7923410)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete records that will be updated from original trusted
# MAGIC <pre>
# MAGIC Now we need to delete the changed records from the original trusted, because want to update it!
# MAGIC So the logic is to delete those records that we'll update later.
# MAGIC In order to perform this operation we need to select the same columns with the same name from changed_records.
# MAGIC </pre>

# COMMAND ----------

#print("count of old trusted before delete", df_trs.count())

# COMMAND ----------

#The records we need to update from trusted will be those with the same columns as the df_trs_active. This action is necessary in order to compare the old trusted and the records_to_update later
records_to_update = changed_records.select(df_trs_active_2.columns).distinct()

# COMMAND ----------

#As we already performed the join, the records to update can have the old name again. Also we can drop the df_trs_active as it won't be used anymore
inverse_columns_trs_mapping = {v: k for k, v in columns_trs_mapping.items()}
for key in inverse_columns_trs_mapping:
  records_to_update = records_to_update.withColumnRenamed(key, inverse_columns_trs_mapping[key])

# COMMAND ----------

#Finally, to delete the records that will be changed from trusted just subtract the old trusted by the records to update
df_trs = df_trs.subtract(records_to_update)

# COMMAND ----------

#print("count of records to update", records_to_update.count())
#print("count of old trusted after delete", df_trs.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update records (close validity)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Note: we need to discard the records in which "dh_fim_vigencia" is earlier than "dh_inicio_vigencia" not only because it does not make sense, but because otherwise we would have 2 records with the same "cd_matricula_educacao_sesi" and "dh_inicio_vigencia". So in these cases, we can safely ignore (drop) the records because in "new_records" we'll get one with more refreshed information
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b>FROM DOCUMENTATION</b>:
# MAGIC UPDATE trs matric_educ_basica_continuada_tipo_vinculo (existente)
# MAGIC     set trs.dh_fim_vigencia = dat_registro, trs.fl_corrente = 0
# MAGIC     trs.dh_ultima_atualizacao_oltp = raw.dat_registro --inserido em 16/12/2020
# MAGIC </pre>

# COMMAND ----------

#Now, it's time to get the correponding dt_atualizacao to close the validity of a previous open vigency. This value must be the oldest date that changed.
get_min_dt_atualizacao = changed_records.groupBy("cd_matricula_educacao_sesi", col("dh_insercao_trs").alias("new_dh_insercao_trs"))\
.agg(min(col("dat_registro")).alias("dat_registro"))

# COMMAND ----------

updated_records = records_to_update\
.join(get_min_dt_atualizacao,["cd_matricula_educacao_sesi"], how="left")\
.withColumn("dh_fim_vigencia", col("dat_registro"))\
.filter(col("dh_fim_vigencia") > col("dh_inicio_vigencia"))\
.withColumn("fl_corrente", lit(0).cast("int"))\
.withColumn("dh_ultima_atualizacao_oltp", col("dat_registro"))\
.drop("dat_registro", "dh_insercao_trs")\
.withColumnRenamed("new_dh_insercao_trs","dh_insercao_trs")\
.coalesce(12)\
.cache()

#active cache
updated_records.count()

# COMMAND ----------

#display(updated_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining new trusted with old trusted
# MAGIC <pre>
# MAGIC Defining new trusted as a union between the old records that had already been saved in the last load and the new records that we are loading right now
# MAGIC </pre>

# COMMAND ----------

#Specify order of columns to be able to do the union operation correctly
df_trs = df_trs.select(updated_records.columns)\
.union(new_records_2\
       .select(updated_records.columns)\
       .union(updated_records))

# COMMAND ----------

# MAGIC %md
# MAGIC Get years that have some change

# COMMAND ----------

years_with_change = new_records_2.select("cd_ano_inicio_vigencia").distinct()
if first_load == False:
  years_with_change = years_with_change.\
  union(records_to_update\
        .select("cd_ano_inicio_vigencia")\
        .distinct())\
  .dropDuplicates()

# COMMAND ----------

#display(years_with_change)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only changed years and write as incremental using partition

# COMMAND ----------

#df_trs.count()

# COMMAND ----------

df_trs = df_trs.join(years_with_change, ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink

# COMMAND ----------

df_trs.coalesce(4).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

new_records_2.unpersist(), df_trs_active_2.unpersist(), updated_records.unpersist(), changed_records.unpersist()

# COMMAND ----------

