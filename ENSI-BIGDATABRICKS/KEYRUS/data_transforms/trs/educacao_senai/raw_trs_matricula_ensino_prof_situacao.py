# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type versionamento (insert + update)
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_matricula_ensino_prof_situacao
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_atendimento_matricula /raw/bdo/bd_basi/tb_atendimento
# MAGIC Tabela/Arquivo Destino	/trs/evt/matricula_ensino_prof_situacao
# MAGIC Particionamento Tabela/Arquivo Destino	fl_corrente/YEAR(dt_inicio)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre a situação da matrícula no ensino profissional, que geram produção e que são contabilizados em termos de consolidação dos dados de produção SENAI. O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e < que data de fim e a posição atual com versão corrente informada = 1 OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), matricula_ensino_profissional.
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou versiona o registro caso a chave cd_atendimento seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00.
# MAGIC 
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

import re
import json

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_atendimento_matricula", "/bdo/bd_basi/tb_atendimento"], "destination": "/evt/matricula_ensino_prof_situacao"}

var_dls = {
  "folders":{
    "landing":"/lnd",
    "error":"/err", 
    "staging":"/stg", 
    "log":"/log", 
    "raw":"/raw", 
    "trusted": "/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': '',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-10-09T11:20:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_tbm, src_tb = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tbm, src_tb)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, asc, desc, when, lit, col, concat_ws, lead, lag, date_add, count, array, lower, trim
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dh_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX("dh_inicio_vigencia") from matricula_ensino_prof_situacao
# MAGIC 
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink)
  var_max_dh_atualizacao = trusted_data.select(max(col("dh_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

var_max_dh_atualizacao

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC /*=============================================================================================================
# MAGIC   Passo 1: obter as matriculas ordenadas dt_atualizacao					
# MAGIC   =============================================================================================================*/
# MAGIC   (
# MAGIC   SELECT 
# MAGIC   cd_atendimento, 
# MAGIC   cd_tipo_situacao_matricula, 
# MAGIC   dt_congelamento_matricula, 
# MAGIC   dt_atualizacao,
# MAGIC   
# MAGIC   --campos incluídos em 03/09/2020
# MAGIC   qt_horas_pratica_profis
# MAGIC   
# MAGIC   FROM tb_atendimento_matricula m 
# MAGIC   WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp 
# MAGIC   ORDER BY dt_atualizacao
# MAGIC   ) "MATRICULA"
# MAGIC   
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

#var_max_first_load = datetime.datetime.strptime("01/09/2020 00:00", "%d/%m/%Y %H:%M")
#var_max_first_load                                    
#& (col("dt_atualizacao") < var_max_first_load)

# COMMAND ----------

tbm_columns = ["cd_atendimento", "cd_tipo_situacao_matricula", "dt_congelamento_matricula", "dt_atualizacao", "qt_horas_pratica_profis"]

df_matricula = spark.read.parquet(src_tbm). \
select(*tbm_columns). \
filter(col("dt_atualizacao") > var_max_dh_atualizacao). \
dropDuplicates(). \
coalesce(8). \
cache()

df_matricula.count()

# COMMAND ----------

# If there's no new data, then just let it die.
if df_matricula.count() == 0:
  df_matricula = df_matricula.unpersist()
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC /*=============================================================================================================
# MAGIC   Passo 2: obter os atendimentos para as distintas matriculas, evitando produto cartesiano
# MAGIC   Passo 3: ordenar o atendimento por data de atualizacao e criar a "data fim" do registro atraves de LEAD
# MAGIC   =============================================================================================================*/
# MAGIC   (
# MAGIC    SELECT 
# MAGIC    a.cd_atendimento, 
# MAGIC    a.dt_termino, 
# MAGIC    a.dt_atualizacao as dt_atualizacao_ini,
# MAGIC    
# MAGIC    --campo incluído em 03/09/2020
# MAGIC    a.fl_excluido,
# MAGIC    a.cd_tipo_gratuidade,
# MAGIC    a.dt_inicio,
# MAGIC 
# MAGIC    LEAD(a.dt_atualizacao,1,CAST('2090-12-31' as date)) OVER(PARTITION BY  a.cd_atendimento ORDER BY a.dt_atualizacao) AS dt_atualizacao_fim
# MAGIC    
# MAGIC    FROM tb_atendimento a
# MAGIC    
# MAGIC    INNER JOIN (SELECT DISTINCT cd_atendimento FROM "MATRICULA") m
# MAGIC    ON a.cd_atendimento = m.cd_atendimento
# MAGIC    
# MAGIC    ORDER BY a.dt_atualizacao
# MAGIC   ) "ATENDIMENTO"
# MAGIC 
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

tb_columns = ["cd_atendimento", "dt_termino", "dt_atualizacao", "fl_excluido", "cd_tipo_gratuidade", "dt_inicio"]

df_atendimento = spark.read.parquet(src_tb). \
select(*tb_columns). \
join(df_matricula.select("cd_atendimento").distinct(),
    ["cd_atendimento"], "inner"). \
withColumn("dt_atualizacao_fim", 
            lead("dt_atualizacao", 1, "2090-12-31")
            .over(Window.partitionBy("cd_atendimento").orderBy(asc("dt_atualizacao")))). \
withColumnRenamed("dt_atualizacao", "dt_atualizacao_ini"). \
withColumnRenamed("cd_atendimento", "cd_atendimento_encontrado"). \
coalesce(8). \
cache()


df_atendimento.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC /*============================================================================================================================
# MAGIC   Passo 4: encontrar no atendimento o registros que satisfaça para o cd_atendimento a dt_atualizacao da matricula < data fim e
# MAGIC   dt_atualizacao da matricula >= data ini atualizacao do atendimento					
# MAGIC   ============================================================================================================================*/  
# MAGIC   (
# MAGIC   SELECT 
# MAGIC   m.cd_atendimento,
# MAGIC   m.cd_tipo_situacao_matricula, 
# MAGIC   m.dt_congelamento_matricula, 
# MAGIC   m.dt_atualizacao, 
# MAGIC   a.dt_termino, 
# MAGIC   a.cd_atendimento as cd_atendimento_encontrado,
# MAGIC   
# MAGIC   --campos incluídos em 03/09/2020
# MAGIC   m.qt_horas_pratica_profis,
# MAGIC   a.cd_tipo_gratuidade,
# MAGIC   a.fl_excluido,
# MAGIC   a.dt_inicio,
# MAGIC   
# MAGIC   FROM "MATRICULA" m
# MAGIC   LEFT JOIN "ATENDIMENTO" a
# MAGIC   ON m.cd_atendimento = a.cd_atendimento
# MAGIC   AND  m.dt_atualizacao >= a.dt_atualizacao_ini AND m.dt_atualizacao < a.dt_atualizacao_fim
# MAGIC   ) "MATRIC_ATEND"
# MAGIC 
# MAGIC   REGISTROS "MATRIC_ATEND" COM cd_atendimento_encontrado <> NULL, podem seguir para o PASSO 6
# MAGIC   
# MAGIC   REGISTROS "MATRIC_ATEND" COM cd_atendimento_encontrado = NULL, devem seguir para o PASSO 5 (repescagem) antes
# MAGIC   
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

df_matric_atend = df_matricula. \
join(df_atendimento,
    ((df_matricula.cd_atendimento == df_atendimento.cd_atendimento_encontrado) &\
    (df_matricula.dt_atualizacao >= df_atendimento.dt_atualizacao_ini) &\
    (df_matricula.dt_atualizacao < df_atendimento.dt_atualizacao_fim)),
    "left"). \
drop("dt_atualizacao_ini","dt_atualizacao_fim"). \
coalesce(8). \
cache()

df_matric_atend.count()

# COMMAND ----------

df_matricula = df_matricula.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC /*============================================================================================================================
# MAGIC   Passo 5: tratar a lista de matrículas que não foram atendidas no passo 4, na "repescagem"
# MAGIC   ============================================================================================================================*/  
# MAGIC   SELECT 
# MAGIC   m.cd_atendimento, 
# MAGIC   m.cd_tipo_situacao_matricula, 
# MAGIC   m.dt_congelamento_matricula, 
# MAGIC   m.dt_atualizacao,
# MAGIC   a.dt_inicio,
# MAGIC   a.dt_termino,
# MAGIC   a.cd_atendimento as cd_atendimento_encontrado,
# MAGIC  
# MAGIC   --campos incluídos em 03/09/2020
# MAGIC   m.qt_horas_pratica_profis,
# MAGIC   a.cd_tipo_gratuidade,
# MAGIC   a.fl_excluido 
# MAGIC   
# MAGIC   FROM "MATRIC_ATEND" m
# MAGIC   INNER JOIN "ATENDIMENTO" a
# MAGIC   ON m.cd_atendimento = a.cd_atendimento
# MAGIC   WHERE m.cd_atendimento_encontrado IS NULL
# MAGIC   
# MAGIC </pre>

# COMMAND ----------

df_repescagem = df_matric_atend. \
filter(col("cd_atendimento_encontrado").isNull()). \
drop("dt_termino", "cd_atendimento_encontrado", "cd_tipo_gratuidade", "fl_excluido", "dt_inicio"). \
join(df_atendimento.drop("dt_atualizacao_ini", "dt_atualizacao_fim"),
    (df_matric_atend.cd_atendimento == df_atendimento.cd_atendimento_encontrado),
    "inner"). \
coalesce(4). \
cache()

df_repescagem.count()

# COMMAND ----------

df_atendimento = df_atendimento.unpersist()

# COMMAND ----------

new_data = df_matric_atend. \
filter(col("cd_atendimento_encontrado").isNotNull()). \
union(df_repescagem.select(df_matric_atend.columns)). \
drop("cd_atendimento_encontrado"). \
coalesce(8). \
cache()

new_data.count()

# COMMAND ----------

df_matric_atend = df_matric_atend.unpersist()
df_repescagem = df_repescagem.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cd_tipo_situacao_atendimento and dt_termino for each cd_atendimento compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns = ["cd_atendimento", "cd_tipo_situacao_matricula", "dt_termino", "dt_congelamento_matricula", "qt_horas_pratica_profis", "cd_tipo_gratuidade", "fl_excluido", "dt_inicio"]
var_key = ["cd_atendimento"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

new_data = new_data.withColumn("row_hash", concat_ws("@", *hash_columns))\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_atualizacao")))).dropDuplicates()\
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
# MAGIC * dh_inicio_vigencia = dt_atualizacao
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

w = Window.partitionBy('cd_atendimento')
new_data = new_data.select('*', count('cd_atendimento').over(w).alias('count_cd'))

# COMMAND ----------

new_data = new_data\
.withColumn("dt_lead", lead("dt_atualizacao").over(w.orderBy("dt_atualizacao")))\
.withColumnRenamed("dt_atualizacao", "dh_inicio_vigencia")\
.withColumn("dh_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(col("dt_lead")))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dh_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we just need some make-up to get <i>new_data</i> ready to go and finally get to versioning logic.
# MAGIC As some issues were trated before, only a few columns need to be renamed.

# COMMAND ----------

column_name_list = ["fl_excluido"]

for item in column_name_list:
  new_data = new_data.withColumn(item, when(trim(lower(col(item))) == "n", 0).when(trim(lower(col(item))) == "s", 1).otherwise(lit(None).cast("int")))

# COMMAND ----------

column_name_mapping = {"cd_atendimento": "cd_atendimento_matricula_ensino_prof", 
                       "dt_termino": "dt_saida",
                       "fl_excluido": "fl_excluido_oltp",
                       "dt_inicio": "dt_entrada"
                      }
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_atendimento_matricula_ensino_prof": "int", 
                   "cd_tipo_situacao_matricula": "int", 
                   "dt_congelamento_matricula": "date", 
                   "dt_saida": "date", 
                   "fl_excluido_oltp": "int",
                   "qt_horas_pratica_profis": "int",
                   "cd_tipo_gratuidade": "int",
                   "dh_inicio_vigencia": "timestamp",
                   "dh_fim_vigencia": "timestamp",
                   "fl_corrente": "int",
                   "dt_entrada": "date"}

for c in column_type_map:
  new_data = new_data.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

#new_data.count(), new_data.select(concat_ws("@", *["cd_atendimento_matricula_ensino_prof", "cd_tipo_situacao_matricula", "dt_saida", "dt_congelamento_matricula", "qt_horas_pratica_profis", "cd_tipo_gratuidade", "fl_excluido_oltp", "dh_inicio_vigencia"])).dropDuplicates().count(), new_data.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  # Partitioning as specified by the documentation. repartition(5) is a good parameter since we avoid to write many small files in ADLS
  new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")
  new_data = new_data.unpersist()
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC Versioning - This is the else section: *if first_load is False:*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Magic words:
# MAGIC 
# MAGIC <pre>
# MAGIC /* Versionar se e somente se houve alteração em cd_tipo_situacao_matricula ou dt_congelamento_matricula */
# MAGIC Para cada dt_atualizacao encontrada
# MAGIC     Verifica se existe na trs matricula_ensino_prof_situacao: 
# MAGIC 	    raw.cd_atendimento = trs.cd_atendimento_matricula_ensino_prof 
# MAGIC      	e trs.fl_corrente = 1
# MAGIC         Se existe, 
# MAGIC             Se houve alteração: 
# MAGIC  	    raw.cd_tipo_situacao_matricula <> trs.cd_tipo_situacao_matricula 
# MAGIC 	    ou raw.dt_congelamento_matricula  <> trs.dt_congelamento_matricula  
# MAGIC             ou raw.dt_termino                 <> trs.dt_saida  -- renomeado em 03/09/2020 trs.dt_termino_matricula para trs.dt_saida
# MAGIC 		      	
# MAGIC 	    --campos incluídos em 03/09/2020
# MAGIC             ou raw.qt_horas_pratica_profis    <> trs.qt_horas_pratica_profis
# MAGIC             ou raw.cd_tipo_gratuidade         <> trs.cd_tipo_gratuidade
# MAGIC             ou raw.fl_excluido 		      <> trs.fl_excluido_oltp
# MAGIC             ou raw.dt_inicio                  <> trs.dt_entrada  
# MAGIC 			
# MAGIC                 UPDATE trs matricula_ensino_prof_situacao (existente)
# MAGIC                     set trs.dh_fim_vigencia = raw.dt_atualizacao, trs.fl_corrente = 0
# MAGIC 					
# MAGIC                INSERT trs matricula_ensino_prof_situacao (nova)	
# MAGIC                     trs.cd_atendimento_matricula_ensino_prof = raw.cd_atendimento
# MAGIC                     trs.cd_tipo_situacao_matricula           = raw.cd_tipo_situacao_matricula
# MAGIC                     trs.dt_congelamento_matricula            = raw.dt_congelamento_matricula
# MAGIC                     trs.dt_saida                             = raw.dt_termino -- renomeado em 03/09/2020 trs.dt_termino_matricula para trs.dt_saida
# MAGIC                     trs.dh_inicio_vigencia                   = raw.dt_atualizacao
# MAGIC                     trs.dh_fim_vigencia                      = NULL
# MAGIC                     trs.fl_corrente                          = 1
# MAGIC 
# MAGIC 					--campos incluídos em 03/09/2020
# MAGIC                     trs.qt_horas_pratica_profis              = raw.qt_horas_pratica_profis
# MAGIC                     trs.cd_tipo_gratuidade                   = raw.cd_tipo_gratuidade
# MAGIC                     trs.fl_excluido_oltp                     = raw.fl_excluido
# MAGIC                     trs.dt_entrada                           = raw.dt_inicio
# MAGIC 					
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs matricula_ensino_prof_situacao (nova)	
# MAGIC </pre>

# COMMAND ----------

var_key = list(["cd_atendimento_matricula_ensino_prof"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = trusted_data.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare = old_data_active. \
join(new_data.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active. \
join(new_data.select(var_key).dropDuplicates(),var_key, "leftanti") 

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dh_fim_vigencia"

new_data = new_data.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

new_data = new_data.union(old_data_to_compare.select(new_data.columns))

# COMMAND ----------

hash_columns = var_key + ["cd_tipo_situacao_matricula", "dt_congelamento_matricula", "dt_saida", "qt_horas_pratica_profis", "cd_tipo_gratuidade", "fl_excluido_oltp", "dt_entrada"]

new_data = new_data.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_inicio_vigencia")))).dropDuplicates()

# COMMAND ----------

#display(new_data)

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

new_data = new_data\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

new_data = new_data.withColumn("lead_dh_inicio_vigencia", lead("dh_inicio_vigencia", 1).over(var_window.orderBy(asc("dh_inicio_vigencia"))))

# COMMAND ----------

# When lead_dh_inicio_vigencia is not NULL, then we must close the period with "lead_dh_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

new_data = new_data. \
withColumn("dh_fim_vigencia", when(col("lead_dh_inicio_vigencia").isNotNull(), col("lead_dh_inicio_vigencia"))). \
withColumn("fl_corrente", when(col("lead_dh_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dh_inicio_vigencia")

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"

old_data_inactive_to_rewrite = trusted_data. \
filter(col("fl_corrente") == 0). \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite. \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns))

# COMMAND ----------

#new_data.select("cd_atendimento_matricula_ensino_prof", "dh_inicio_vigencia").groupBy("cd_atendimento_matricula_ensino_prof", "dh_inicio_vigencia").count().filter(col("count")%2 != 1).show(20,False) 

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

new_data = new_data.unpersist()

# COMMAND ----------

