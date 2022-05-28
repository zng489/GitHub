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
# MAGIC ```
# MAGIC Processo	raw_trs_estabelecimento_caracteristica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/smd/estabelecimento
# MAGIC Tabela/Arquivo Destino	/trs/evt/smd/estabelecimento_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	Não há
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre as características dos estabelecimentos O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e <= que data de fim e a posição atual com versão corrente informada = 1 OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), estabelecimento
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou versiona o registro caso a chave id_estabelecimento seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw inddesempenho, que ocorre às xx:00
# MAGIC 
# MAGIC Dev Tiago Shin
# MAGIC ```

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
var_tables =  {"origins": ["/bdo/inddesempenho/estabelecimento"], "destination": "/mtd/sesi/estabelecimento_atendido_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_estabelecimento_atendido_caracteristica',
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

src_es = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_es)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

from pyspark.sql.functions import substring, trim, lit, asc, desc, row_number, col, when, concat, length, upper, lower, dense_rank, count, from_utc_timestamp, current_timestamp, max, date_sub, year, isnull, lag, date_add, min, lead, array, to_date, concat_ws, row_number
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
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from estabelecimento_atendido_caracteristica */
# MAGIC /* Ler estabelecimento da raw com as regras para carga incremental, execucao diária, query abaixo: */
# MAGIC select
# MAGIC id,codcnae,porteempresa,indicaindustriacnae,indicaindustriafpas,indicaoptantesimples,indicaorigem,
# MAGIC quantidadeempregados,dataatualizacao FROM estabelecimento
# MAGIC WHERE dataatualizacao >= #var_max_dh_ultima_atualizacao_oltp ORDER BY a.dataatualizacao
# MAGIC /* Como resultado, pode existir mais de um registro de atualização no período para o mesmo id_estabelecimento */
# MAGIC /* Versionar se e somente se houve alteração em alguma informacao */
# MAGIC Para cada dt_atualizacao encontrada
# MAGIC     Verifica se existe na trs estabelecimento_atendido_caracteristica o raw.id = trs.id_estabelecimento
# MAGIC         Se existe, 
# MAGIC             Se houve alteração em algum dos campos envolvidos no select
# MAGIC                UPDATE trs estabelecimento_atendido_caracteristica (existente)
# MAGIC                     set trs.dt_fim_vigencia = (raw.dt_atualizacao - 1 dia), trs.fl_corrente = 0
# MAGIC                INSERT trs estabelecimento_atendido_caracteristica (nova)
# MAGIC                     trs.cd_estabelecimento_atendido_oltp  = raw.id,			
# MAGIC                     trs.cd_cnae 		= raw.codcnae,			
# MAGIC                     trs.ds_cnae 		= raw.descricaocnae,		
# MAGIC                     trs.cd_indicador_industria_cnae = raw.indicaindustriacnae,
# MAGIC                     trs.cd_indicador_industria_fpas = raw.indicaindustriafpas,
# MAGIC                     trs.cd_indicador_optante_simples = raw.indicaoptantesimples
# MAGIC                     trs.cd_indica_origem 	= raw.indicaorigem,	
# MAGIC                     trs.qt_empregados 	= raw.quantidadeempregados,	
# MAGIC                     trs.dt_inicio_vigencia 	= raw.dataatualizacao,		
# MAGIC                     trs.dt_fim_vigencia = NULL,
# MAGIC                     trs.fl_corrente = 1
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs trs estabelecimento_atendido_caracteristica (nova)
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting max date of dt_inicio_vigencia from last load

# COMMAND ----------

first_load = False
var_max_dt_inicio_vigencia = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  df_trs = spark.read.parquet(sink)
  var_max_dt_inicio_vigencia = df_trs.select(max(col("dt_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)
print("Max dt inicio vigencia", var_max_dt_inicio_vigencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading raw and filtering

# COMMAND ----------

var_useful_columns = ["ID", "CODCNAE", "PORTEEMPRESA", "INDICAINDUSTRIACNAE", "INDICAINDUSTRIAFPAS", "INDICAOPTANTESIMPLES", "INDICAORIGEM", "QUANTIDADEEMPREGADOS", "DATAATUALIZACAO"]

# COMMAND ----------

df_raw = spark.read.parquet(src_es)\
.select(*var_useful_columns)\
.distinct()\
.filter((col("DATAATUALIZACAO") >= var_max_dt_inicio_vigencia) & (col("ID").isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC Change column names 

# COMMAND ----------

var_column_name_mapping = {"ID" : "cd_estabelecimento_atendido_oltp",                            
                           "PORTEEMPRESA" : "cd_porte_empresa",
                           "CODCNAE" : "cd_cnae",
                           "INDICAINDUSTRIACNAE": "cd_indicador_industria_cnae",
                           "INDICAINDUSTRIAFPAS" : "cd_indicador_industria_fpas", 
                           "INDICAOPTANTESIMPLES" : "cd_indicador_optante_simples",
                           "INDICAORIGEM" : "cd_indica_origem",
                           "QUANTIDADEEMPREGADOS" : "qt_empregado",
                           "DATAATUALIZACAO" : "dataatualizacao"}
  
for key in var_column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, var_column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC Trim columns

# COMMAND ----------

var_columns_to_trim = ["cd_cnae", "cd_indica_origem"]

for key in var_columns_to_trim:
  df_raw = df_raw.withColumn(key, trim(col(key)))

# COMMAND ----------

# MAGIC %md
# MAGIC Conform types

# COMMAND ----------

var_column_type_map = {"cd_estabelecimento_atendido_oltp" : "string",                       
                       "cd_porte_empresa" : "int",
                       "cd_cnae" : "int",
                       "cd_indicador_industria_cnae": "string",
                       "cd_indicador_industria_fpas" : "string",
                       "cd_indicador_optante_simples" : "string",
                       "cd_indica_origem" : "int",
                       "qt_empregado" : "int",
                       "dataatualizacao": "date"}

for c in var_column_type_map:
  df_raw = df_raw.withColumn(c, col(c).cast(var_column_type_map[c]))

# COMMAND ----------

new_data = df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cd_cnae, cd_porte_empresa, cd_indicador_industria_cnae, cd_indicador_industria_fpas, cd_indicador_optante_simples, cd_indica_origem and qt_empregado for each cd_estabelecimento_atendido_oltp compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns = ["cd_estabelecimento_atendido_oltp", "cd_cnae", "cd_porte_empresa", "cd_indicador_industria_cnae", "cd_indicador_industria_fpas", "cd_indicador_optante_simples", "cd_indica_origem", "qt_empregado"]
var_key = ["cd_estabelecimento_atendido_oltp"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

new_data = new_data\
.withColumn("row_hash", concat_ws("@", *hash_columns))\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dataatualizacao")))).dropDuplicates()\
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
# MAGIC * dh_inicio_vigencia = dataatualizacao
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

w = Window.partitionBy('cd_estabelecimento_atendido_oltp')
new_data = new_data\
.select('*', count('cd_estabelecimento_atendido_oltp').over(w).alias('count_cd'))

# COMMAND ----------

new_data = new_data\
.withColumn("dt_lead", lead("dataatualizacao").over(w.orderBy("dataatualizacao")))\
.withColumnRenamed("dataatualizacao", "dt_inicio_vigencia")\
.withColumn("dt_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(date_sub(col("dt_lead"),1)))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dt_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"dt_inicio_vigencia": "date",
                   "dt_fim_vigencia": "date",
                   "fl_corrente": "int"}

for c in column_type_map:
  new_data = new_data.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  new_data.coalesce(6).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")  
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning 
# MAGIC 
# MAGIC <pre>
# MAGIC Para cada dt_atualizacao encontrada
# MAGIC     Verifica se existe na trs estabelecimento_atendido_caracteristica o raw.id = trs.id_estabelecimento
# MAGIC         Se existe, 
# MAGIC             Se houve alteração em algum dos campos envolvidos no select
# MAGIC                UPDATE trs estabelecimento_atendido_caracteristica (existente)
# MAGIC                     set trs.dt_fim_vigencia = (raw.dt_atualizacao - 1 dia), trs.fl_corrente = 0
# MAGIC                INSERT trs estabelecimento_atendido_caracteristica (nova)
# MAGIC                     trs.cd_estabelecimento_atendido_oltp  = raw.id,			
# MAGIC                     trs.cd_cnae 		= raw.codcnae,			
# MAGIC                     trs.ds_cnae 		= raw.descricaocnae,		
# MAGIC                     trs.cd_indicador_industria_cnae = raw.indicaindustriacnae,
# MAGIC                     trs.cd_indicador_industria_fpas = raw.indicaindustriafpas,
# MAGIC                     trs.cd_indicador_optante_simples = raw.indicaoptantesimples
# MAGIC                     trs.cd_indica_origem 	= raw.indicaorigem,	
# MAGIC                     trs.qt_empregados 	= raw.quantidadeempregados,	
# MAGIC                     trs.dt_inicio_vigencia 	= raw.dataatualizacao,		
# MAGIC                     trs.dt_fim_vigencia = NULL,
# MAGIC                     trs.fl_corrente = 1
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs trs estabelecimento_atendido_caracteristica (nova)

# COMMAND ----------

var_key = list(["cd_estabelecimento_atendido_oltp"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = df_trs\
.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare = old_data_active. \
join(new_data.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active. \
join(new_data\
     .select(var_key)\
     .dropDuplicates(),
     var_key, "leftanti") 

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dh_fim_vigencia"

new_data = new_data\
.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

new_data = new_data\
.union(old_data_to_compare\
       .select(new_data.columns))

# COMMAND ----------

hash_columns = var_key + ["cd_cnae", "cd_porte_empresa", "cd_indicador_industria_cnae", "cd_indicador_industria_fpas", "cd_indicador_optante_simples", "cd_indica_origem", "qt_empregado"]

new_data = new_data\
.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_inicio_vigencia"), asc("dh_insercao_trs")))).dropDuplicates()

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

new_data = new_data\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

new_data = new_data\
.withColumn("lead_dt_inicio_vigencia", lead("dt_inicio_vigencia", 1).over(var_window.orderBy(asc("dt_inicio_vigencia"))))

# COMMAND ----------

# When lead_dh_inicio_vigencia is not NULL, then we must close the period with "lead_dh_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

new_data = new_data. \
withColumn("dt_fim_vigencia", when(col("lead_dt_inicio_vigencia").isNotNull(), date_sub(col("lead_dt_inicio_vigencia"),1))). \
withColumn("fl_corrente", when(col("lead_dt_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dt_inicio_vigencia")

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"

old_data_inactive_to_rewrite = df_trs. \
filter(col("fl_corrente") == 0). \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite. \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite.select(old_data_inactive_to_rewrite.columns))

# COMMAND ----------

new_data = new_data\
.union(old_data_to_rewrite.select(new_data.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink

# COMMAND ----------

df_trs.coalesce(1).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

