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
# MAGIC Processo	raw_trs_empresa_atendida_caracteristica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/scae/matricula
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/empresa_atendida_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	Não há
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre as características das empresas atendidas vinculadas em ações educativas do SESI. O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e <= que data de fim e a posição atual com versão corrente informada = 1 OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), empresa_atendida
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou versiona o registro caso a chave cd_cnpj seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC Dev Tiago Shin
# MAGIC Manutenção:
# MAGIC   2020/07/20 - adição de cache() condicional para cargas incrementais, evitando o broadcast join
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "100")
sqlContext.setConf("spark.default.parallelism", "100")

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
var_tables =  {"origins": ["/bdo/scae/vw_matricula"], "destination": "/mtd/sesi/empresa_atendida_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_empresa_atendida_caracteristica',
       'adf_pipeline_run_id': 'development',
       'adf_trigger_id': 'development',
       'adf_trigger_name': 'development',
       'adf_trigger_time': '2020-07-20T10:00:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

var_src_mt = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(var_src_mt)

# COMMAND ----------

var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(var_sink)

# COMMAND ----------

from pyspark.sql.functions import substring, trim, lit, asc, desc, row_number, col, when, concat, length, upper, lower, dense_rank, count, from_utc_timestamp, current_timestamp, max, date_sub, year, isnull, lag, date_add, min, lead, array, to_date, concat_ws, row_number, coalesce
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
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from empresa_atendida_caracteristica */
# MAGIC /* Ler matricula da raw com as regras para carga incremental, execucao diária, query abaixo: */
# MAGIC SELECT DISTINCT
# MAGIC num_cnpj_vinculo, cod_cnae, porte_empresa, ind_optante_simples, ind_contribuinte_senai_ca, ind_contribuinte_senai_tc, ind_contribuinte_senai_ai, ind_contribuinte_sesi_ad, ind_contribuinte_sesi_ai, ind_industria_cnae, ind_industria_fpas, ind_situacao_industria, ind_comunidade_industria, dat_registro 
# MAGIC FROM matricula
# MAGIC WHERE dat_registro > #var_max_dh_ultima_atualizacao_oltp ORDER BY dat_registro
# MAGIC /* Como resultado, pode existir mais de um registro de atualização no período para o mesmo num_cnpj_vinculo */
# MAGIC /* Versionar se e somente se houve alteração em alguma informacao */
# MAGIC Para cada dat_registro encontrada
# MAGIC     Verifica se existe na trs empresa_atendida_caracteristica para raw.num_cnpj_vinculo = trs.cd_cnpj
# MAGIC         Se existe, 
# MAGIC             Se houve alteração em algum dos campos envolvidos no select
# MAGIC                UPDATE trs empresa_atendida_caracteristica (existente)
# MAGIC                     set trs.dt_fim_vigencia = (raw.dat_registro - 1 dia), trs.fl_corrente = 0
# MAGIC                INSERT trs empresa_atendida_caracteristica (nova)
# MAGIC                     trs.cd_cnpj = raw.num_cnpj_vinculo
# MAGIC 					trs.cd_versao_cnae = NULL
# MAGIC 					trs.cd_subclasse_cnae = raw.cod_cnae
# MAGIC                     trs.cd_porte = raw.porte_empresa
# MAGIC 					trs.cd_indicador_optante_simples = raw.ind_optante_simples
# MAGIC                     trs.cd_indicador_contribuinte_senai_ca = raw.ind_contribuinte_senai_ca   
# MAGIC 					trs.cd_indicador_contribuinte_senai_tc = raw.ind_contribuinte_senai_tc
# MAGIC                     trs.cd_indicador_contribuinte_senai_ai = raw.ind_contribuinte_senai_ai
# MAGIC                     trs.cd_indicador_contribuinte_sesi_ad = raw.ind_contribuinte_sesi_ad
# MAGIC                     trs.cd_indicador_contribuinte_sesi_ai = raw.ind_contribuinte_sesi_ai
# MAGIC 					trs.fl_industria_cnae = raw.ind_industria_cnae
# MAGIC                     trs.cd_indicador_industria_fpas = raw.ind_industria_fpas
# MAGIC                     trs.fl_situacao_industria = raw.ind_situacao_industria
# MAGIC                     trs.fl_comunidade_industria = raw.ind_comunidade_industria
# MAGIC 					trs.qt_empregado = NULL
# MAGIC 					trs.dt_inicio_vigencia = raw.dat_registro
# MAGIC                     trs.dt_fim_vigencia = NULL
# MAGIC                     trs.fl_corrente = 1
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs empresa_atendida_caracteristica (nova) 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting max date of dt_inicio_vigencia from last load

# COMMAND ----------

var_first_load = False
var_max_dt_inicio_vigencia = datetime.date(1800, 1, 1)

try:
  df_trs = spark.read.parquet(var_sink).coalesce(8)
  var_max_dt_inicio_vigencia = df_trs.select(max(col("dt_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  var_first_load = True

# COMMAND ----------

print("Is first load?", var_first_load)
print("Max dt inicio vigencia", var_max_dt_inicio_vigencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading raw and filtering

# COMMAND ----------

"""
First load object is about 6.3M records. Don't keep it in cache!

For incremental loads, as the resulting object is small (<500 records), it's really worth caching cause will save a lot of time during next steps.
Remember that cache won't really happen here! CACHE here is lazy!

Coalesce here will only apply to cached object! So we can keep it in 1 partition in incremental loads as the object is too light!
"""

var_coalesce = 32 if var_first_load is True else 1

var_mt_columns = ["NUM_CNPJ_VINCULO", "COD_CNAE", "PORTE_EMPRESA", "IND_OPTANTE_SIMPLES", "IND_CONTRIBUINTE_SENAI_CA", "IND_CONTRIBUINTE_SENAI_TC", "IND_CONTRIBUINTE_SENAI_AI", "IND_CONTRIBUINTE_SESI_AD", "IND_CONTRIBUINTE_SESI_AI", "IND_INDUSTRIA_CNAE", "IND_INDUSTRIA_FPAS", "IND_SITUACAO_INDUSTRIA", "IND_COMUNIDADE_INDUSTRIA", "DAT_REGISTRO"]

df_raw = spark.read.parquet(var_src_mt)\
.filter((col("DAT_REGISTRO") > var_max_dt_inicio_vigencia) &\
        (col("NUM_CNPJ_VINCULO").isNotNull()))\
.select(*var_mt_columns)\
.distinct()\
.sort(desc("DAT_REGISTRO"))\
.coalesce(var_coalesce)

if var_first_load is False:
  print("Cache df_raw: YES")
  df_raw = df_raw.cache()
else:
  print("Cache df_raw: NO")

# COMMAND ----------

"""
If there's no new data, then just let it die.
cache() happens HERE since here we've got a true action! But if things are useless, let it die!

In source we've got ~ 21200 files. That's why it is absolutely slow to read! With or without coalesce, there's no much magic available for data being spread in too many small files as this.

Cached object is ~ 51kB in memory =D
"""
if var_first_load is False:
  if df_raw.count() == 0:
    df_raw.unpersist()
    dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Drop if there are more than 1 dt_atualizacao by day and get the last one. If the records have changes in the same "dat_registro" timestamp, it will choose one arbitrarily because there's nothing else we can do in these cases.
# MAGIC Note: This operation need to be performed before transforming DAT_REGISTRO to date, otherwise we would keep with an enormous amount of records that aren't useful
# MAGIC </pre>

# COMMAND ----------

df_raw = df_raw\
.withColumn("row_number", row_number().over(Window.partitionBy("NUM_CNPJ_VINCULO", to_date(col("DAT_REGISTRO"), 'yyyy-MM-dd')).orderBy(col("DAT_REGISTRO").desc())))\
.filter(col("row_number") == 1)\
.drop("row_number")

# COMMAND ----------

#display(df_raw.groupBy("NUM_CNPJ_VINCULO", "DAT_REGISTRO").count().filter(col("count") > 1))

# COMMAND ----------

#df_raw.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changing column names and types

# COMMAND ----------

column_name_mapping = {"NUM_CNPJ_VINCULO" : "cd_cnpj", \
                       "COD_CNAE" : "cd_subclasse_cnae", \
                       "PORTE_EMPRESA" : "cd_porte", \
                       "IND_OPTANTE_SIMPLES" : "cd_indicador_optante_simples", \
                       "IND_CONTRIBUINTE_SENAI_CA" : "cd_indicador_contribuinte_senai_ca", \
                       "IND_CONTRIBUINTE_SENAI_TC": "cd_indicador_contribuinte_senai_tc",
                       "IND_CONTRIBUINTE_SENAI_AI" : "cd_indicador_contribuinte_senai_ai", \
                       "IND_CONTRIBUINTE_SESI_AD" : "cd_indicador_contribuinte_sesi_ad", \
                       "IND_CONTRIBUINTE_SESI_AI" : "cd_indicador_contribuinte_sesi_ai", \
                       "IND_INDUSTRIA_CNAE" : "fl_industria_cnae", \
                       "IND_INDUSTRIA_FPAS" : "cd_indicador_industria_fpas", \
                       "IND_SITUACAO_INDUSTRIA": "fl_situacao_industria", \
                       "IND_COMUNIDADE_INDUSTRIA" : "fl_comunidade_industria", \
                       "DAT_REGISTRO" : "dat_registro"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC Transform flag fl_industria_cnae from S/N to 1/0

# COMMAND ----------

df_raw = df_raw\
.withColumn('fl_industria_cnae', when(col("fl_industria_cnae") == "S", lit(1)).otherwise(when(col("fl_industria_cnae") == "N", lit(0))))

# COMMAND ----------

column_type_map = {"cd_cnpj" : "string", \
                   "cd_subclasse_cnae" : "int", \
                   "cd_porte" : "int", \
                   "cd_indicador_optante_simples" : "string", \
                   "cd_indicador_contribuinte_senai_ca" : "string", \
                   "cd_indicador_contribuinte_senai_tc": "string",
                   "cd_indicador_contribuinte_senai_ai" : "string", \
                   "cd_indicador_contribuinte_sesi_ad" : "string", \
                   "cd_indicador_contribuinte_sesi_ai" : "string", \
                   "fl_industria_cnae" : "int", \
                   "cd_indicador_industria_fpas" : "string", \
                   "fl_situacao_industria": "int", \
                   "fl_comunidade_industria" : "int", \
                   "dat_registro" : "date"}

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
# MAGIC Drop records that didn't change any columns except for dat_registro compared to the last record
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
# MAGIC It's true that using dense rank we'll avoid the random nature of the operation, but we still would keep only the last "unique_id" for the same "cd_cnpj". If the same "unique_id" happens more than 1 time and represents a change, it shouldn't be ignored.
# MAGIC So using lag enable us to compare the "unique_id" with the last one and keep only the records that had some change compared to the last "dat_registro"
# MAGIC </pre>

# COMMAND ----------

var_id_columns = ["cd_cnpj", "cd_subclasse_cnae", "cd_porte", "cd_indicador_optante_simples", "cd_indicador_contribuinte_senai_ca", "cd_indicador_contribuinte_senai_tc", "cd_indicador_contribuinte_senai_ai","cd_indicador_contribuinte_sesi_ad","cd_indicador_contribuinte_sesi_ai", "fl_industria_cnae","cd_indicador_industria_fpas","fl_situacao_industria","fl_comunidade_industria"]

# COMMAND ----------

def custom_concat(*cols):
    return concat(*[concat(coalesce(c, lit("|")), lit("@")) for c in cols])

# COMMAND ----------

"""
Apply all things until here and refresh cache. This will keep things smooth
"""

df_raw = df_raw.withColumn("unique_id", custom_concat(*var_id_columns))\
.withColumn("changed", (col("unique_id") != lag('unique_id', 1, 0).over( Window.partitionBy("cd_cnpj").orderBy(col("dat_registro").asc()))).cast("int"))\
.filter(col("changed") == 1)\
.drop("unique_id", "changed")

# COMMAND ----------

"""
Just another NEEDED action to activate the cache, and this will be really fast. A new object is created, but it is 7kB in memory.
Also, even if you drop the old object, spark won't really do it. Cause he needs the history of the object
"""
if var_first_load is False:
  df_raw = df_raw.cache()
  df_raw.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert records (active) & Update records with many ids

# COMMAND ----------

# MAGIC %md
# MAGIC Here we don't have to worry about dt_fim_vigencia being lower than dt_inicio_vigencia because we already ensured before that we'll have only 1 new record by day. But if it's not the case, then have attention with this section!

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b>FROM DOCUMENTATION</b>:
# MAGIC                INSERT trs empresa_atendida_caracteristica (nova)
# MAGIC                     trs.cd_cnpj = raw.num_cnpj_vinculo
# MAGIC 					trs.cd_versao_cnae = NULL
# MAGIC 					trs.cd_subclasse_cnae = raw.cod_cnae
# MAGIC                     trs.cd_porte = raw.porte_empresa
# MAGIC 					trs.cd_indicador_optante_simples = raw.ind_optante_simples
# MAGIC                     trs.cd_indicador_contribuinte_senai_ca = raw.ind_contribuinte_senai_ca   
# MAGIC 					trs.cd_indicador_contribuinte_senai_tc = raw.ind_contribuinte_senai_tc
# MAGIC                     trs.cd_indicador_contribuinte_senai_ai = raw.ind_contribuinte_senai_ai
# MAGIC                     trs.cd_indicador_contribuinte_sesi_ad = raw.ind_contribuinte_sesi_ad
# MAGIC                     trs.cd_indicador_contribuinte_sesi_ai = raw.ind_contribuinte_sesi_ai
# MAGIC 					trs.fl_industria_cnae = raw.ind_industria_cnae
# MAGIC                     trs.cd_indicador_industria_fpas = raw.ind_industria_fpas
# MAGIC                     trs.fl_situacao_industria = raw.ind_situacao_industria
# MAGIC                     trs.fl_comunidade_industria = raw.ind_comunidade_industria
# MAGIC 					trs.qt_empregado = NULL
# MAGIC 					trs.dt_inicio_vigencia = raw.dat_registro
# MAGIC                     trs.dt_fim_vigencia = NULL
# MAGIC                     trs.fl_corrente = 1
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC           Senão, NÃO existe: INSERT trs empresa_atendida_caracteristica (nova)
# MAGIC </pre>

# COMMAND ----------

w = Window.partitionBy('cd_cnpj')
df_raw = df_raw.select('*', count('cd_cnpj').over(w).alias('count_cd'))

# COMMAND ----------

"""
if not the first load, as it reads from a cached object, this is really fast! No need to cache it here too!
"""

new_records = df_raw\
.withColumn("dt_lead", lead("dat_registro").over(w.orderBy("dat_registro")))\
.withColumn("cd_versao_cnae", lit(None).cast("int"))\
.withColumn("qt_empregado", lit(None).cast("int"))\
.withColumn("dt_inicio_vigencia", col("dat_registro"))\
.withColumn("dt_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("date")).otherwise(date_sub(col("dt_lead"),1)))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
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

if var_first_load is True:
  del df_raw
  new_records = new_records.drop("dat_registro")
  # Partitioning as specified by the documentation. coalesce(5) is a good parameter since we avoid to write many small files in ADLS
  new_records.coalesce(5).write.partitionBy("fl_corrente").save(path=var_sink, format="parquet", mode="overwrite")
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b> FROM DOCUMENTATION </b>:
# MAGIC Verifica se existe na trs empresa_atendida_caracteristica para raw.num_cnpj_vinculo = trs.cd_cnpj
# MAGIC         Se existe, 
# MAGIC             Se houve alteração em algum dos campos envolvidos no select
# MAGIC </pre>

# COMMAND ----------

#Add unique_id to raw and trusted that performs a contenation between the columns stated on id_columns and filter trusted by the active flag
new_records = new_records.withColumn("unique_id", custom_concat(*var_id_columns))
df_trs_active = df_trs.filter(col("fl_corrente") == 1).withColumn("unique_id", custom_concat(*var_id_columns))

# COMMAND ----------

"""
Performing an "leftanti join" by unique_id means selecting only the records that are present in raw and not present in trs, that is, the new records or the ones the have some change between the columns in id_columns
"""
new_records = new_records.join(df_trs_active, ["unique_id"], how="leftanti").drop("unique_id").cache()

# Activate new_records's cache
new_records.count()

# COMMAND ----------

#Just drop "unique_id" as it won't be used anymore
df_trs_active = df_trs_active.drop("unique_id")

# COMMAND ----------

#Rename all columns of trusted adding "trs" prefix, except for the column cd_cnpj
columns_trs_mapping = {}

for key in df_trs_active.columns:
  columns_trs_mapping["{}".format(key)] = "trs_" + key
del columns_trs_mapping["cd_cnpj"]

for key in columns_trs_mapping:
  df_trs_active =  df_trs_active.withColumnRenamed(key, columns_trs_mapping[key])

# COMMAND ----------

#To get the changed records with both data from trusted and the new data, perform an "inner join" between df_raw and df_trs_old_active by the cd_cnpj column 
changed_records = new_records.join(df_trs_active, ['cd_cnpj'], 'inner')

# COMMAND ----------

#In trusted, we won't keep the column "dat_registro". Drop it here because we needed it to define "changed_records"
new_records = new_records.drop("dat_registro")

# COMMAND ----------

#new_records.count(), changed_records.count(), df_trs_active.count() #(67, 63, 102815)

# COMMAND ----------

"""
If there's no new data and changed data, then just let it die.
This section here surely just applies to incremental loads.
Will process really fast since all this data is in cache!
""" 
if new_records.count() == 0 and changed_records.count() == 0:
  new_records.unpersist()
  df_raw.unpersist()
  dbutils.notebook.exit('{"new_and_changed_data_is_zero": 1}')

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
records_to_update = changed_records.select(df_trs_active.columns).distinct()
del df_trs_active

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
# MAGIC Note: we need to discard the records in which "dt_fim_vigencia" is earlier than "dt_inicio_vigencia" not only because it does not make sense, but because otherwise we would have 2 records with the same "cd_cnpj" and "dt_inicio_vigencia". So in these cases, we can safely ignore (drop) the records because in "new_records" we'll get one with more refreshed information
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <b>FROM DOCUMENTATION</b>:
# MAGIC UPDATE trs matricula_ensino_prof_situacao (existente)
# MAGIC   set trs.dt_fim_vigencia = (raw.dt_atualizacao - 1 dia), trs.fl_corrente = 0
# MAGIC </pre>

# COMMAND ----------

#Now, it's time to get the correponding dt_atualizacao to close the validity of a previous open vigency. This value must be the oldest date that changed.
get_min_dt_atualizacao = changed_records.groupBy("cd_cnpj", col("dh_insercao_trs").alias("new_dh_insercao_trs"))\
.agg(min(col("dat_registro")).alias("dat_registro"))

# COMMAND ----------

updated_records = records_to_update\
.join(get_min_dt_atualizacao,["cd_cnpj"], how="left")\
.withColumn("dt_fim_vigencia", date_sub(col("dat_registro"), 1))\
.filter(col("dt_fim_vigencia") >= col("dt_inicio_vigencia"))\
.withColumn("fl_corrente", lit(0).cast("int"))\
.drop("dat_registro", "dh_insercao_trs", "cd_ano_inicio_vigencia")\
.withColumnRenamed("new_dh_insercao_trs","dh_insercao_trs")

# COMMAND ----------

del get_min_dt_atualizacao
del records_to_update
del changed_records

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
df_trs = df_trs.select(new_records.columns).union(updated_records.select(new_records.columns).union(new_records))

# COMMAND ----------

#Deleting intermediary objects
del updated_records

# COMMAND ----------

#df_trs.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink

# COMMAND ----------

df_trs.coalesce(6).write.partitionBy("fl_corrente").save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

new_records.unpersist()
df_raw.unpersist()
del new_records