# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_unidade_atendimento_caracteristica
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/bd_basi/tb_unidade_atendimento
# MAGIC /raw/bdo/oba/tb_industria_conhecimento"
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/unidade_atendimento_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 4.870 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	"Informações de caracteristicas de natureza, integrações, flags das Unidades de Atendimento do Sistema Indústria
# MAGIC Ex: As escolas do SESI e do SENAI, os Instituos do SENAI, as unidades móveis, bem como as unidades de indústria do conhecimento, enfim, todas as Unidades que efetuam atendimentos, e que estão vinculadas a um DR"
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou versiona o registro caso a chave cd_unidade_atendimento_dr seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
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
var_tables =  {"origins": ["/bdo/bd_basi/tb_unidade_atendimento","/bdo/oba/tb_industria_conhecimento"],"destination": "/mtd/corp/unidade_atendimento_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_atendimento_tecnologia_inovacao_situacao',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't2',
       'adf_trigger_name': 'marcela',
       'adf_trigger_time': '2020-11-10T18:00:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_tbu, src_tbc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tbu, src_tbc)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, asc, desc, when, lit, col, concat_ws, lead, lag, date_add, count, array, lower, trim, date_sub
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
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from unidade_atendimento_caracteristica
# MAGIC 
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink)
  var_max_dh_atualizacao = trusted_data.select(max(col("dt_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

var_max_dh_atualizacao

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC cd_unidade_atendimento_dr,cd_pessoa,dt_atualizacao_oba,fl_possui_instituto,fl_ativo,cd_detalhamto_mobilidd,cd_status_financ_bndes,cd_tipo_vinculo_ativo,fl_excluido_logicamente,cd_tipo_categoria_ativo,cd_status_integracao,cd_status, cd_tipo_descricao,dt_atualizacao
# MAGIC FROM tb_unidade_atendimento
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC +
# MAGIC SELECT 
# MAGIC cd_industria_conhecimento,  fl_excluido, fl_categoria_ic_fixamovel, dt_atualizacao, fl_ativainativa
# MAGIC FROM tb_industria_conhecimento
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC </pre>

# COMMAND ----------

useful_columns_unidade_atendimento = ["cd_unidade_atendimento_dr", "cd_pessoa", "dt_atualizacao_oba", "fl_possui_instituto", "fl_ativo", "cd_detalhamto_mobilidd", "cd_status_financ_bndes", "cd_tipo_vinculo_ativo", "fl_excluido_logicamente", "cd_tipo_categoria_ativo", "cd_status_integracao", "cd_status", "cd_tipo_descricao", "dt_atualizacao"]

new_data_unidade_atendimento = spark.read.parquet(src_tbu).select(*useful_columns_unidade_atendimento) \
.filter(col("dt_atualizacao") > var_max_dh_atualizacao) \
.withColumn("cd_tipo_categoria_ativo", when(col("cd_tipo_categoria_ativo").isin(1,2), col("cd_tipo_categoria_ativo")).otherwise(lit(None)))\
.withColumn("fl_excluido_logicamente", when(lower(col("fl_excluido_logicamente"))=="s", 1).when(lower(col("fl_excluido_logicamente"))=="n", 0).otherwise(0))\
.withColumn("dt_atualizacao", col("dt_atualizacao").cast("date"))\
.dropDuplicates()


# COMMAND ----------

useful_columns_industria_conhecimento = ["cd_industria_conhecimento", "fl_excluido", "fl_categoria_ic_fixamovel", "dt_atualizacao", "fl_ativainativa"]

new_data_industria_conhecimento = spark.read.parquet(src_tbc).select(*useful_columns_industria_conhecimento) \
.filter(col("dt_atualizacao") > var_max_dh_atualizacao) \
.withColumn("fl_categoria_ic_fixamovel", when(lower(col("fl_categoria_ic_fixamovel")) == "f", 1).when(lower(col("fl_categoria_ic_fixamovel")) == "m", 2).otherwise(lit(None)))\
.withColumn("fl_excluido", when(lower(col("fl_excluido"))=="s", 1).when(lower(col("fl_excluido"))=="n", 0).otherwise(0))\
.withColumn("dt_atualizacao", col("dt_atualizacao").cast("date"))\
.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming

# COMMAND ----------

unidade_atendimento_column_name_mapping = {"cd_pessoa":"cd_pessoa_unidade_atendimento",                                            
                                           "fl_excluido_logicamente": "fl_excluido_oltp"}
  
for key in unidade_atendimento_column_name_mapping:
  new_data_unidade_atendimento =  new_data_unidade_atendimento.withColumnRenamed(key, unidade_atendimento_column_name_mapping[key])

# COMMAND ----------

industria_conhecimento_column_name_mapping = {"cd_industria_conhecimento":"cd_unidade_atendimento_dr", 
                                              "fl_ativainativa": "fl_ativo",
                                              "fl_excluido":"fl_excluido_oltp", 
                                              "fl_categoria_ic_fixamovel":"cd_tipo_categoria_ativo"}
  
for key in industria_conhecimento_column_name_mapping:
  new_data_industria_conhecimento =  new_data_industria_conhecimento.withColumnRenamed(key, industria_conhecimento_column_name_mapping[key])

# COMMAND ----------

# If there's no new data, then just let it die.
if new_data_unidade_atendimento.count() + new_data_industria_conhecimento.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Normalyzing schemas to union raws

# COMMAND ----------

raw_columns = ["cd_unidade_atendimento_dr", "cd_pessoa_unidade_atendimento", "dt_atualizacao_oba", "fl_possui_instituto", "fl_ativo", "cd_detalhamto_mobilidd", "cd_status_financ_bndes", "cd_tipo_vinculo_ativo", "fl_excluido_oltp", "cd_tipo_categoria_ativo", "cd_status_integracao", "cd_status", "cd_tipo_descricao", "dt_atualizacao"]

# COMMAND ----------

unidade_atendimento_column_to_add = list(set(raw_columns) - set(new_data_unidade_atendimento.columns))

for item in unidade_atendimento_column_to_add:
  new_data_unidade_atendimento =  new_data_unidade_atendimento.withColumn(item, lit(None))

# COMMAND ----------

industria_conhecimento_column_to_add = list(set(raw_columns) - set(new_data_industria_conhecimento.columns))

for item in industria_conhecimento_column_to_add:
  new_data_industria_conhecimento =  new_data_industria_conhecimento.withColumn(item, lit(None))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cd_tipo_situacao_atendimento and dt_termino for each cd_atendimento compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns_unidade_atendimento = ["dt_atualizacao_oba", "fl_possui_instituto", "fl_ativo", "cd_detalhamto_mobilidd", "cd_status_financ_bndes", "cd_tipo_vinculo_ativo", "fl_excluido_oltp", "cd_tipo_categoria_ativo", "cd_status_integracao", "cd_status", "cd_tipo_descricao"]
hash_columns_industria_conhecimento = ["fl_ativo", "fl_excluido_oltp", "cd_tipo_categoria_ativo"]
var_key = ["cd_unidade_atendimento_dr"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn("row_hash", concat_ws("@", *hash_columns_unidade_atendimento))\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_atualizacao")))).dropDuplicates()\
.withColumn("delete", when(col("row_hash") == col("lag_row_hash"), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop("delete", "row_hash", "lag_row_hash")

# COMMAND ----------

new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn("row_hash", concat_ws("@", *hash_columns_industria_conhecimento))\
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
# MAGIC * dt_inicio_vigencia = dt_atualizacao
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.select('*', count('cd_unidade_atendimento_dr').over(var_window).alias('count_cd'))
new_data_industria_conhecimento = new_data_industria_conhecimento.select('*', count('cd_unidade_atendimento_dr').over(var_window).alias('count_cd'))

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento\
.withColumn("dt_lead", lead("dt_atualizacao").over(var_window.orderBy("dt_atualizacao")))\
.withColumnRenamed("dt_atualizacao", "dt_inicio_vigencia")\
.withColumn("dt_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("date")).otherwise(date_sub(col("dt_lead"), 1)))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dt_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

new_data_industria_conhecimento = new_data_industria_conhecimento\
.withColumn("dt_lead", lead("dt_atualizacao").over(var_window.orderBy("dt_atualizacao")))\
.withColumnRenamed("dt_atualizacao", "dt_inicio_vigencia")\
.withColumn("dt_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("date")).otherwise(date_sub(col("dt_lead"), 1)))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dt_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data_unidade_atendimento = tcf.add_control_fields(new_data_unidade_atendimento, var_adf)
new_data_industria_conhecimento = tcf.add_control_fields(new_data_industria_conhecimento, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing types

# COMMAND ----------

column_type_map = {"cd_unidade_atendimento_dr": "string",
                   "cd_pessoa_unidade_atendimento": "int",
                   "dt_atualizacao_oba": "date",
                   "fl_possui_instituto": "string",
                   "fl_ativo": "string",
                   "cd_detalhamto_mobilidd": "int",
                   "cd_status_financ_bndes": "int",
                   "cd_tipo_vinculo_ativo": "int",
                   "fl_excluido_oltp": "int",
                   "cd_tipo_categoria_ativo": "int", 
                   "cd_status_integracao": "int",
                   "cd_status": "int", 
                   "cd_tipo_descricao": "int",
                   "dt_inicio_vigencia": "date",
                   "dt_fim_vigencia": "date"}

for c in column_type_map:
  new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn(c, new_data_unidade_atendimento[c].cast(column_type_map[c]))
  new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn(c, new_data_industria_conhecimento[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  new_data = new_data_unidade_atendimento.union(new_data_industria_conhecimento.select(new_data_unidade_atendimento.columns))
  new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")
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
# MAGIC /* Versionar se e somente se houve alteração em algum dos campos encerrar a vigencia do registro existente atualizar dt_fim_vigencia com a dt_atualização - 1
# MAGIC E incluir o novo registro com a dt_inicio_vigencia = dt_atualizacao e dt_fim_vigencia nulo  */
# MAGIC Para cada dt_atualizacao encontrada na tb_unidade_atendimento
# MAGIC     Verifica se existe na trs unidade_atendimento_situacao o raw.cd_unidade_atendimento_dr = trs.cd_unidade_atendimento
# MAGIC         Se existe 
# MAGIC         Se houve alteração: 
# MAGIC         raw.dt_atualizacao_oba	  <>  trs.dt_atualizacao_oba
# MAGIC         raw.fl_possui_instituto	  <>  trs.fl_possui_instituto
# MAGIC         raw.fl_ativo		         <>  trs.fl_ativo
# MAGIC         raw.cd_detalhamto_mobilidd	  <>  trs.cd_detalhamento_mobilidade
# MAGIC         raw.cd_status_financ_bndes	  <>  trs.cd_status_financiamento_bndes
# MAGIC         raw.cd_tipo_vinculo_ativo	  <>  trs.cd_tipo_vinculo_ativo
# MAGIC         raw.fl_excluido_logicamente  <>  trs.fl_excluido_oltp
# MAGIC         raw.cd_tipo_categoria_ativo  <>  trs.cd_tipo_categoria_ativo
# MAGIC         raw.cd_status_integracao	  <>  trs.cd_status_integracao
# MAGIC         raw.cd_status               <>  trs.cd_status
# MAGIC         raw.cd_tipo_descricao	  <>  trs.cd_tipo_descricao
# MAGIC 
# MAGIC Para cada dt_atualizacao encontrada na tb_industria_conhecimento
# MAGIC     Verifica se existe na trs unidade_atendimento_situacao o raw.cd_industria_conhecimento = trs.cd_unidade_atendimento
# MAGIC         Se existe 
# MAGIC         Se houve alteração: 
# MAGIC         raw.fl_ativoinativo           <>  trs.fl_ativo
# MAGIC         raw.fl_excluido               <>  trs.fl_excluido_oltp
# MAGIC         raw.fl_categoria_ic_fixamovel <>  trs.cd_tipo_categoria_ativo
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = trusted_data.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare_unidade_atendimento = old_data_active. \
join(new_data_unidade_atendimento.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

old_data_to_compare_industria_conhecimento = old_data_active. \
join(new_data_industria_conhecimento.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite_unidade_atendimento = old_data_active. \
join(new_data_unidade_atendimento.select(var_key).dropDuplicates(),var_key, "leftanti") 

old_data_active_to_rewrite_industria_conhecimento = old_data_active. \
join(new_data_industria_conhecimento.select(var_key).dropDuplicates(),var_key, "leftanti") 

old_data_active_to_rewrite = old_data_active_to_rewrite_unidade_atendimento.\
union(old_data_active_to_rewrite_industria_conhecimento.\
      select(old_data_active_to_rewrite_unidade_atendimento.columns)).\
dropDuplicates()

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dt_fim_vigencia"

new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn("is_new", lit(1).cast("int"))
new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.union(old_data_to_compare_unidade_atendimento.select(new_data_unidade_atendimento.columns))
new_data_industria_conhecimento = new_data_industria_conhecimento.union(old_data_to_compare_industria_conhecimento.select(new_data_industria_conhecimento.columns))

# COMMAND ----------

hash_columns_unidade_atendimento = var_key + ["dt_atualizacao_oba", "fl_possui_instituto", "fl_ativo", "cd_detalhamto_mobilidd", "cd_status_financ_bndes", "cd_tipo_vinculo_ativo", "fl_excluido_oltp", "cd_tipo_categoria_ativo", "cd_status_integracao", "cd_status", "cd_tipo_descricao"]
hash_columns_industria_conhecimento = var_key + ["fl_ativo", "fl_excluido_oltp", "cd_tipo_categoria_ativo"]

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn("row_hash", concat_ws("@", *hash_columns_unidade_atendimento))
new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn("row_hash", concat_ws("@", *hash_columns_industria_conhecimento))

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_inicio_vigencia")))).dropDuplicates()
new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_inicio_vigencia")))).dropDuplicates()

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

new_data_unidade_atendimento = new_data_unidade_atendimento\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

new_data_industria_conhecimento = new_data_industria_conhecimento\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

new_data_unidade_atendimento = new_data_unidade_atendimento.withColumn("lead_dt_inicio_vigencia", lead("dt_inicio_vigencia", 1).over(var_window.orderBy(asc("dt_inicio_vigencia"))))
new_data_industria_conhecimento = new_data_industria_conhecimento.withColumn("lead_dt_inicio_vigencia", lead("dt_inicio_vigencia", 1).over(var_window.orderBy(asc("dt_inicio_vigencia"))))

# COMMAND ----------

# When lead_dt_inicio_vigencia is not NULL, then we must close the period with "lead_dt_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

new_data_unidade_atendimento = new_data_unidade_atendimento. \
withColumn("dt_fim_vigencia", when(col("lead_dt_inicio_vigencia").isNotNull(), date_sub(col("lead_dt_inicio_vigencia"), 1))). \
withColumn("fl_corrente", when(col("lead_dt_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dt_inicio_vigencia")

new_data_industria_conhecimento = new_data_industria_conhecimento. \
withColumn("dt_fim_vigencia", when(col("lead_dt_inicio_vigencia").isNotNull(), date_sub(col("lead_dt_inicio_vigencia"), 1))). \
withColumn("fl_corrente", when(col("lead_dt_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dt_inicio_vigencia")

new_data = new_data_unidade_atendimento.union(new_data_industria_conhecimento.select(new_data_unidade_atendimento.columns)). \
dropDuplicates()

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

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite). \
dropDuplicates()

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns)). \
dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

