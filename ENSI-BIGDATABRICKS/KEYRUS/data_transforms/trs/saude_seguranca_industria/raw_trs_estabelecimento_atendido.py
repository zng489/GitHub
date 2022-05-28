# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_estabelecimento_atendido
# MAGIC Tabela/Arquivo Origem	/raw/bdo/smd/estabelecimento
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/estabelecimento_atendido
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp)
# MAGIC Descrição Tabela/Arquivo Destino	"Registra as informações de identificação (ESTÁVEIS) do estabelecimento
# MAGIC OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria."
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dataatualizacao, que insere ou sobrescreve o registro caso a chave id seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw inddesempenho, que ocorre às 22:00
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
# MAGIC ## Trusted specific parameter section

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
var_tables =  {"origins": ["/bdo/inddesempenho/estabelecimento",
                           "/bdo/inddesempenho/empresa_dn"],
               "destination": "/mtd/sesi/estabelecimento_atendido"}

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
       'adf_pipeline_name': 'raw_trs_estabelecimento_atendido',
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

src_es, src_edn = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_es, src_edn)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation section

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, current_date, dense_rank, asc, desc, greatest, trim, substring, when, lit, col, concat_ws, lead, lag, row_number, sum, date_format
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from estabelecimento_atendido
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink).cache()
  var_max_dh_ultima_atualizacao_oltp = trusted_data.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))
except AnalysisException:
  first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting new available data:
# MAGIC <pre>
# MAGIC SELECT
# MAGIC id,cei,cnpj,razaosocial,dataatualizacao, 
# MAGIC WHEN 
# MAGIC    CASE edn.estabelecimento_id IS NULL THEN 0 ELSE 1 END fl_empresa_dn
# MAGIC FROM estabelecimento e
# MAGIC LEFT JOIN empresa_dn edn ON edn.estabelecimento_id = e.id
# MAGIC WHERE dataatualizacao >= #var_max_dh_ultima_atualizacao_oltp
# MAGIC </pre>

# COMMAND ----------

estabelecimento_columns = ["id", "cei", "cnpj", "razaosocial", "dataatualizacao"]
new_data = spark.read.parquet(src_es).select(*estabelecimento_columns) \
.filter(col("dataatualizacao") >= var_max_dh_ultima_atualizacao_oltp) \
.withColumn("cd_ano_atualizacao", year(col("dataatualizacao")))

# COMMAND ----------

# If there's no new data, then just let it die.
if new_data.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

empresa_dn_columns = ["estabelecimento_id"]
df_edn = spark.read.parquet(src_edn).select(*empresa_dn_columns)

# COMMAND ----------

new_data = new_data.join(df_edn, new_data["id"] == df_edn["estabelecimento_id"] , "left") \
.withColumn("fl_empresa_dn", when(col("estabelecimento_id").isNull(), 0).otherwise(1))\
.drop("estabelecimento_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the last updates in new data
# MAGIC <pre>
# MAGIC From documentation:
# MAGIC   Se existir mais de uma versão no período utilizar a mais recente 
# MAGIC </pre>

# COMMAND ----------

new_data = new_data.withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy(desc("dataatualizacao")))) \
                   .filter(col("row_number")==1) \
                   .drop("row_number")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing column names and types

# COMMAND ----------

column_name_mapping = {'id':'cd_estabelecimento_atendido_oltp',
                       'cei':'cd_cei',
                       'cnpj':'cd_cnpj',
                       'razaosocial':'nm_razao_social',
                       'dataatualizacao':'dh_ultima_atualizacao_oltp'}
  
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

var_columns_to_trim = ["cd_cei", "cd_cnpj", "nm_razao_social"]

for key in var_columns_to_trim:
  new_data = new_data.withColumn(key, trim(col(key)))

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_estabelecimento_atendido_oltp" : "string",      
                   "cd_cei": "string",
                   "cd_cnpj": "string",
                   "nm_razao_social": "string",
                   "dh_ultima_atualizacao_oltp": "timestamp", 
                   "fl_empresa_dn": "int",
                   "cd_ano_atualizacao": "integer"}
for c in column_type_map:
  new_data = new_data.withColumn(c, new_data[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert control column

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### If first load, save it now!

# COMMAND ----------

if first_load is True:
  # Partitioning as specified by the documentation
  new_data.coalesce(5).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ####   Get old data to replace
# MAGIC <pre>
# MAGIC From all the old data available in trusted, the intersection of *keys_from_new_data* and *trusted_data* will provide us with what should be replaced.
# MAGIC </pre>

# COMMAND ----------

# Just getting what was already available on trusted that needs to append balance. This will also lead to what records need to be updated in the next operation.
old_data_to_replace = trusted_data.join(new_data.select("cd_estabelecimento_atendido_oltp").dropDuplicates(), ["cd_estabelecimento_atendido_oltp"], "inner")

# COMMAND ----------

new_data = new_data.union(old_data_to_replace.select(new_data.columns))

# COMMAND ----------

new_data = new_data.withColumn("row_number", 
                               row_number().over(Window.partitionBy("cd_estabelecimento_atendido_oltp") \
                                           .orderBy(desc("dh_ultima_atualizacao_oltp"), asc("dh_insercao_trs")))) \
.filter(col("row_number")==1) \
.drop("row_number")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete records that will be updated from original trusted.
# MAGIC <pre>
# MAGIC Now we need to delete the changed records from the original trusted, because want to update it!
# MAGIC </pre>

# COMMAND ----------

trusted_data = trusted_data.subtract(old_data_to_replace.select(trusted_data.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Joining new data with old trusted
# MAGIC <pre>
# MAGIC Defining new trusted as a union between the old records that had already been saved in the last load and the new records that we are loading right now
# MAGIC </pre>

# COMMAND ----------

df = trusted_data.union(new_data.select(trusted_data.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get years that have some change

# COMMAND ----------

years_with_change = new_data.select("cd_ano_atualizacao").distinct() \
                            .union(old_data_to_replace.select("cd_ano_atualizacao").distinct()) \
                            .dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Keep only changed years and write as incremental using partition

# COMMAND ----------

df = df.join(years_with_change, ["cd_ano_atualizacao"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink

# COMMAND ----------

df.coalesce(5).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

trusted_data.unpersist()