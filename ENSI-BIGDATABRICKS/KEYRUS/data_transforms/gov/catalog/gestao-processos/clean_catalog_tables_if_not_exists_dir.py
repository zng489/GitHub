# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import datetime
from pyspark.sql import *
from decimal import Decimal
from pyspark.sql import functions as F

# COMMAND ----------

## captura os dados das tabelas no blob storage
var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
var_folder = "" 
#var_folder = "tmp/dev/raw/gov"
path_gov = "{var_adls_uri}/{var_folder}/gov/tables/".format(var_adls_uri=var_adls_uri,var_folder=var_folder)

table = spark.read.format("delta").load(path_gov + "table")
field = spark.read.format("delta").load(path_gov + "field")
profiling = spark.read.format("delta").load(path_gov + "profiling")

# COMMAND ----------

## cria a função para identificar se a pasta existe ou não no datalake
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

## varre todos os caminhos de tabelas do catálogo e indica quais não existem pasta no datalake
df = table.filter('replica = 1')
lista = df.collect()
nova_lista = []
for row in lista:
  source_name = row[0]
  schema_name = row[2]
  table_name = row[3]
  replica = row[5]
  path = row[10]
  
  var_path = path
  diretorio_cheio = "{var_adls_uri}/{var_folder}{var_path}".format(var_adls_uri=var_adls_uri,var_folder=var_folder,var_path=var_path)
  if not file_exists(diretorio_cheio):
    print(path)

# COMMAND ----------

## lista a tabela via path encontrada para capturar os campos da chave primária "source_name, schema_name, table_name"
display(table.filter('path like "%/caged_ajustes/"'))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* consultas sql nas tabelas do esquema de governança de dados no databricks (deltalake) */
# MAGIC 
# MAGIC select * from governance_gen2_prd.profiling where source_name = 'crw' and schema_name = 'me' and table_name = 'caged_ajustes'
# MAGIC -- table   field    profiling

# COMMAND ----------

# MAGIC %sql
# MAGIC /* deleta a linha do delta lake */
# MAGIC delete from governance_gen2_prd.profiling where source_name = 'crw' and schema_name = 'me' and table_name = 'caged_ajustes'
# MAGIC -- table   field    profiling

# COMMAND ----------

#executa atualização dos dbfs
env = 'prod'
tabela = 'table' ## table  field  profiling
params = {"env": env,"gov_table": tabela}
try:
  dbutils.notebook.run("/KEYRUS/dev/gov/update_dbfs_gov_tables".format(env=env),0,params)
except:
  print("atualização falhou")

# COMMAND ----------


