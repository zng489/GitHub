# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

dbutils.widgets.text("source_type","metadata")
source_type = dbutils.widgets.get("source_type")
dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

#captura do arquivo da pasta de upload
if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_lnd_gov = "{adl_path}{default_dir}/gov/usr_upload/table/".format(adl_path=var_adls_uri,default_dir=default_dir,source_type=source_type)
path_table = "{adl_path}{default_dir}/gov/tables/table".format(adl_path=var_adls_uri,default_dir=default_dir)
table_dt = DeltaTable.forPath(spark, path_table)

# COMMAND ----------

file_exists = 1
try:  
  table_desc_df = spark.read.format("csv")\
                             .option("sep", ";")\
                             .option("header", "true")\
                             .option("encoding", "ISO-8859-1")\
                             .load(path_lnd_gov)
except(AnalysisException) as e:
  file_exists = 0
  print("Arquivo não encontrado")
  dbutils.notebook.exit("File not found")

# COMMAND ----------

#se o arquivo existe no diretório de upload "gov/usr_upload/table/"
if file_exists == 1:
  
  #atualiza (no arquivo importado) a coluna de data de atualização e filtra somente os registros cuja descrição não é nula
  table_desc_df = table_desc_df.withColumn("updated_at", current_timestamp())\
                               #.filter("description is not null")  
  
  #campos que devem ser atualiados (captura as colunas do arquivo)
  colunas_csv = table_desc_df.columns
  dic = ''
  for coluna in colunas_csv:
    if coluna not in ['source_name','source_type','schema_name','table_name']:
      dic = dic + '"' + coluna + '"' + ':' + '"update.' + coluna + '", '
  dic = '{' + dic[:-2] + '}'
  update_set = eval(dic)



# COMMAND ----------

update_set

# COMMAND ----------

  #faz o merge na tabela
  table_dt.alias("governance").merge(
                                table_desc_df.alias("update"),\
                                 "   governance.source_name = update.source_name\
                                 and governance.schema_name = update.schema_name\
                                 and governance.table_name = update.table_name")\
                               .whenMatchedUpdate(set = update_set)\
                               .execute()  

# COMMAND ----------

"""
#se o arquivo existe no diretório de upload "gov/usr_upload/table/"
if file_exists == 1:
  
  #atualiza (no arquivo importado) a coluna de data de atualização e filtra somente os registros cuja descrição não é nula
  table_desc_df = table_desc_df.withColumn("updated_at", current_timestamp())\
                               #.filter("description is not null")  

  #campos que devem ser atualiados
  update_set = {
     "description": "update.description"
    ,"cod_data_steward": "update.cod_data_steward"
    ,"replica": "update.replica"
    ,"updated_at": "update.updated_at"
    ,"dsc_business_subject": "update.dsc_business_subject"
  }

  #faz o merge na tabela
  table_dt.alias("governance").merge(
                                table_desc_df.alias("update"),\
                                 "   governance.source_name = update.source_name\
                                 and governance.schema_name = update.schema_name\
                                 and governance.table_name = update.table_name")\
                               .whenMatchedUpdate(set = update_set)\
                               .execute()
"""

# COMMAND ----------

if file_exists == 1:
   dbutils.fs.rm(path_lnd_gov,True)
   dbutils.fs.mkdirs(path_lnd_gov)

# COMMAND ----------

