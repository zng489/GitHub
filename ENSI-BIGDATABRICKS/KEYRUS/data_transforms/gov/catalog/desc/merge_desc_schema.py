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
path_lnd_gov = "{adl_path}{default_dir}/gov/usr_upload/schema/".format(adl_path=var_adls_uri,default_dir=default_dir,source_type=source_type)
path_schema = "{adl_path}{default_dir}/gov/tables/schema".format(adl_path=var_adls_uri,default_dir=default_dir)
schema_dt = DeltaTable.forPath(spark, path_schema)

# COMMAND ----------

file_exists = 1
try:
  schema_desc_df = spark.read.format("csv")\
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
  schema_desc_df = schema_desc_df.withColumn("updated_at", current_timestamp())#\
                                 #.filter("description is not null")
  
  #campos que devem ser atualiados
  update_set = {
    "cod_data_steward": "update.cod_data_steward",
    "description": "update.description",
    "updated_at": "update.updated_at",
    "replica": "update.replica"
  }

  schema_dt.alias("governance").merge(
                                schema_desc_df.alias("update"),\
                                "governance.source_name = update.source_name and governance.schema_name = update.schema_name")\
                               .whenMatchedUpdate(set = update_set)\
                               .execute()

# COMMAND ----------

if file_exists == 1:
   dbutils.fs.rm(path_lnd_gov,True)
   dbutils.fs.mkdirs(path_lnd_gov)