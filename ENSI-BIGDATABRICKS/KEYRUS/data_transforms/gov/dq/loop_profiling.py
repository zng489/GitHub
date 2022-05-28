# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
from pyspark.sql.utils import AnalysisException
import datetime
from datetime import date
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")
if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_catalog = "{adl_path}{default_dir}/gov/tables/table".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

df_catalog = spark.read.format("delta").load(path_catalog)

# COMMAND ----------

#df_tables_to_profile = df_catalog.filter("(source_type = 'oracle' or source_type = 'external') and replica = 1")
df_tables_to_profile = df_catalog.filter("replica = 1")
#df_tables_to_profile = df_catalog.filter("lower(table_name) = 'tb_tipo_acao'")

# COMMAND ----------

# ajusta temporariamente para executar o profiling do protheus
df_tables_to_profile = df_tables_to_profile.withColumn('schema_name', 
                                                       F.when(df_tables_to_profile['schema_name']=='PROTHEUS.dbo','protheus11').\
                                                       otherwise(df_tables_to_profile['schema_name']))

# COMMAND ----------

df_tables_to_profile = df_tables_to_profile.filter("not lower(table_name) like '%bkp_%'")

# COMMAND ----------

# cria a data única geral para passar como parâmetro
marca = date.today().strftime("%Y-%m-%d")

# COMMAND ----------

profiling_list = df_tables_to_profile.collect()
for row in profiling_list:
  params = {"source": str(row[0]),
            "source_type": str(row[1]).lower(),
            "schema": str(row[2]).lower(),
            "table": str(row[3]).lower(),
            "env": env,
            "marca": marca}
  try:
    dbutils.notebook.run("/KEYRUS/{env}/gov/dq/execute_profiling".format(env=env),0,params)
  except:
    print("profile falhou")