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

var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
var_folder = "" 
#var_folder = "tmp/dev/raw/gov"
path_gov = "{var_adls_uri}/{var_folder}/gov/tables/".format(var_adls_uri=var_adls_uri,var_folder=var_folder)

table = spark.read.format("delta").load(path_gov + "table")
field = spark.read.format("delta").load(path_gov + "field")
profiling = spark.read.format("delta").load(path_gov + "profiling")
data_steward = spark.read.format("delta").load(path_gov + "data_steward")

# COMMAND ----------

############################################################################################################
################################### Lista os CURADORES #####################################################
############################################################################################################
display(data_steward)

# COMMAND ----------

############################################################################################################
################################### Lista os ativos SEM CURADOR ############################################
############################################################################################################
display(table \
        .select('source_name','source_type','schema_name','table_name','path','cod_data_steward')
        .filter('replica = 1 and cod_data_steward is null') \
        .orderBy('path'))

# COMMAND ----------

#executa atualização dos dbfs
env = 'prod'
tabela = 'field'
params = {"env": env,"gov_table": tabela}
try:
  dbutils.notebook.run("/KEYRUS/dev/gov/update_dbfs_gov_tables".format(env=env),0,params)
except:
  print("atualização falhou")
