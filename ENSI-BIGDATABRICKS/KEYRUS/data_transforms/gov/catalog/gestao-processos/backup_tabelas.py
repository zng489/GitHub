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

# COMMAND ----------

# backup das tabelas
pasta = '__bkp/bkp_2022_01_25/'
var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
path_gov = "{var_adls_uri}/gov/tables/".format(var_adls_uri=var_adls_uri)
tabelas = ['table','source','schema','profiling','field','data_steward']
for tabela in tabelas:
  df = spark.read.format("delta").load(path_gov + tabela)
  df.write.format("delta").mode("overwrite").save(path_gov + pasta + tabela)
  print(tabela)

# COMMAND ----------

#deltaTable = DeltaTable.forPath(spark, path_gov + "field")

# COMMAND ----------

#deltaTable.update("login = ''", { "login": "'-'" } )

# COMMAND ----------

display(field.filter("login = '-'"))

# COMMAND ----------


