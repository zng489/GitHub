# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import regexp_replace,col
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# captura os data stewards
default_dir = ""
path_gov = "{var_adls_uri}/{default_dir}/gov/tables/".format(var_adls_uri=var_adls_uri,default_dir=default_dir)
data_steward = spark.read.format("delta").load(path_gov + "data_steward")
data_steward = data_steward.withColumn('name_data_steward', regexp_replace(col('name_data_steward'), " ", "_"))
data_steward.show()

# COMMAND ----------

for var_data_steward in data_steward.rdd.collect():
  for var_tabela in ['field','table']:
    #executa atualização dos dbfs
    env = 'prod'
    tabela = var_tabela
    steward = var_data_steward['cod_data_steward']
    steward_name = var_data_steward['name_data_steward']
    params = {"env": env,"gov_table": tabela, "gov_steward": steward, "gov_steward_name": steward_name}
    try:
      dbutils.notebook.run("generate_desc_csv - por curador".format(env=env),0,params)
    except:
      print("atualização falhou")    

# COMMAND ----------


