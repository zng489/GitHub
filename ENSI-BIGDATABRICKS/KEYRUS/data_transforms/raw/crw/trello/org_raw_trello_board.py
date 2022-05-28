# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import os
from pyspark.sql.functions import *

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

#objeto de controle do log
##################################################
metadata = {'finished_with_errors': False}
lista_metadata = []

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

#PARAMETROS
lnd = dls['folders']['landing']
raw = dls['folders']['raw']
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

arquivos_brutos =       "{adl_path}"+lnd+"/crw/trello__board/"                       #local onde esta as extrações do PYTHON
arquivos_tratados_raw = "{adl_path}"+raw+"/crw/trello__board/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

#TESTE MANUAL
#var_adls_uri    =       "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net"
#lnd="/tmp/dev/lnd"
#raw="/tmp/dev/raw"
#arquivos_brutos =       "{adl_path}"+lnd+"/crw/trello__board/"                #local onde esta as extrações do PYTHON
#arquivos_tratados_raw = "{adl_path}"+raw+"/crw/trello__board/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

# DBTITLE 1,Trata os Boards
arquivos = dbutils.fs.ls( arquivos_brutos.format(adl_path=var_adls_uri) )

li = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if nome.find("_boards")>=1  :
    print(nome)
    boards = spark.read.parquet(arquivos_brutos.format(adl_path=var_adls_uri)+nome)
    try:
      li.append(boards)
    except Exception as e:
      print("ERRO: {0} ".format(nome))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('BOARDS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(li)))

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame


lista_board = reduce(DataFrame.unionAll, li)

#Ajsuta campos
lista_board = lista_board.withColumn("dateLastActivity",to_timestamp(col("dateLastActivity")))

#parse
lista_board = lista_board.toPandas()

# COMMAND ----------

#gera parquet e csv
df = spark.createDataFrame(lista_board)
df.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='boards'))

print('\nREGISTROS: {0}'.format(df.count()))

# COMMAND ----------

df.coalesce(1).write.mode("Overwrite").csv("{adl_path}/lnd/crw/trello/config/quadros".format(adl_path=var_adls_uri))

files = dbutils.fs.ls("{adl_path}/lnd/crw/trello/config/quadros".format(adl_path=var_adls_uri))
#print(files)

for i in files:
  if i.name[:4]=="part": 
    print(i.path)
    dbutils.fs.mv(i.path,"{adl_path}/lnd/crw/trello/config".format(adl_path=var_adls_uri)+"/"+"quadros.csv")
    dbutils.fs.rm("{adl_path}/lnd/crw/trello/config/quadros".format(adl_path=var_adls_uri),True)
