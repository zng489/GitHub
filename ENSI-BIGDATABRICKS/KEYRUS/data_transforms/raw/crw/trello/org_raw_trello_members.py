# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import os
from pyspark.sql.functions import *

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

#objeto de controle do log
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

arquivos_brutos =       "{adl_path}"+lnd+"/crw/trello__member/"  #local onde esta as extrações do PYTHON
arquivos_tratados_raw = "{adl_path}"+raw+"/crw/trello__member/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

#TESTE MANUAL

#var_adls_uri    =       "adl://cnibigdatadls.azuredatalakestore.net/tmp/dev/"
#arquivos_brutos =       "{adl_path}/lnd/crw/trello__member/"  #local onde esta as extrações do PYTHON
#arquivos_tratados_raw = "{adl_path}/raw/crw/trello__member/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

# DBTITLE 1,Trata MEMBERS
arquivos = dbutils.fs.ls( arquivos_brutos.format(adl_path=var_adls_uri) )

li = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if  nome.find("_members_") >=1:
    print(nome)
    c = spark.read.parquet(arquivos_brutos.format(adl_path=var_adls_uri)+nome)
    try:
      li.append(c)
    except Exception as e:
      print("ERRO: {0} ".format(nome))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('MEMBERS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(li)))

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

lista_member = reduce(DataFrame.unionAll, li)
lista_member = lista_member.replace('None','').drop('idCard').drop('idBoard').distinct()

# COMMAND ----------

#gera parquet
lista_member.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='members'))