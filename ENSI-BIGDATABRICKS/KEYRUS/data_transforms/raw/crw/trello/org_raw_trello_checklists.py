# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import os
from pyspark.sql.functions import *
#import pandas as pd

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

arquivos_brutos =       "{adl_path}"+lnd+"/crw/trello__checklist/"  #local onde esta as extrações do PYTHON
arquivos_tratados_raw = "{adl_path}"+raw+"/crw/trello__checklist/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

##TESTE MANUAL

#var_adls_uri    =       "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net"
#arquivos_brutos =       "{adl_path}/tmp/dev/lnd/crw/trello__checklist/"  #local onde esta as extrações do PYTHON
#arquivos_tratados_raw = "{adl_path}/tmp/dev/raw/crw/trello__checklist/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas


# COMMAND ----------

# DBTITLE 1,Trata CHECKLISTS
print(arquivos_brutos.format(adl_path=var_adls_uri) )
arquivos = dbutils.fs.ls( arquivos_brutos.format(adl_path=var_adls_uri) )

li = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  caminho_arquivo = file.path
  if nome.find("_checklists")>=1:
    print(nome)
    str_shortnome = str(nome).replace('boards_cards_checklists_','').replace('.parquet','')
    c = spark.read.parquet(arquivos_brutos.format(adl_path=var_adls_uri)+nome)
    c = c.withColumn("shortLink", lit(str_shortnome))
    
    try:
      c = c.drop('shortLink_board')
    except Exception as e:
      print('arquivo antigo')
    try:
      li.append(c)
    except Exception as e:
      print("ERRO: {0} ".format(nome))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('CHECKLISTS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(li)))

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

lista_checklist = reduce(DataFrame.unionAll, li)
lista_checklist = lista_checklist.distinct().replace('None','')

# COMMAND ----------

#gera parquet
lista_checklist.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='checklist'))