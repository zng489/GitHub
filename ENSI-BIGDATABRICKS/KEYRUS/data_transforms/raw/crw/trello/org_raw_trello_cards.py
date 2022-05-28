# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import os
from pyspark.sql.functions import *
import pandas as pd

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

arquivos_brutos =       "{adl_path}"+lnd+"/crw/trello__card/"  #local onde esta as extrações do PYTHON
arquivos_tratados_raw = "{adl_path}"+raw+"/crw/trello__card/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

#TESTE MANUAL

#var_adls_uri    =       "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net"
#arquivos_brutos =       "{adl_path}/tmp/dev/lnd/crw/trello__card/"  #local onde esta as extrações do PYTHON
#arquivos_tratados_raw = "{adl_path}/tmp/dev/raw/crw/trello__card/{file}.parquet"  #local onde ficara os dados tratados em nivel de linhas

# COMMAND ----------

print( arquivos_brutos.format(adl_path=var_adls_uri)  )

# COMMAND ----------

# DBTITLE 1,Trata os Cards
arquivos = dbutils.fs.ls( arquivos_brutos.format(adl_path=var_adls_uri) )

li = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if  ( nome.find("_cards_")>=1 and  nome.find("_checklists_") ==-1 and   nome.find("_members_") ==-1 and   nome.find("_customfields_") ==-1 and  nome.find("_list_") ==-1  and   nome.find("_organization_") ==-1 and  nome.find("_actions_")) ==-1:
    print(nome)
    cards = spark.read.parquet(arquivos_brutos.format(adl_path=var_adls_uri)+nome)
    try:
      card    = cards.select('area','idBoard','idCard','idMembers','idList','idChecklists','idLabels','name','desc','closed','dateLastActivity','due','labels','pos')
      li.append(card)
    except Exception as e:
      print("ERRO: {0} ".format(nome))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('CARDS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(li)))

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

#lista_cards = pd.concat(li, axis=0, ignore_index=True).replace('None','')
lista_cards = reduce(DataFrame.unionAll, li)

#Ajsuta campos
lista_cards = lista_cards.withColumn("dateLastActivity",to_timestamp(col("dateLastActivity")))
lista_cards = lista_cards.withColumn("due",to_timestamp(col("due")))

lista_cards = lista_cards.toPandas()

# COMMAND ----------

# DBTITLE 1,DADOS DO CARD
#pd_card = lista_cards
#card_filter = pd_card[(pd_card.name.str.contains("Identificar ferramentas sobre "))]
#card_filter[{'area','idBoard','idCard','name','idChecklists'}] 

# COMMAND ----------

# DBTITLE 1,TABELA DE JUNCAO : MEMBERS IN CARDS
BOARDS_CARDS_MEMBERS = pd.DataFrame(columns=[
          "idBoard",
          "idCard",
          "idMember"
])

#varre todos os cards
for i,card in lista_cards.iterrows():
  conjunto = eval(card['idMembers'])
  #varre todos os membros
  for member in conjunto:
    #Constroi lista de cards final
    BOARDS_CARDS_MEMBERS = BOARDS_CARDS_MEMBERS.append(
                        {
                            "idBoard":  card['idBoard'],
                            "idCard":   card['idCard'],
                            "idMember": member
                        }, ignore_index=True)
    
#gera parquet
df_member_card = spark.createDataFrame(BOARDS_CARDS_MEMBERS)
df_member_card.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='juncao_boards_cards_members'))

# COMMAND ----------

# DBTITLE 1,TRATA TABELA DE CHECKLIST IN CARDS
BOARDS_CARDS_CHECKLIST = pd.DataFrame(columns=[
          "idBoard",
          "idCard",
          "idChecklist"
])

#varre todos os cards
for i,card in lista_cards.iterrows():
  conjunto = eval(card['idChecklists'])
  #varre todos os membros
  for item in conjunto:
    if item:
      #Constroi lista de cards final
      BOARDS_CARDS_CHECKLIST = BOARDS_CARDS_CHECKLIST.append(
                          {
                              "idBoard":  card['idBoard'],
                              "idCard":   card['idCard'],
                              "idChecklist": item
                          }, ignore_index=True)
    


#gera parquet
df_check_card = spark.createDataFrame(BOARDS_CARDS_CHECKLIST)
df_check_card.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='juncao_boards_cards_checklits'))

# COMMAND ----------

# DBTITLE 1,TRATA TABELA DE LABEL IN CARDS

BOARDS_CARDS_LABEL = pd.DataFrame(columns=[
          "idBoard",
          "idCard",
          "idLabel",
          "labels_name",
          "labels_color"
])

#varre todos os cards
for i,card in lista_cards.iterrows():
  conjunto = eval(card['idLabels'])
  #varre todos os membros
  for item in conjunto:
    if item:
      #Constroi lista de cards final
      json_label =   eval(card['labels'])
      df = sqlContext.read.json(sc.parallelize(json_label))
      for lrow in df.collect():
        name_label  = ''
        color_label = ''
        
        try:
          name_label  = lrow.name
          color_label = lrow.color
        except Exception as e:
          print('Label não definida')
          
        
        BOARDS_CARDS_LABEL = BOARDS_CARDS_LABEL.append(
                          {
                              "idBoard":  card['idBoard'],
                              "idCard":   card['idCard'],
                              "idLabel": item,
                              "labels_name":name_label,
                              "labels_color":color_label
                          }, ignore_index=True)
      
#gera parquet
if len(BOARDS_CARDS_LABEL) != 0:
  df_label_card = spark.createDataFrame(BOARDS_CARDS_LABEL)
  df_label_card.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='juncao_boards_cards_labels'))

# COMMAND ----------

# DBTITLE 1,Cards
#gera parquet
df = spark.createDataFrame(lista_cards[['area','idBoard','idCard','idList','name','desc','closed','dateLastActivity','due','pos']])
df.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='cards'))

print('\nREGISTROS: {0}'.format(df.count()))

# COMMAND ----------

# DBTITLE 1,CUSTOMFIELDS
arquivos = dbutils.fs.ls( var_adls_uri+'/lnd/crw/trello__customfields/' )

lic = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if   nome.find("_customfields")>=1:
    print(nome)
    cards = spark.read.parquet(var_adls_uri+'/lnd/crw/trello__customfields/'+nome)
    try:
      c    = cards.select('area','idBoard','idCards','idCustomField','name','type','value').toPandas()
      lic.append(c)
    except Exception as e:
      print("ERRO: {0} : {1}".format(nome,e))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('CUSTOMFIELDS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(lic)))

# COMMAND ----------

lista_customfields = pd.concat(lic, axis=0, ignore_index=True).replace('None','')
lista_customfields = lista_customfields.rename(columns = {'idCards': 'idCard'}, inplace = False)

# COMMAND ----------

#gera parquet
df = spark.createDataFrame(lista_customfields[['area','idBoard','idCard','idCustomField','name','type','value']])
df.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='customfields'))

print('\nREGISTROS: {0}'.format(df.count()))

# COMMAND ----------

# DBTITLE 1,ACTIONS
arquivos = dbutils.fs.ls( var_adls_uri+'/lnd/crw/trello__actions/' )

lia = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if   nome.find("_actions")>=1:
    print(nome)
    cards = spark.read.parquet(var_adls_uri+'/lnd/crw/trello__actions/'+nome)
    try:
      c    = cards.select('area','idBoard','idCard','idAction','idMemberCreator','type','date','data_text').toPandas()
      lia.append(c)
    except Exception as e:
      print("ERRO: {0} : {1}".format(nome,e))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('ACTIONS')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(lic)))

# COMMAND ----------

lista_actions = pd.concat(lia, axis=0, ignore_index=True).replace('None','')
lista_actions['date']=pd.to_datetime(lista_actions['date'])

# COMMAND ----------

#gera parquet
df = spark.createDataFrame(lista_actions[['area','idBoard','idCard','idAction','idMemberCreator','type','date','data_text']])
df.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='actions'))

print('\nREGISTROS: {0}'.format(df.count()))

# COMMAND ----------

# DBTITLE 1,LIST
arquivos = dbutils.fs.ls( var_adls_uri+'/lnd/crw/trello__list/' )

lic = []
#LISTA APENAS OS CARDS
for file in arquivos:
  nome = file.name
  if   nome.find("_list")>=1:
    print(nome)
    cards = spark.read.parquet(var_adls_uri+'/lnd/crw/trello__list/'+nome)
    try:
      c    = cards.select('area','idBoard','idCard','idList','pos','closed','name').toPandas()
      lic.append(c)
    except Exception as e:
      print("ERRO: {0} : {1}".format(nome,e))
      
      metadata['exit'] = 0
      metadata['finished_with_errors'] = True
      metadata['tipo'] = str('LIST')
      metadata['arquivo'] = str(nome)
      metadata['desc'] = str('Formato invalido de entrada')
      metadata['msg'] = str(e)
      
      lista_metadata.append(metadata)
      
print('\nREGISTROS: {0}'.format(len(lic)))

# COMMAND ----------

lista_list = pd.concat(lic, axis=0, ignore_index=True).replace('None','')

# COMMAND ----------

#gera parquet
df = spark.createDataFrame(lista_list[['area','idBoard','idCard','idList','pos','closed','name']])
df.write.mode("Overwrite").parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri,file='lista'))

print('\nREGISTROS: {0}'.format(df.count()))