# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import json
import re
import os
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.functions import lower, col

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
trs = dls['folders']['trusted']
biz = dls['folders']['business']
path_destination = table["path_destination"]
area = table["area"]

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

arquivos_tratados_raw = "{adl_path}"+raw+"/crw/{pasta}/{file}.parquet"
arquivos_tratados_trs = "{adl_path}"+trs+"/trello/{file}.parquet"   

print(var_adls_uri)

# COMMAND ----------

#var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
#raw = 'tmp/dev/raw'
#trs = 'tmp/dev/trs'
#biz = 'tmp/dev/biz'
#path_destination = 'trello/trell_flat_customfield_sti'
#area = 'GEMAS'

#arquivos_tratados_raw = "{adl_path}"+raw+"/crw/{pasta}/{file}.parquet"
#arquivos_tratados_trs = "{adl_path}"+trs+"/trello/{file}.parquet" 
#dbutils.fs.ls('adl://cnibigdatadls.azuredatalakestore.net/tmp/dev/raw/crw/')

# COMMAND ----------

boards       = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__board' ,     file = 'boards')) 
cards        = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__card' ,      file = 'cards'))     
customfields = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__card' ,      file = 'customfields'))
actions      = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__card' ,      file = 'actions'))
members      = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__member' ,    file = 'members'))
checklist    = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__checklist' , file = 'checklist')) 
lista        = spark.read.parquet(arquivos_tratados_raw.format( adl_path=var_adls_uri, pasta ='trello__card'      , file = 'lista')) 

#joins
juncao_boards_cards_members    = spark.read.parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri, pasta ='trello__card' , file='juncao_boards_cards_members'))  
juncao_boards_cards_checklits  = spark.read.parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri, pasta ='trello__card' , file='juncao_boards_cards_checklits')) 
juncao_boards_cards_labels     = spark.read.parquet(arquivos_tratados_raw.format(adl_path=var_adls_uri, pasta ='trello__card' , file='juncao_boards_cards_labels')) 

#RENOMEIA COLUNAS
boards = boards.withColumnRenamed("closed","boards_closed")
boards = boards.withColumnRenamed("name","boards_name")
boards = boards.withColumnRenamed("desc","boards_desc")
boards = boards.withColumnRenamed("dateLastActivity","boards_dateLastActivity")
boards = boards.withColumn('boards_desc', substring('boards_desc', 1, 3600))

cards = cards.withColumnRenamed("closed","cards_closed")
cards = cards.withColumnRenamed("name","cards_name")
cards = cards.withColumnRenamed("desc","cards_desc")
cards = cards.withColumnRenamed("dateLastActivity","cards_dateLastActivity")
cards = cards.withColumnRenamed("due","cards_due")
cards = cards.withColumnRenamed("pos","cards_position")
cards = cards.withColumn('cards_desc', substring('cards_desc', 1, 3600))
cards = cards.drop("area")

customfields = customfields.withColumnRenamed("type","customfields_type")
customfields = customfields.withColumnRenamed("name","customfields_name")
customfields = customfields.withColumnRenamed("value","customfields_value")

customfields = customfields.drop("area","area")

actions = actions.withColumnRenamed("type","actions_type")
actions = actions.withColumnRenamed("idMemberCreator","actions_idMemberCreator")
actions = actions.drop("area")

checklist = checklist.withColumnRenamed("name","checklist_name")
checklist = checklist.withColumnRenamed("due","checklist_due")
checklist = checklist.withColumnRenamed("pos","checklist_position")
checklist = checklist.withColumnRenamed("idMember","checklist_idMember")
checklist = checklist.withColumnRenamed("id","checklist_id_item")
checklist = checklist.drop("nameData")

members   = members.drop("nonPublicAvailable").drop("activityBlocked").drop("idMemberReferrer")
members   = members.drop("area")

member_card = members
member_card = member_card.withColumnRenamed("idMember","cards_idMember")
member_card = member_card.withColumnRenamed("username","cards_username")
member_card = member_card.withColumnRenamed("avatarHash","cards_avatarHash")
member_card = member_card.withColumnRenamed("avatarUrl","cards_avatarUrl")
member_card = member_card.withColumnRenamed("fullName","cards_fullName")
member_card = member_card.withColumnRenamed("initials","cards_initials")

member_check = members
member_check = member_check.withColumnRenamed("idMember","checklist_idMember")
member_check = member_check.withColumnRenamed("username","checklist_username")
member_check = member_check.withColumnRenamed("avatarHash","checklist_avatarHash")
member_check = member_check.withColumnRenamed("avatarUrl","checklist_avatarUrl")
member_check = member_check.withColumnRenamed("fullName","checklist_fullName")
member_check = member_check.withColumnRenamed("initials","checklist_initials")

juncao_boards_cards_members = juncao_boards_cards_members.withColumnRenamed("idMember","cards_idMember")

lista  = lista.withColumnRenamed("name","lista_name")
lista  = lista.withColumnRenamed("closed","lista_closed")
lista  = lista.withColumnRenamed("pos","lista_position")
lista  = lista.drop("idList")
lista  = lista.drop("area")

# COMMAND ----------

# DBTITLE 1,AJUSTA CHECKLIST
boards.createOrReplaceTempView("TRELLO_BOARD")
checklist.createOrReplaceTempView("TRELLO_CHECK")

checklist  = spark.sql("""
select c.*
 from TRELLO_CHECK c
INNER JOIN  TRELLO_BOARD b on b.area = c.area and b.idBoard =  c.idBoard  and b.shortLink = c.shortLink 
""")

checklist = checklist.drop("area")
checklist = checklist.drop("shortLink")

# COMMAND ----------

customfields = customfields.withColumn("customfields_name", f.lower( f.col("customfields_name") ))
customfields = customfields.withColumn("customfields_name", regexp_replace('customfields_name', '\(', ''))
customfields = customfields.withColumn("customfields_name", regexp_replace('customfields_name', '\)', ''))
customfields = customfields.withColumn("customfields_name",regexp_replace(col("customfields_name"), "/[^0-9]+/", ""))
customfields = customfields.withColumn("customfields_name", regexp_replace('customfields_name', ' ', '_'))
customfields = customfields.withColumn("customfields_name", regexp_replace('customfields_name','-', ''))

# COMMAND ----------

#display(customfields.select("customfields_name").distinct())

# COMMAND ----------

flat_trello = boards.join(cards, on=['idBoard'], how='left')\
                           .join(juncao_boards_cards_members , on=['idBoard','idCard'], how='left')\
                           .join(member_card , on=['cards_idMember'] , how='left' )\
                           .join(customfields , on=['idBoard','idCard'], how='left')

#flat_trello = flat_trello.withColumn('data_text', substring('data_text', 1, 3600))

# COMMAND ----------

# DBTITLE 1,TRELLO - LIMPA
trello_puro = flat_trello.select(
       'idBoard',
       'idCard',
       'area',
       'idOrganization',
       'idCustomField',
       'customfields_name',
       'customfields_value'
).filter("area == '"+area+"'").distinct()

# COMMAND ----------

customfields_name = trello_puro.select("customfields_name").orderBy(f.col("customfields_name").asc()).distinct().filter("customfields_name != 'None'").filter("area == '"+area+"'")
print(area)

# COMMAND ----------

display(customfields_name)

# COMMAND ----------

#alimeta custom fields no array para pivot
#custom = ["Responsável Principal","Situação","Produto","Quadro Raiz","Dt Solicitação","Tipo","Solicitante","Dt Previsão de entrega"]
custom = []
for row in customfields_name.rdd.collect():
  custom.append( row.customfields_name )

# COMMAND ----------

print( custom) 

# COMMAND ----------

# DBTITLE 1,CUSTOM
trello_custom = flat_trello.select(
       'idBoard',
       'idCard',
       'area',
       'idOrganization',
       'idCustomField',
       'customfields_name',
       'customfields_value'
).distinct()


pivotDF = trello_custom.groupBy(
       'area',
       'idOrganization',
       'idBoard',
       'idCard'
).pivot("customfields_name", custom).agg(first("customfields_value"))

# COMMAND ----------

from functools import reduce

def colrename(x):
  coluna_ = x
  coluna_ = coluna_.lower().replace(' ', '_').replace('-', '').replace('ç', 'c').replace('ó', 'o').replace('ã', 'a').replace('é', 'e').replace('á', 'a').replace('(', '').replace(')', '').replace('[', '').replace(']', '').replace(',', '').replace(';', '').replace('?', '').replace(':', '').replace('.', '').replace('/', '').replace('ê','e')
  
  return coluna_

for i in pivotDF.schema.names: #LISTA COLUNAS DO DF
  pivotDF = pivotDF.withColumnRenamed(i,colrename(i))
  
pivotDF.printSchema()

# COMMAND ----------

filename = 'trello_flat_customfields_'+area
filename = filename.lower()
print(filename)

pivotDF.write.mode("Overwrite").parquet(arquivos_tratados_trs.format(adl_path=var_adls_uri, file=filename))

# COMMAND ----------

print(arquivos_tratados_trs.format(adl_path=var_adls_uri, file=filename))

# COMMAND ----------

# DBTITLE 1,GERA MODELO FINAL PARA BANCO
trello_flat_customfields_area = pivotDF

biz_target = "{biz}/{origin}".format(biz=biz, origin=path_destination)
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)

trello_flat_customfields_area.write.parquet(target, mode='overwrite')