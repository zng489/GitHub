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
trs = dls['folders']['trusted']
biz = dls['folders']['business']

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

arquivos_tratados_raw = "{adl_path}"+raw+"/crw/{pasta}/{file}.parquet"
arquivos_tratados_trs = "{adl_path}"+trs+"/trello/{file}.parquet"   

print(var_adls_uri)

# COMMAND ----------

#var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"
#raw = 'tmp/dev/raw'
#trs = 'tmp/dev/trs'
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

flat_trello = boards.join(cards, on=['idBoard'], how='left')\
                           .join(juncao_boards_cards_members , on=['idBoard','idCard'], how='left')\
                           .join(member_card , on=['cards_idMember'] , how='left' )\
                           .join(customfields , on=['idBoard','idCard'], how='left')\
                           .join(actions , on=['idBoard','idCard'], how='left')\
                           .join(juncao_boards_cards_checklits , on=['idBoard','idCard'], how='left')\
                           .join(checklist , on=['idBoard','idCard','idChecklist'], how='left')\
                           .join(member_check , on=['checklist_idMember'] , how='left' )\
                           .join(juncao_boards_cards_labels , on=['idBoard','idCard'], how='left')\
                           .join(lista , on=['idBoard','idCard'], how='left')



flat_trello = flat_trello.withColumn('data_text', substring('data_text', 1, 3600))

# COMMAND ----------

# DBTITLE 1,TRELLO - LIMPA
trello_puro = flat_trello.select(
       'idBoard',
       'idCard',
       'checklist_idMember',
       'idChecklist',
       'cards_idMember',
       'area',
       'boards_name',
       'boards_desc',
       'boards_dateLastActivity',
       'starred',
       'shortLink',
       'idOrganization',
       'idEnterprise',
       'boards_closed',
       'idList',
       'cards_name',
       'cards_desc',
       'cards_closed',
       'cards_dateLastActivity',
       'cards_due',
       'cards_position',
       'cards_username',
       'cards_avatarHash',
       'cards_avatarUrl',
       'cards_fullName',
       'cards_initials',
       'idAction',
       'actions_idMemberCreator',
       'actions_type',
       'date',
       'data_text',
       'nameList',
       'state',
       'checklist_id_item',
       'checklist_name',
       'checklist_due',
       'checklist_position',
       'checklist_username',
       'checklist_avatarHash',
       'checklist_avatarUrl',
       'checklist_fullName',
       'checklist_initials',
       'idLabel',
       'labels_name',
       'labels_color',
       'lista_position',
       'lista_closed',
       'lista_name'
).distinct()


trello_puro = trello_puro.withColumn("data_carga",current_date())

# COMMAND ----------

# DBTITLE 1,CUSTOM
custom = ["Responsável Principal","Situação","Produto","Quadro Raiz","Dt Solicitação","Tipo","Solicitante","Dt Previsão de entrega"]

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
  
pivotDF = pivotDF.withColumnRenamed("Responsável Principal","Responsavel_Principal")
pivotDF = pivotDF.withColumnRenamed("Situação","Situacao")
pivotDF = pivotDF.withColumnRenamed("Quadro Raiz","Quadro_Raiz")
pivotDF = pivotDF.withColumnRenamed("Dt Solicitação","Dt_Solicitacao")
pivotDF = pivotDF.withColumnRenamed("Dt Previsão de entrega","Dt_Previsao_de_entrega")

# COMMAND ----------

trello_puro.write.mode("Overwrite").parquet(arquivos_tratados_trs.format(adl_path=var_adls_uri, file='trello_flat'))
#pivotDF.write.mode("Overwrite").parquet(arquivos_tratados_trs.format(adl_path=var_adls_uri, file='trello_flat_customfields_uniepro'))

# COMMAND ----------

print(arquivos_tratados_trs.format(adl_path=var_adls_uri, file='trello_flat'))
#print(arquivos_tratados_trs.format(adl_path=var_adls_uri, file='trello_flat_customfields_uniepro'))

# COMMAND ----------

#DATA_PATH = "adl://cnibigdatadls.azuredatalakestore.net/tmp/dev/trs/trello/trello_flat"
#flat_trello.write.format('csv'). \
#          option('header',True). \
#          mode('overwrite').option('sep',';'). \
#          save(DATA_PATH)

# COMMAND ----------

# DBTITLE 1,GERA MODELO FINAL PARA BANCO
trello_flat = trello_puro
trello_flat_customfields_uniepro = pivotDF

biz_target = "{biz}/{origin}".format(biz=biz, origin=table["path_destination"])
target = "{adl_path}{biz_target}".format(adl_path=var_adls_uri, biz_target=biz_target)

trello_flat.write.parquet(target, mode='overwrite')
#trello_flat_customfields_uniepro.write.parquet(target, mode='overwrite')