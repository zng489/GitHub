# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type truncate/full insert
# MAGIC <pre>
# MAGIC 
# MAGIC Processo	org_raw_rais
# MAGIC Tabela/Arquivo Origem	raw/usr/me/rais_estabelecimento
# MAGIC Tabela/Arquivo Destino	trs/me/rais_estabelecimento
# MAGIC Particionamento Tabela/Arquivo Destino	Ano selecionado no arquivo de origem (ANO) e as duas primeiras posições do campo Municipio (UF)
# MAGIC Descrição Tabela/Arquivo Destino	
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atuaização	N/A
# MAGIC Periodicidade/Horario Execução	Anual depois da disponibilização dos dados na landing zone
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from trs_control_field import trs_control_field as tcf

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# tables =  {
#   "path_origin": "/usr/me/rais_estabelecimento",
#   "path_destination":"/me/rais_estabelecimento"
# }

# #dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#    'adf_factory_name': 'cnibigdatafactory', 
#    'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
#    'adf_pipeline_run_id': 'p1',
#    'adf_trigger_id': 't1',
#    'adf_trigger_name': 'author_dev',
#    'adf_trigger_time': '2020-10-14T15:00:06.0829994Z',
#    'adf_trigger_type': 'Manual'
#  }


# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

# COMMAND ----------

df = df.drop("nr_reg", "nm_arq_in", "dh_arq_in", "kv_process_control", "dh_insercao_raw")

# COMMAND ----------

transformation = [('ID_CEI_VINCULADO','ID_CEI_VINCULADO','long'), ('ID_CEPAO_ESTAB','ID_CEPAO_ESTAB','double'), ('CD_CNAE10_CLASSE','CD_CNAE10_CLASSE','int'), ('ID_CNPJ_CEI','ID_CNPJ_CEI','string'), ('ID_CNPJ_RAIZ','ID_CNPJ_RAIZ','string'), ('DT_ABERTURA','DT_ABERTURA','int'), ('DT_BAIXA','DT_BAIXA','int'), ('DT_ENCERRAMENTO','DT_ENCERRAMENTO','int'), ('NM_EMAIL','NM_EMAIL','string'), ('FL_IND_CEI_VINCULADO','FL_IND_CEI_VINCULADO','int'), ('FL_IND_ESTAB_PARTICIPA_PAT','FL_IND_ESTAB_PARTICIPA_PAT','int'), ('FL_IND_RAIS_NEGAT','FL_IND_RAIS_NEGAT','int'), ('FL_IND_SIMPLES','FL_IND_SIMPLES','int'), ('CD_MUNICIPIO','CD_MUNICIPIO','int'), ('CD_NATUREZA_JURIDICA','CD_NATUREZA_JURIDICA','int'), ('NM_LOGRADOURO','NM_LOGRADOURO','string'), ('NR_LOGRADOURO','NR_LOGRADOURO','double'), ('NM_BAIRRO','NM_BAIRRO','string'), ('NR_TELEFONE_EMPRE','NR_TELEFONE_EMPRE','long'), ('QT_VINC_ATIV','QT_VINC_ATIV','int'), ('QT_VINC_CLT','QT_VINC_CLT','int'), ('QT_VINC_ESTAT','QT_VINC_ESTAT','int'), ('ID_RAZAO_SOCIAL','ID_RAZAO_SOCIAL','string'), ('CD_TAMANHO_ESTABELECIMENTO','CD_TAMANHO_ESTABELECIMENTO','int'), ('CD_TIPO_ESTAB_ID','CD_TIPO_ESTAB_ID','int'), ('CD_IBGE_SUBSETOR','CD_IBGE_SUBSETOR','int'), ('FL_IND_ATIV_ANO','FL_IND_ATIV_ANO','int'), ('CD_CNAE20_CLASSE','CD_CNAE20_CLASSE','int'), ('CD_CNAE20_SUBCLASSE','CD_CNAE20_SUBCLASSE','int'), ('CD_UF','CD_UF','int')]

# COMMAND ----------

for column, renamed, _type in transformation:
  df = df.withColumn(column, col(column).cast(_type))
  df = df.withColumnRenamed(column, renamed)

# COMMAND ----------

df = tcf.add_control_fields(df, adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('ANO').save(path=target, format="parquet", mode='overwrite')