# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type versioning (insert + update)
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_atendimento_tecnologia_inovacao_parceiro
# MAGIC Tabela/Arquivo Origem	 /raw/bdo/bd_basi/tb_parceiro_atendimento /raw/bdo/bd_basi/tb_entidade_regional
# MAGIC Tabela/Arquivo Destino	/trs/evt/atendimento_tecnologia_inovacao_parceiro
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 484 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Corresponde às entidades parceiras no atendimento e seu percentual de participação no serviço de tecnologia e inovação. Estaremos tratando somente a Entidade Nacional SENAI
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por cd_atendimento_sti cd_entidade_regional_parceiro, cd_ano_atendimento_sti, cd_mes_atendimento_sti, que insere ou sobrescreve o registro caso a chave cd_atendimento_sti cd_entidade_regional_parceiro, cd_ano_atendimento_sti, cd_mes_atendimento_sti seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import json
import re

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_parceiro_atendimento","/bdo/bd_basi/tb_entidade_regional"],"destination": "/evt/atendimento_tecnologia_inovacao_parceiro"}

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/tmp/dev/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_trs_atendimento_tecnologia_inovacao_parceiro',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-27T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_tbp, src_tbe = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tbp, src_tbe)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, year, month, max, current_timestamp, from_utc_timestamp, col
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from atendimento_tecnologia_inovacao_parceiro
# MAGIC Ler tb_parceiro_atendimento da raw com as regras para carga incremental, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC SELECT  PAR.CD_ATENDIMENTO, DR.CD_ENTIDADE_REGIONAL
# MAGIC       , PAR.VL_PREVISTO
# MAGIC       , PAR.DT_ATUALIZACAO
# MAGIC      FROM TB_PARCEIRO_ATENDIMENTO PAR
# MAGIC      INNER JOIN TB_ENTIDADE_REGIONAL DR ON (PAR.CD_PESSOA=DR.CD_PESSOA)
# MAGIC     WHERE  DR.CD_ENTIDADE_NACIONAL = 3 AND PAR.dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp)
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get max date of dh_ultima_atualizacao_oltp from last load

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  df_trs = spark.read.parquet(sink)
  var_max_dh_ultima_atualizacao_oltp = df_trs.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)
print("Max dt inicio vigencia", var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying filters

# COMMAND ----------

var_useful_columns_par = ["cd_pessoa", "cd_atendimento", "vl_previsto", "dt_atualizacao"]

df_par = spark.read.parquet(src_tbp)\
.select(*var_useful_columns_par)\
.filter(col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

var_useful_columns_dr = ["cd_entidade_regional", "cd_pessoa", "cd_entidade_nacional"]

df_dr = spark.read.parquet(src_tbe)\
.select(*var_useful_columns_dr)\
.filter(col("cd_entidade_nacional") == 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining tb_atendimento_parceiro and tb_entidade_regional

# COMMAND ----------

df_raw = df_par.join(df_dr, ["cd_pessoa"], "inner").select("cd_atendimento", "cd_entidade_regional", "vl_previsto", "dt_atualizacao")

# COMMAND ----------

del df_par
del df_dr

# COMMAND ----------

#df_raw.count()

# COMMAND ----------

# If there's no new data, then just let it die.
if df_raw.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and change types

# COMMAND ----------

column_name_mapping = {"cd_entidade_regional":"cd_entidade_regional_parceiro", \
                       "cd_atendimento":"cd_atendimento_sti", \
                       "vl_previsto":"qt_hora_entidade_parceira", \
                       "dt_atualizacao":"dh_ultima_atualizacao_oltp"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

column_type_map = {"cd_entidade_regional_parceiro" : "int", \
                   "cd_atendimento_sti" : "int", \
                   "qt_hora_entidade_parceira" : "int", \
                   "dh_ultima_atualizacao_oltp": "timestamp"} 

for c in column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
df_raw = tcf.add_control_fields(df_raw, var_adf)

# COMMAND ----------

#var_num_records = df_raw.count()
#var_num_records

# COMMAND ----------

#var_num_records == df_raw.select("cd_atendimento_sti", "cd_entidade_regional_parceiro").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 

# COMMAND ----------

if first_load == True:
  df_trs = df_raw
  
elif first_load == False:
  df_trs = df_trs.union(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure we get only the last record for key cd_atendimento_sti, cd_entidade_regional_parceiro

# COMMAND ----------

w = Window.partitionBy("cd_atendimento_sti", "cd_entidade_regional_parceiro").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

#df_trs.count()

# COMMAND ----------

del df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

