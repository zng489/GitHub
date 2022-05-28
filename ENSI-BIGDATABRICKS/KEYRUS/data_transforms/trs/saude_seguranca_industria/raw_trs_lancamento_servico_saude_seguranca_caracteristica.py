# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type update
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_lancamento_servico_saude_seguranca_caracteristica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi
# MAGIC Tabela/Arquivo Destino	/trs/evt/lancamento_servico_saude_seguranca_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	
# MAGIC Descrição Tabela/Arquivo Destino	Cadastro das caracteristicas dos lançamentos de serviços de saúde e segurança do trabalhador.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Não se aplica
# MAGIC Periodicidade/Horario Execução	Diária, após a carga da raw inddesempenho
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
var_tables =   {"origins": ["/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi"],"destination": "/evt/lancamento_servico_saude_seguranca_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_lancamento_servico_saude_seguranca_caracteristica',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
      
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_ssi = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_ssi)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, max, current_timestamp, from_utc_timestamp, col, when, udf, array, lag, from_json
from pyspark.sql import Window
from pyspark.sql.types import StringType
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from lancamento_servico_saude_seguranca_caracteristica  */
# MAGIC /* Ler vw_lanc_evento_vl_r_ssi da raw (regitros mais recente) com as regras para carga incremental, execucao diária, query abaixo:*/  
# MAGIC /* Inserir caso o id_filtro_lancamento_oltp não existir. Alterar somente se algum campo da tabela trs diferente da raw lida, para o id_filtro_lancamento_oltp existente. 
# MAGIC select    distinct
# MAGIC 	 vw_lanc_evento_vl_r_ssi.id_lancamento_evento
# MAGIC 	,vw_lanc_evento_vl_r_ssi.id_carga_horaria
# MAGIC 	,vw_lanc_evento_vl_r_ssi.id_modalidade
# MAGIC 	,vw_lanc_evento_vl_r_ssi.id_serie
# MAGIC 	,vw_lanc_evento_vl_r_ssi.id_projeto
# MAGIC FROM 
# MAGIC vw_lanc_evento_vl_r_ssi
# MAGIC WHERE vw_lanc_evento_vl_r_ssi.dh_insercao_raw  > #var_max_dh_ultima_atualizacao_oltp
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
# MAGIC ### Reading raw and Applying filters

# COMMAND ----------

var_useful_columns_vw_lanc_evento_vl_r_ssi = ["id_filtro_lancamento", "id_carga_horaria", "id_modalidade", "id_serie", "id_projeto", "fl_excluido", "dh_insercao_raw", "cd_mes", "cd_ciclo_ano"]

# COMMAND ----------

df_raw = spark.read.parquet(src_ssi)\
.select(*var_useful_columns_vw_lanc_evento_vl_r_ssi)\
.filter(col("dh_insercao_raw") > var_max_dh_ultima_atualizacao_oltp)\
.distinct()

# COMMAND ----------

#USE THIS ONLY IN DEV FOR THE PURPOSE OF TESTING UPDATES
#df_raw = df_raw.filter(col("dh_insercao_raw") < datetime.date(2020, 5, 23))

# COMMAND ----------

#df_raw.count()

# COMMAND ----------

if df_raw.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define new columns

# COMMAND ----------

df_raw = df_raw\
.withColumn("cd_mes_referencia", col("cd_mes") + 1 )\
.drop("cd_mes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and change types

# COMMAND ----------

var_mapping = {"id_filtro_lancamento": {"name": "id_filtro_lancamento_oltp", "type": "long"},
               "id_carga_horaria": {"name": "id_carga_horaria", "type": "int"},
               "id_modalidade": {"name": "id_modalidade", "type": "int"},
               "id_serie": {"name": "id_serie", "type": "int"},   
               "id_projeto": {"name": "id_projeto", "type": "int"},      
               "fl_excluido": {"name": "fl_excluido_oltp", "type": "int"},
               "dh_insercao_raw": {"name": "dh_ultima_atualizacao_oltp", "type": "timestamp"},
               "cd_ciclo_ano": {"name": "cd_ano_referencia", "type": "int"},
               "cd_mes_referencia": {"name": "cd_mes_referencia", "type": "int"},
              }

for cl in var_mapping:
  df_raw = df_raw.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
df_raw = tcf.add_control_fields(df_raw, var_adf)

# COMMAND ----------

#display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 

# COMMAND ----------

if first_load == True:
  df_trs = df_raw
  
elif first_load == False:
  df_trs = df_trs.union(df_raw.select(df_trs.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Check if there's some difference between records for each id_filtro_lancamento_oltp. If there are not any differences, keep the oldest record

# COMMAND ----------

id_columns = ['id_filtro_lancamento_oltp', 'id_carga_horaria', 'id_modalidade', 'id_serie', 'id_projeto', 'fl_excluido_oltp', 'cd_ano_referencia', 'cd_mes_referencia']

# COMMAND ----------

concat_udf = udf(lambda cols: "".join([str(x).strip() + "|" if x is not None else "|" for x in cols]), StringType())

# COMMAND ----------

w = Window.partitionBy("id_filtro_lancamento_oltp").orderBy(col("dh_ultima_atualizacao_oltp").asc())
df_trs = df_trs.withColumn("unique_id", concat_udf(array(*id_columns)))\
.withColumn("changed", (col("unique_id") != lag('unique_id', 1, 0).over(w)).cast("int"))\
.filter(col("changed") == 1)\
.drop("unique_id", "changed")

# COMMAND ----------

#df_trs.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure we get only the last record for key id_filtro_lancamento_oltp

# COMMAND ----------

w = Window.partitionBy("id_filtro_lancamento_oltp").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", row_number().over(w))\
.filter(col("rank") == 1)\
.drop("rank")

# COMMAND ----------

#var_num_records = df_trs.count()
#var_num_records

# COMMAND ----------

#var_num_records == df_trs.select("id_filtro_lancamento_oltp").distinct().count()

# COMMAND ----------

del df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 3 files without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.partitionBy("cd_ano_referencia", "cd_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

