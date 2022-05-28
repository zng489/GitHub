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
# MAGIC Processo	raw_trs_curso_ensino_profissional_oferta
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_curso
# MAGIC Tabela/Arquivo Destino	/trs/mtd/senai/curso_ensino_profissional_oferta
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 63.383 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Corresponde à oferta em datas especificadas dos cursos em Educação do SENAI.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave id_curso_ensino_profissional + dt_inicio_oferta seja encontrada.
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
var_tables =  {"origins": ["/bdo/bd_basi/tb_curso"], "destination": "/mtd/senai/curso_ensino_profissional_oferta"}

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
       'adf_pipeline_name': 'raw_trs_curso_ensino_profissional_oferta',
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

src_tb = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_tb)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import dense_rank, lit, year, max, current_timestamp, from_utc_timestamp, col, trim, row_number
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from curso_ensino_profissional_oferta
# MAGIC Ler tb_curso da raw com as regras para carga incremental, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC SELECT 
# MAGIC cd_curso, dt_inicio_oferta, dt_fim_oferta, nr_carga_horaria, nr_carga_horaria_estagio, dt_atualizacao
# MAGIC FROM tb_curso
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
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
# MAGIC ### Filter and change types

# COMMAND ----------

useful_columns = ["cd_curso", "dt_inicio_oferta", "dt_fim_oferta", "nr_carga_horaria", "nr_carga_horaria_estagio", "dt_atualizacao"]
df_raw = spark.read.parquet(src_tb)\
.select(*useful_columns)\
.filter(col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# If there's no new data, then just let it die.
if df_raw.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Get only last record by cd_curso and dt_inicio_oferta
# MAGIC </pre>

# COMMAND ----------

w = Window.partitionBy("cd_curso", "dt_inicio_oferta").orderBy(col("dt_atualizacao").desc())
df_raw = df_raw.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and change types

# COMMAND ----------

column_name_mapping = {"cd_curso":"id_curso_ensino_profissional", 
                       "nr_carga_horaria":"qt_hr_carga_aula", 
                       "nr_carga_horaria_estagio":"qt_hr_carga_estagio",
                       "dt_atualizacao": "dh_ultima_atualizacao_oltp"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

column_type_map = {"id_curso_ensino_profissional" : "int", 
                   "dt_inicio_oferta" : "date",
                   "dt_fim_oferta" : "date", 
                   "qt_hr_carga_aula": "int", 
                   "qt_hr_carga_estagio": "int",
                   "dh_ultima_atualizacao_oltp": "timestamp"}

for c in column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
df_raw = tcf.add_control_fields(df_raw, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 

# COMMAND ----------

if first_load == True:
  df_trs = df_raw
  
elif first_load == False:
  df_trs = df_trs.union(df_raw)

# COMMAND ----------

del df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only last refresh date by id_curso_ensino_profissional and dt_inicio_oferta

# COMMAND ----------

w = Window.partitionBy("id_curso_ensino_profissional", "dt_inicio_oferta").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_trs.unpersist()

# COMMAND ----------

