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
# MAGIC Processo	raw_trs_valor_lancamento_ssi_pessoa_beneficiada
# MAGIC Tabela/Arquivo Origem	/raw/bdo/inddesempenho/vw_lancamento_evento_valor_realizado_ssi /raw/bdo/inddesempenho/valorlanc_pessoaf /raw/bdo/inddesempenho/pessoafisica
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/valor_lancamento_ssi_pessoa_beneficiada
# MAGIC Particionamento Tabela/Arquivo Destino	
# MAGIC Descrição Tabela/Arquivo Destino	Cadastro das pessoas beneficiadas relacionados ao valor do lancamento dos serviços de saúde e segurança do trabalhador.
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
var_tables = {"origins": ["/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi", "/bdo/inddesempenho/valorlanc_pessoaf", "/bdo/inddesempenho/pessoafisica"],"destination": "/mtd/sesi/valor_lancamento_ssi_pessoa_beneficiada"}

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
       'adf_pipeline_name': 'raw_trs_valor_lancamento_ssi_pessoa_beneficiada',
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

src_ssi, src_vp, src_pf = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_ssi, src_vp, src_pf)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, max, current_timestamp, from_utc_timestamp, col, when, udf, array, lag, from_json, desc, asc, coalesce
from pyspark.sql import Window
from pyspark.sql.types import StringType
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC NEW
# MAGIC 
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp)       */
# MAGIC /*                                                                                  from valor_lancamento_ssi_pessoa_beneficiada */
# MAGIC /* Ler conjuntamente vw_lanc_evento_vl_r_ssi, valorlanc_pessoaf e pessoafisica da raw (regitros mais recente de ambas)           */
# MAGIC /* com as regras para carga incremental, execucao diária, query abaixo:                                                          */
# MAGIC /* Caso id_valor_lancamento_ssi_pessoa_fisica_oltp não existir inserir o registro 						 */
# MAGIC /* Caso id_valor_lancamento_ssi_pessoa_fisica_oltp existir e os campos cd_cpf_pessoa ou fl_excluido_oltp iguais, desprezar registro   */
# MAGIC /* Caso id_valor_lancamento_ssi_pessoa_fisica_oltp existir e os campos cd_cpf_pessoa ou fl_excluido_oltp diferentes:                  */
# MAGIC /*   Atualizar os campos correspondentes e atualizar as datas de alteração e exclusão (caso o fl_excluido_oltp = 1                    */
# MAGIC 
# MAGIC select 
# MAGIC       valorlanc_pessoaf.id			  as id_valor_lancamento_ssi_pessoa_fisica_oltp
# MAGIC      ,vw_lanc_evento_vl_r_ssi.id_valor_lancamento as id_valor_lancamento_oltp
# MAGIC      ,vw_lanc_evento_vl_r_ssi.cd_mes              as cd_mes
# MAGIC      ,vw_lanc_evento_vl_r_ssi.cd_ciclo_ano        as cd_ciclo_ano
# MAGIC      ,valorlanc_pessoaf.pf_id		          as id_pessoa_beneficiada_oltp
# MAGIC      ,pessoafisica.cpf			   	  as cd_cpf_pessoa
# MAGIC      ,valorlanc_pessoaf.fl_excluido               as fl_excluido_oltp		
# MAGIC      ,ROW_NUMBER() OVER (PARTITION BY valorlanc_pessoaf.id ORDER BY dh_insercao_raw DESC) as SQ
# MAGIC      ,timestamp as dh_insercao_trs
# MAGIC FROM 
# MAGIC vw_lanc_evento_vl_r_ssi vw
# MAGIC      INNER JOIN
# MAGIC valorlanc_pessoaf  ON valorlanc_pessoaf.valor_id = vw_lancamento_evento_valor_realizado_ssi.id_valor_lancamento
# MAGIC      INNER JOIN 
# MAGIC pessoafisica      ON pessoafisica.id = valorlanc_pessoaf.pf_id
# MAGIC           LEFT JOIN 
# MAGIC valor_lancamento_ssi_pessoa_beneficiada ON valor_lancamento_ssi_pessoa_beneficiada.id_valor_lancamento_ssi_pessoa_fisica_oltp = valorlanc_pessoaf.id
# MAGIC WHERE valorlanc_pessoaf.dh_insercao_raw  > #var_max_dh_ultima_atualizacao_oltp
# MAGIC and SQ = 1
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

var_useful_columns_vw_lanc_evento_vl_r_ssi = ["id_valor_lancamento", "cd_mes", "cd_ciclo_ano"]

df_ssi = spark.read.parquet(src_ssi).select(*var_useful_columns_vw_lanc_evento_vl_r_ssi)

# COMMAND ----------

var_useful_columns_valorlanc_pessoaf = ["id", "pf_id", "fl_excluido", "dh_insercao_raw", "valor_id"]

df_vp = spark.read.parquet(src_vp)\
.select(*var_useful_columns_valorlanc_pessoaf)\
.filter(col("dh_insercao_raw") > var_max_dh_ultima_atualizacao_oltp)\
.withColumnRenamed("valor_id", "id_valor_lancamento")

# COMMAND ----------

#USE THIS ONLY IN DEV FOR THE PURPOSE OF TESTING UPDATES
#df_vp = df_vp.filter(col("dh_insercao_raw") < datetime.date(2020, 8, 1))

# COMMAND ----------

if df_vp.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

var_useful_columns_pessoa_fisica = ["id", "cpf"]

df_pf = spark.read.parquet(src_pf)\
.select(*var_useful_columns_pessoa_fisica)\
.withColumnRenamed("id", "pf_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join tables

# COMMAND ----------

new_records = df_ssi\
.join(df_vp, 
      ["id_valor_lancamento"], 
      "inner")\
.join(df_pf, 
      ["pf_id"], 
      "inner")\
.distinct()

# COMMAND ----------

del df_ssi, df_vp, df_pf

# COMMAND ----------

#new_records.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and change types

# COMMAND ----------

new_records = new_records\
.withColumn("cd_mes_referencia", col("cd_mes") + 1 )\
.drop("cd_mes")

# COMMAND ----------

var_mapping = {"id": {"name": "id_valor_lancamento_ssi_pessoa_fisica_oltp", "type": "long"},
               "id_valor_lancamento": {"name": "id_valor_lancamento_oltp", "type": "long"},
               "pf_id": {"name": "id_pessoa_beneficiada_oltp", "type": "long"},
               "cpf": {"name": "cd_cpf_pessoa", "type": "long"},   
               "fl_excluido": {"name": "fl_excluido_oltp", "type": "int"},      
               "dh_insercao_raw": {"name": "dh_ultima_atualizacao_oltp", "type": "timestamp"},
               "cd_ciclo_ano": {"name": "cd_ano_referencia", "type": "int"},
               "cd_mes_referencia": {"name": "cd_mes_referencia", "type": "int"},
              }

for cl in var_mapping:
  new_records = new_records.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

#Just equalizing schemas
new_records = new_records\
.withColumn("dh_inclusao_valor_pessoa", lit(None).cast("timestamp"))\
.withColumn("dh_exclusao_valor_pessoa", lit(None).cast("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
new_records = tcf.add_control_fields(new_records, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update 
# MAGIC Get newest and oldest record and attach the dh_ultima_atualizacao_oltp of the oldest record to dh_inclusao_valor_pessoa of the newest record

# COMMAND ----------

if first_load == True:
  df = new_records
  
elif first_load == False:
  df = df_trs.union(new_records.select(df_trs.columns))

# COMMAND ----------

w_desc = Window.partitionBy("id_valor_lancamento_ssi_pessoa_fisica_oltp").orderBy(col("dh_ultima_atualizacao_oltp").desc(), col("fl_excluido_oltp").asc()) #get newest with lower fl_excluido_oltp 
w_asc = Window.partitionBy("id_valor_lancamento_ssi_pessoa_fisica_oltp").orderBy(col("dh_ultima_atualizacao_oltp").asc(), col("fl_excluido_oltp").asc()) #get oldest with lower fl_excluido_oltp
w_lag = Window.partitionBy("id_valor_lancamento_ssi_pessoa_fisica_oltp").orderBy(col("dh_ultima_atualizacao_oltp").asc()) #lag dh_ultima_atualizacao_oltp to the next record
w_max = Window.partitionBy("id_valor_lancamento_ssi_pessoa_fisica_oltp") #get max dh_inclusao_valor_pessoa

df = df\
.withColumn("row_number_desc", row_number().over(w_desc))\
.withColumn("row_number_asc", row_number().over(w_asc))\
.filter((col("row_number_desc") == 1) | (col("row_number_asc") == 1))\
.withColumn("max_dh_inclusao", max("dh_inclusao_valor_pessoa").over(w_max))\
.withColumn("lag_dh_ultima_atualizacao_oltp", lag(col("dh_ultima_atualizacao_oltp"), 1).over(w_lag))\
.withColumn("dh_inclusao_valor_pessoa", when(col("max_dh_inclusao").isNotNull(), col("max_dh_inclusao"))\
                                       .when((col("max_dh_inclusao").isNull()) & (col("lag_dh_ultima_atualizacao_oltp").isNotNull()),col("lag_dh_ultima_atualizacao_oltp"))\
                                       .otherwise(col("dh_ultima_atualizacao_oltp")))\
.withColumn("dh_exclusao_valor_pessoa", when(col("fl_excluido_oltp") == 1, col("dh_ultima_atualizacao_oltp")).otherwise(lit(None)))\
.filter(col("row_number_desc") == 1)\
.drop("lag_dh_ultima_atualizacao_oltp", "row_number_desc", "row_number_asc", "max_dh_inclusao")

# COMMAND ----------

#df.count()

# COMMAND ----------

#display(df)

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC We can keep as 3 files for each partition without worries. 

# COMMAND ----------

df.coalesce(3).write.partitionBy("cd_ano_referencia", "cd_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

