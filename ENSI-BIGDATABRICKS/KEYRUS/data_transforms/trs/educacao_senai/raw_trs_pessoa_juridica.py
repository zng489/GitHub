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
# MAGIC Processo	raw_trs_pessoa_juridica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_pessoa_juridica
# MAGIC Tabela/Arquivo Destino	/trs/mtd/senai/pessoa_juridica
# MAGIC Particionamento Tabela/Arquivo Destino	TBD
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações de identificação (ESTÁVEIS) de pessoa jurídica.  OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_pessoa seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import json
import re
import datetime

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_pessoa_juridica", "/bdo/bd_basi/tb_pessoa_juridica_estrangeira"],"destination": "/mtd/senai/pessoa_juridica"}

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
       'adf_pipeline_name': 'raw_trs_pessoa_juridica',
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

src_tpj, src_tpje = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tpj, src_tpje)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import dense_rank, lit, year, max, current_timestamp, from_utc_timestamp, col, trim, from_json
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from pessoa_juridica
# MAGIC Ler tb_pessoa_juridica da raw com as regras para carga incremental, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC SELECT
# MAGIC cd_pessoa, nr_cnpj, nm_razao_social, cd_natureza_juridica, cd_nomenclatura_mercosul, nr_cnpj_dv, nr_cnpj_estabelecimento, nr_cnpj_radical, dt_abertura, dt_encerramento,
# MAGIC nm_fantasia, ds_codigo_empresa_estrangeira,
# MAGIC dt_atualizacao 
# MAGIC FROM tb_pessoa_juridica
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC +
# MAGIC SELECT cd_pessoa, nm_razao_social, nm_fantasia, nr_documento_estrangeiro, pai_sg
# MAGIC FROM tb_pessoa_juridica_estrangeira
# MAGIC 
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
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)
print("Max dt inicio vigencia", var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading raw and filter and change types

# COMMAND ----------

useful_columns_tb_pessoa_juridica = ["cd_pessoa", "nr_cnpj", "nm_razao_social", "cd_natureza_juridica", "cd_nomenclatura_mercosul", "nr_cnpj_dv", "nr_cnpj_estabelecimento", "nr_cnpj_radical", "dt_abertura", "dt_encerramento", "nm_fantasia", "ds_codigo_empresa_estrangeira", "dt_atualizacao"]

df_pj = spark.read.parquet(src_tpj)\
.select(*useful_columns_tb_pessoa_juridica)\
.filter(col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

useful_columns_tb_pessoa_juridica_estrangeira = ["cd_pessoa", "nm_razao_social", "nm_fantasia", "nr_documento_estrangeiro", "pai_sg"]

df_pje = spark.read.parquet(src_tpje)\
.select(*useful_columns_tb_pessoa_juridica_estrangeira)

# COMMAND ----------

# MAGIC %md
# MAGIC Union tb_pessoa_jurifica with tb_pessoa_juridica_estrangeira

# COMMAND ----------

df_pj = df_pj.withColumn("pai_sg", lit("BR").cast("string"))

# COMMAND ----------

df_pje = df_pje\
.withColumnRenamed("nr_documento_estrangeiro", "ds_codigo_empresa_estrangeira")\
.withColumn("dt_atualizacao", lit(var_max_dh_ultima_atualizacao_oltp).cast("timestamp"))

# COMMAND ----------

var_column_type_map = {"nr_cnpj" : "string", 
                       "cd_natureza_juridica" : "string",
                       "cd_nomenclatura_mercosul" : "decimal(38,18)",
                       "nr_cnpj_dv" : "string",
                       "nr_cnpj_estabelecimento" : "string",
                       "nr_cnpj_radical" : "string",
                       "dt_abertura" : "timestamp",
                       "dt_encerramento" : "timestamp",                               
                      }

for c in var_column_type_map:
  df_pje = df_pje.withColumn(c, lit(None).cast(var_column_type_map[c]))

# COMMAND ----------

df_raw = df_pj.union(df_pje.select(df_pj.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming and chaning types

# COMMAND ----------

column_name_mapping = {"cd_pessoa":"cd_pessoa_juridica", 
                       "nr_cnpj":"cd_cnpj",
                       "nm_razao_social": "nm_razao_social",
                       "cd_natureza_juridica": "cd_natureza_juridica",
                       "cd_nomenclatura_mercosul": "cd_nomenclatura_mercosul",
                       "nr_cnpj_dv":"cd_cnpj_dv",
                       "nr_cnpj_estabelecimento":"cd_cnpj_estabelecimento",
                       "nr_cnpj_radical":"cd_cnpj_radical",
                       "dt_abertura": "dt_abertura",
                       "dt_encerramento": "dt_encerramento",
                       "nm_fantasia": "nm_fantasia",
                       "ds_codigo_empresa_estrangeira": "cd_documento_estrangeiro",
                       "dt_atualizacao": "dh_ultima_atualizacao_oltp",
                       "pai_sg": "sg_pais"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

var_column_type_map = {"cd_pessoa_juridica": "int", 
                       "cd_cnpj": "string", 
                       "nm_razao_social": "string", 
                       "cd_natureza_juridica": "int", 
                       "cd_nomenclatura_mercosul": "int", 
                       "cd_cnpj_dv": "string", 
                       "cd_cnpj_estabelecimento": "string", 
                       "cd_cnpj_radical": "string", 
                       "dt_abertura": "date", 
                       "dt_encerramento": "date", 
                       "nm_fantasia": "string", 
                       "cd_documento_estrangeiro": "string", 
                       "sg_pais": "string",
                       "dh_ultima_atualizacao_oltp": "timestamp"}

for c in var_column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(var_column_type_map[c]))

# COMMAND ----------

trim_columns = ["cd_cnpj", "nm_razao_social"]

for c in trim_columns:
  df_raw = df_raw.withColumn(c, trim(df_raw[c]))  

# COMMAND ----------

# MAGIC %md
# MAGIC Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
df_raw = tcf.add_control_fields(df_raw, var_adf)\
.withColumn("cd_ano_atualizacao", year(col("dh_ultima_atualizacao_oltp")))

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
# MAGIC Keep only last refresh date by cd_pessoa_juridica and get changed years

# COMMAND ----------

w = Window.partitionBy("cd_pessoa_juridica").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", dense_rank().over(w))

# COMMAND ----------

years_with_change = df_raw.select("cd_ano_atualizacao").distinct()
if first_load == False:
  years_with_change_old = df_trs.filter(col("rank") > 1).select("cd_ano_atualizacao").distinct()
  years_with_change = years_with_change.union(years_with_change_old).dropDuplicates()
  del years_with_change_old

# COMMAND ----------

del df_raw

# COMMAND ----------

df_trs = df_trs.filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only changed years and write as incremental using partition

# COMMAND ----------

df_trs = df_trs.join(years_with_change, ["cd_ano_atualizacao"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC If there's no new record, exit notebook

# COMMAND ----------

if df_trs.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC We can keep as 3 file without worries. 

# COMMAND ----------

df_trs.coalesce(3).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")