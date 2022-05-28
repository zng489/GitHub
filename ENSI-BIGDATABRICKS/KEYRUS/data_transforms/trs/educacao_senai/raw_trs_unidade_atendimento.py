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
# MAGIC Processo	raw_trs_unidade_atendimento
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/bd_basi/tb_unidade_atendimento
# MAGIC /raw/bdo/oba/tb_industria_conhecimento"
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/unidade_atendimento
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 4.870 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	"Informações de identificação (ESTÁVEIS) das Unidades de Atendimento do Sistema Indústria
# MAGIC Ex: As escolas do SESI e do SENAI, os Instituos do SENAI, as unidades móveis, bem como as unidades de indústria do conhecimento, enfim, todas as Unidades que efetuam atendimentos, e que estão vinculadas a um DR
# MAGIC OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria"
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_pessoa cd_unidade_atendimento_dr seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC 
# MAGIC Dev: Tiago Shin, Marcela
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

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_unidade_atendimento","/bdo/oba/tb_industria_conhecimento"],"destination": "/mtd/corp/unidade_atendimento"}

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
       'adf_pipeline_name': 'raw_trs_unidade_atendimento',
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

src_tbu, src_tbc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tbu, src_tbc)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import dense_rank, lit, year, max, current_timestamp, from_utc_timestamp, col, trim, when, lower, from_unixtime, unix_timestamp
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from unidade_atendimento
# MAGIC Ler tb_unidade_atendimento da raw com as regras para carga incremental, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC SELECT 
# MAGIC cd_pessoa, nm_unidade_atendimento, cd_pessoa_unid_responsavel, cd_pessoa_juridica, cd_unidade_atendimento_dr, cd_entidade_regional, cd_unidade_atendimento_oba, cd_unidade_atendimento_scop, cd_unidade_atendimento_sistec, cd_inep, cd_inep_superior, dt_atualizacao, fl_excluido_logicamente, cd_tipo_categoria_ativo, cnpj_unidade
# MAGIC FROM tb_unidade_atendimento
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC +
# MAGIC SELECT 
# MAGIC cd_industria_conhecimento, nm_industria_conhecimento, cd_entidade_regional, cd_sgf, fl_excluido_logicamente, fl_categoria_ic_fixamovel, nr_cnpj_industria_conhecimento, dt_atualizacao
# MAGIC FROM tb_industria_conhecimento
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get max date of dh_ultima_atualizacao_oltp from last load

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
# MAGIC #### Reading raws with filter

# COMMAND ----------

useful_columns_unidade_atendimento = ["cd_pessoa", "nm_unidade_atendimento", "cd_pessoa_unid_responsavel","cd_pessoa_juridica", "cd_unidade_atendimento_dr", "cd_entidade_regional", "cd_unidade_atendimento_oba", "cd_unidade_atendimento_scop", "cd_unidade_atendimento_sistec", "cd_inep", "cd_inep_superior", "dt_atualizacao", "fl_excluido_logicamente", "cd_tipo_categoria_ativo", "cnpj_unidade"]

# COMMAND ----------

w = Window.partitionBy("cd_unidade_atendimento_dr").orderBy(col("dt_atualizacao").desc())

df_unidade_atendimento = spark.read.parquet(src_tbu).select(*useful_columns_unidade_atendimento) \
.filter(col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp) \
.withColumn("rank", dense_rank().over(w)).filter(col("rank") == 1) \
.drop("rank")

# COMMAND ----------

#df_unidade_atendimento.count()
#4949

# COMMAND ----------

useful_columns_industria_conhecimento = ["cd_industria_conhecimento", "nm_industria_conhecimento", "cd_entidade_regional", "cd_sgf", "fl_excluido", "fl_categoria_ic_fixamovel", "dt_atualizacao", "nr_cnpj_industria_conhecimento"]

# COMMAND ----------

w = Window.partitionBy("cd_industria_conhecimento").orderBy(col("dt_atualizacao").desc())

df_industria_conhecimento = spark.read.parquet(src_tbc).select(*useful_columns_industria_conhecimento) \
.filter(col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp) \
.withColumn("rank", dense_rank().over(w)).filter(col("rank") == 1) \
.drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming and aplying transformations

# COMMAND ----------

unidade_atendimento_column_name_mapping = {"cd_pessoa":"cd_pessoa_unidade_atendimento", 
                                           "cd_pessoa_unid_responsavel":"cd_pessoa_unidade_atend_responsavel",
                                           "cd_pessoa_juridica":"cd_pessoa_juridica_unidade_atend", 
                                           "cd_inep":"cd_unidade_atendimento_inep", 
                                           "cd_inep_superior":"cd_unidade_atendimento_inep_superior", 
                                           "dt_atualizacao": "dh_ultima_atualizacao_oltp",                                            
                                           "fl_excluido_logicamente": "fl_excluido_oltp",
                                           "cnpj_unidade": "cd_cnpj_unidade"}
  
for key in unidade_atendimento_column_name_mapping:
  df_unidade_atendimento =  df_unidade_atendimento.withColumnRenamed(key, unidade_atendimento_column_name_mapping[key])

# COMMAND ----------

df_unidade_atendimento = df_unidade_atendimento.withColumn("cd_tipo_categoria_ativo", when(col("cd_tipo_categoria_ativo").isin(1,2), col("cd_tipo_categoria_ativo")).otherwise(lit(None)))

# COMMAND ----------

industria_conhecimento_column_name_mapping = {"cd_industria_conhecimento":"cd_unidade_atendimento_dr", 
                                              "nm_industria_conhecimento":"nm_unidade_atendimento",
                                              "cd_sgf":"cd_unidade_atendimento_sgf", 
                                              "fl_excluido":"fl_excluido_oltp", 
                                              "fl_categoria_ic_fixamovel":"cd_tipo_categoria_ativo", 
                                              "dt_atualizacao": "dh_ultima_atualizacao_oltp",
                                              "nr_cnpj_industria_conhecimento": "cd_cnpj_unidade"}
  
for key in industria_conhecimento_column_name_mapping:
  df_industria_conhecimento =  df_industria_conhecimento.withColumnRenamed(key, industria_conhecimento_column_name_mapping[key])

# COMMAND ----------

df_industria_conhecimento = df_industria_conhecimento.withColumn("cd_tipo_categoria_ativo", when(lower(col("cd_tipo_categoria_ativo")) == "f", 1).when(lower(col("cd_tipo_categoria_ativo")) == "m", 2).otherwise(lit(None)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Normalyzing schemas to union raws

# COMMAND ----------

raw_columns = ["cd_pessoa_unidade_atendimento", "nm_unidade_atendimento", "cd_pessoa_unidade_atend_responsavel", "cd_pessoa_juridica_unidade_atend", "cd_unidade_atendimento_dr", "cd_entidade_regional", "cd_unidade_atendimento_oba", "cd_unidade_atendimento_scop", "cd_unidade_atendimento_sistec", "cd_unidade_atendimento_inep", "cd_unidade_atendimento_inep_superior", "dh_ultima_atualizacao_oltp", "cd_unidade_atendimento_sgf", "fl_excluido_oltp", "cd_tipo_categoria_ativo", "cd_cnpj_unidade"]

# COMMAND ----------

unidade_atendimento_column_to_add = list(set(raw_columns) - set(df_unidade_atendimento.columns))

for item in unidade_atendimento_column_to_add:
  df_unidade_atendimento =  df_unidade_atendimento.withColumn(item, lit(None))

# COMMAND ----------

industria_conhecimento_column_to_add = list(set(raw_columns) - set(df_industria_conhecimento.columns))

for item in industria_conhecimento_column_to_add:
  df_industria_conhecimento =  df_industria_conhecimento.withColumn(item, lit(None))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing types

# COMMAND ----------

column_type_map = {"cd_pessoa_unidade_atendimento" : "int",
                   "nm_unidade_atendimento": "string",
                   "cd_pessoa_unidade_atend_responsavel" : "int",
                   "cd_pessoa_juridica_unidade_atend" : "int",
                   "cd_unidade_atendimento_dr": "string",
                   "cd_entidade_regional": "int",
                   "cd_unidade_atendimento_oba": "int",
                   "cd_unidade_atendimento_scop": "string", 
                   "cd_unidade_atendimento_sistec": "string",
                   "cd_unidade_atendimento_inep": "int",
                   "cd_unidade_atendimento_inep_superior": "int",
                   "dh_ultima_atualizacao_oltp": "timestamp",
                   "cd_unidade_atendimento_sgf": "int",
                   "fl_excluido_oltp": "int",
                   "cd_tipo_categoria_ativo": "int",
                   "cd_cnpj_unidade": "string"}

# COMMAND ----------

for c in column_type_map:
  df_unidade_atendimento = df_unidade_atendimento.withColumn(c, df_unidade_atendimento[c].cast(column_type_map[c]))
  df_industria_conhecimento = df_industria_conhecimento.withColumn(c, df_industria_conhecimento[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Union raws

# COMMAND ----------

df_raw = df_unidade_atendimento.union(df_industria_conhecimento.select(df_unidade_atendimento.columns))

# COMMAND ----------

# If there's no new data, then just let it die.
if df_raw.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

#df_raw.count()
#5333

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aplying transformations

# COMMAND ----------

df_raw = df_raw.withColumn("fl_excluido_oltp", when(lower(col("fl_excluido_oltp"))=="s", 1).when(lower(col("fl_excluido_oltp"))=="n", 0).otherwise(0))

# COMMAND ----------

trim_columns = ["nm_unidade_atendimento", "cd_unidade_atendimento_inep"]

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
  df_trs = df_trs.union(df_raw.select(df_trs.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only last refresh date by cd_pessoa_juridica and get changed years

# COMMAND ----------

w = Window.partitionBy("cd_unidade_atendimento_dr").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", dense_rank().over(w))

# COMMAND ----------

years_with_change = df_raw.select("cd_ano_atualizacao").distinct()
if first_load == False:
  years_with_change = years_with_change.union(df_trs.filter(col("rank") > 1).select("cd_ano_atualizacao").distinct()).dropDuplicates()

# COMMAND ----------

df_trs = df_trs.filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only changed years and write as incremental using partition

# COMMAND ----------

#df_trs.count()
#5333

# COMMAND ----------

df_trs = df_trs.join(years_with_change, ["cd_ano_atualizacao"], "inner")

# COMMAND ----------

#df_trs.count()
#5333

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")