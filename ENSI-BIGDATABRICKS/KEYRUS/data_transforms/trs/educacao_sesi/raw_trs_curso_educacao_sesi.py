# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_curso_educacao_sesi
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/scae/curso
# MAGIC /raw/bdo/scae/produto_servico"
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/curso_educacao_sesi
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp) as cd_ano_atualizacao - 30.202 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Corresponde a relação de cursos voltados à educação: Educação Infantil, Ensino Fundamental, Ensino Médio e EJA (Educação de Jovens e Adultos) e de Educação continuada (cursos apenas, isto é, exceto eventos), Educação Profissional e Educação Superior oferecidos pelo SESI.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou sobrescreve o registro caso a chave cd_curso_educacao_sesi seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC 
# MAGIC Dev: Thiago Shin / Marcela
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
var_tables =  {"origins": ["/bdo/scae/curso", "/bdo/scae/produto_servico"], "destination": "/mtd/sesi/curso_educacao_sesi"}

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
       'adf_pipeline_name': 'raw_trs_curso_educacao_sesi',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-25T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""      

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_cs, src_ps = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]

print(src_cs, src_ps)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])

print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, year, max, current_timestamp, from_utc_timestamp, col, trim, substring, count, when
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

curso_useful_columns = ["cod_curso", "nom_curso", "des_curso", "cod_curso_dr", "num_carga_horaria_hora", "num_carga_horaria_minuto", "cod_produto_servico", "cd_tipo_portfolio", "cd_portfolio", "dat_registro", "ind_ativo", "cod_produto_servico"]

# COMMAND ----------

df_curso = spark.read.parquet(src_cs).select(*curso_useful_columns)

# COMMAND ----------

produto_servico_useful_columns = ["cod_produto_servico", "cod_centro_responsabilidade"]

# COMMAND ----------

df_produto_servico = spark.read.parquet(src_ps).select(*produto_servico_useful_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC /* 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from curso_educacao_sesi
# MAGIC Ler curso da raw com as regras para carga incremental, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC SELECT
# MAGIC C.cod_curso, C.nom_curso, C.des_curso, C.cod_curso_dr, C.num_carga_horaria_hora, C.num_carga_horaria_minuto, C.cod_produto_servico, 
# MAGIC C.cd_tipo_portfolio, C.cd_portfolio, C.dat_registro, C.ind_ativo
# MAGIC FROM curso C
# MAGIC INNER JOIN produto_servico P ON C.cod_produto_servico = P.cod_produto_servico
# MAGIC WHERE (SUBSTRING(P.cod_centro_responsabilidade, 3, 5) IN ('30301', '30302', '30303', '30304')  -- EDUCACAO BASICA, EDUCACAO CONTINUADA, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC        OR SUBSTRING(P.cod_centro_responsabilidade, 3, 9) = '305010103') -- FORMACAO CULTURAL (na verdade é um curso em EDUCACAO CONTINUADA)
# MAGIC AND SUBSTRING(P.cod_centro_responsabilidade, 3, 7) <> '3030202' -- não contabilizar como matrícula: Eventos em EDUCACAO CONTINUADA
# MAGIC AND dat_registro > #var_max_dh_ultima_atualizacao_oltp
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

# MAGIC %md
# MAGIC ### Filter and change types

# COMMAND ----------

df_curso = df_curso.filter(col("dat_registro") > var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# If there's no new data, then just let it die.
if df_curso.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

df_produto_servico = df_produto_servico.filter(((substring(col("cod_centro_responsabilidade"), 3, 5).isin('30301', '30302', '30303', '30304')) | 
                                                (substring(col("cod_centro_responsabilidade"), 3, 9) == '305010103')) &
                                               (substring(col("cod_centro_responsabilidade"), 3, 7) != '3030202')) \
.drop("cod_centro_responsabilidade")

# COMMAND ----------

# MAGIC %md
# MAGIC Joining curso and produto servico

# COMMAND ----------

df_raw = df_curso.join(df_produto_servico, ["cod_produto_servico"], "inner")

# COMMAND ----------

del df_curso
del df_produto_servico

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Get only last record by cod_curso and dat_registro
# MAGIC </pre>

# COMMAND ----------

w = Window.partitionBy("cod_curso").orderBy(col("dat_registro").desc())
df_raw = df_raw.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

df_raw = df_raw.withColumn("cd_tipo_portfolio", when(col("cd_tipo_portfolio") != 1, lit(0)).otherwise(col("cd_tipo_portfolio")))

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns and adjusting types

# COMMAND ----------

column_name_mapping = {"cod_curso": "cd_curso_educacao_sesi", 
                       "nom_curso": "nm_curso_educacao_sesi", 
                       "des_curso": "ds_curso_educacao_sesi", 
                       "cod_curso_dr": "ds_codificacao_curso_educacao_sesi_dr", 
                       "num_carga_horaria_hora": "qt_hora_carga_horaria",
                       "num_carga_horaria_minuto": "qt_minuto_carga_horaria", 
                       "cod_produto_servico": "cd_produto_servico_educacao_sesi",
                       "cd_tipo_portfolio": "fl_pertence_portfolio",
                       "dat_registro": "dh_ultima_atualizacao_oltp", 
                       "ind_ativo": "fl_ind_ativo_oltp"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

trim_columns = ["nm_curso_educacao_sesi", "ds_curso_educacao_sesi"]

for c in trim_columns:
  df_raw = df_raw.withColumn(c, trim(df_raw[c]))  

# COMMAND ----------

column_type_map = {"cd_curso_educacao_sesi":"int",
                   "nm_curso_educacao_sesi": "string",
                   "ds_curso_educacao_sesi": "string",
                   "ds_codificacao_curso_educacao_sesi_dr": "string",
                   "qt_hora_carga_horaria": "int",
                   "qt_minuto_carga_horaria": "int",
                   "cd_produto_servico_educacao_sesi": "int",
                   "fl_pertence_portfolio": "int", 
                   "cd_portfolio": "int", 
                   "dh_ultima_atualizacao_oltp": "timestamp",
                   "fl_ind_ativo_oltp": "int"}

for c in column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(column_type_map[c]))

# COMMAND ----------

df_raw = df_raw.withColumn("cd_ano_atualizacao", year(col("dh_ultima_atualizacao_oltp")))

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

if first_load is True:
  df_raw.coalesce(1).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

df_trs = df_trs.union(df_raw.select(df_trs.columns))

# COMMAND ----------

del df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only last refresh date by cd_curso_ensino_basico

# COMMAND ----------

w = Window.partitionBy("cd_curso_educacao_sesi").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.partitionBy("cd_ano_atualizacao").save(path=sink, format="parquet", mode="overwrite")