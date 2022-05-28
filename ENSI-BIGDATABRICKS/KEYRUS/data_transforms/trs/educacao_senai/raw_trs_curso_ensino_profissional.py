# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_curso_ensino_profissional
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_curso
# MAGIC Tabela/Arquivo Destino	/trs/mtd/senai/curso_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 63.383 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Corresponde ao portfolio de cursos que será ofertado nas matrículas de Educação do SENAI. Os cursos são ofertados individualmente por Departamento Regional e vinculados a um produto de Educação definido no "roll" de produtos do Departamento Nacional. 
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_curso seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira
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

import re
import json

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_curso"], "destination": "/mtd/senai/curso_ensino_profissional"}

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
       'adf_pipeline_name': 'raw_trs_curso_ensino_profissional',
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
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, desc, greatest, trim, when, col, lit
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from curso_ensino_profissional
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_atualizacao = df_sink.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC cd_curso, 
# MAGIC nm_curso, 
# MAGIC cd_produto_servico, 
# MAGIC fl_portfolio, 
# MAGIC cd_curso_dr, 
# MAGIC cd_entidade_regional, 
# MAGIC cd_linha_acao, 
# MAGIC cd_area_atuacao, 
# MAGIC cd_curso_mec, 
# MAGIC cd_cbo_6, 
# MAGIC dt_inicio_oferta, 
# MAGIC dt_fim_oferta, 
# MAGIC nr_carga_horaria, 
# MAGIC nr_carga_horaria_estagio, 
# MAGIC cd_curso_scop, dt_atualizacao
# MAGIC FROM tb_curso
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First thing is to apply filtering for the maximum available var_max_dt_atualizacao.

# COMMAND ----------

# Columns for tb_atendimento
curso_columns = ["cd_curso", "nm_curso", "cd_produto_servico", "fl_portfolio", "cd_curso_dr", "cd_entidade_regional", "cd_linha_acao", "cd_area_atuacao", "cd_curso_mec", "cd_cbo_6", "cd_curso_scop", "dt_atualizacao"]
curso = spark.read.parquet(src_tb).\
select(*curso_columns).\
filter(col("dt_atualizacao") > var_max_dt_atualizacao).\
dropDuplicates()

# COMMAND ----------

# If there's no new data, then just let it die.
if curso.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

column_name_mapping = {"cd_curso": "id_curso_ensino_profissional", "nm_curso": "nm_curso_ensino_profissional", "cd_produto_servico": "cd_produto_servico_modalidade_ensino_prof", "fl_portfolio": "fl_portifolio_nacional", "cd_curso_dr": "cd_curso_ensino_profissional_dr", "cd_entidade_regional": "cd_entidade_regional", "cd_linha_acao": "cd_linha_acao", "cd_area_atuacao": "cd_area_atuacao", "cd_curso_mec": "cd_curso_mec", "cd_cbo_6": "cd_cbo_6", "cd_curso_scop": "cd_curso_ensino_profissional_scop", "dt_atualizacao": "dh_ultima_atualizacao_oltp"}
  
for key in column_name_mapping:
  curso =  curso.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC Transforming flag from S/N to 1/0

# COMMAND ----------

curso = curso.withColumn('fl_portifolio_nacional', when(col("fl_portifolio_nacional") == "S", lit(1)).otherwise(lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Adding the time control column

# COMMAND ----------

#add control fields from trusted_control_field egg
curso = tcf.add_control_fields(curso, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data quality section
# MAGIC 
# MAGIC nm_curso_ensino_profissional : Mapeamento direto, mas tirar os brancos do início e fim
# MAGIC 
# MAGIC cd_cbo_6 : Mapeamento direto, mas tirar os brancos do início e fim	cd_cbo_6

# COMMAND ----------

trim_columns = ["nm_curso_ensino_profissional", "cd_cbo_6"]
for c in trim_columns:
  curso = curso.withColumn(c, trim(curso[c]))  

# COMMAND ----------

column_type_map = {"id_curso_ensino_profissional" : "int", "nm_curso_ensino_profissional" : "string", "cd_produto_servico_modalidade_ensino_prof" : "int", "fl_portifolio_nacional" : "int", "cd_curso_ensino_profissional_dr" : "string", "cd_entidade_regional" : "int", "cd_linha_acao" : "int", "cd_area_atuacao" : "int", "cd_curso_mec" : "int", "cd_cbo_6" : "string", "cd_curso_ensino_profissional_scop" : "int", "dh_ultima_atualizacao_oltp" : "timestamp", "dh_insercao_trs" : "timestamp"}

for c in column_type_map:
  curso = curso.withColumn(c, curso[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now it gets beautiful because if it is the first load, then we'll union this with an empty DataFrame of the same schema. If not, then the magic of updating partitions happen. Previous data has already the same schema, so, no need to worry.

# COMMAND ----------

if first_load is True:
  df_sink = spark.createDataFrame([], schema=curso.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the only thing left is to implement this:
# MAGIC 
# MAGIC Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_curso seja encontrada.
# MAGIC 
# MAGIC We must union the existing trusted object with the new data and rank it by the most recent modification, keeping only the most recent one. Also, there's the need to keep control on what was old and what is new. No partition, no additional complexity.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Harmonizing schema with the available data in ADLS will always make it work.

# COMMAND ----------

curso = curso.select(df_sink.columns)

# COMMAND ----------

df_sink = df_sink.union(curso)

# COMMAND ----------

df_sink = df_sink.withColumn("rank", dense_rank().over(Window.partitionBy("id_curso_ensino_profissional").orderBy(desc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

df_sink = df_sink.filter(col("rank") == 1).drop("rank").dropDuplicates()

# COMMAND ----------

# Debug
#df_sink.count() == df_sink.select(df_sink["id_curso_ensino_profissional"]).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated. 
# MAGIC 
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_sink.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")