# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_convenio_ensino_profissional
# MAGIC Tabela/Arquivo Origem	"Principais:
# MAGIC /raw/bdo/bd_basi/tb_atendimento
# MAGIC Relacionadas:
# MAGIC /raw/bdo/bd_basi/tb_produto_servico_centro_resp
# MAGIC "
# MAGIC Tabela/Arquivo Destino	/trs/evt/convenio_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dt_entrada)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações dos convênios em termo de cooperação em ensino profissional que geram produção e que são contabilizados em termos de consolidação dos dados de produção SENAI. 
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_atendimento seja encontrada.
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
var_tables =  {"origins": ["/bdo/bd_basi/tb_atendimento", "/bdo/bd_basi/tb_produto_servico_centro_resp"], "destination": "/evt/convenio_ensino_profissional"}

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
       'adf_pipeline_name': 'raw_trs_convenio_ensino_profissional',
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

src_atd, src_pscr = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_atd, src_pscr)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, row_number, desc, when, lit, col, trim, substring
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de atualizacao carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) 
# MAGIC from convenio_ensino_profissional
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_atualizacao = df_sink.select(max(df_sink["dh_ultima_atualizacao_oltp"])).collect()[0][0]
except AnalysisException:
  var_first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC at.cd_atendimento, 
# MAGIC at.cd_atendimento_dr, 
# MAGIC at.cd_entidade_regional, 
# MAGIC at.cd_pessoa, 
# MAGIC cr.cd_centro_responsabilidade, 
# MAGIC at.dt_inicio, 
# MAGIC at.dt_termino_previsto, 
# MAGIC at.dt_termino,
# MAGIC at.fl_excluido,
# MAGIC at.dt_atualizacao,
# MAGIC ROW_NUMBER() OVER(PARTITION BY cd_atendimento ORDER BY dt_atualizacao DESC) as SQ 
# MAGIC FROM tb_atendimento at
# MAGIC 
# MAGIC LEFT JOIN tb_produto_servico_centro_resp pcr
# MAGIC ON at.cd_produto_servico = pcr.cd_produto_servico
# MAGIC AND pcr.cd_tipo_acao = 1
# MAGIC 
# MAGIC WHERE SQ = 1  -- registro mais recente da tb_atendimento
# MAGIC AND at.cd_tipo_atendimento = 5 -- convênio
# MAGIC AND at.dt_atualizacao  > #var_max_dh_ultima_atualizacao_oltp -- filtro para delta T
# MAGIC </pre>

# COMMAND ----------

var_atd_columns =  ["cd_atendimento", "cd_atendimento_dr", "cd_convenio", "cd_entidade_regional", "cd_pessoa", "dt_inicio", "dt_termino_previsto", "dt_termino", "dt_atualizacao", "fl_excluido", "cd_tipo_atendimento", "cd_produto_servico"]

atd = spark.read.parquet(src_atd). \
select(*var_atd_columns). \
filter((col("dt_atualizacao") > var_max_dt_atualizacao) & (col("cd_tipo_atendimento") == 5)). \
drop("cd_tipo_atendimento"). \
withColumn("row_number", row_number().over(Window.partitionBy("cd_atendimento").orderBy(desc("dt_atualizacao")))). \
filter(col("row_number") == 1). \
drop("row_number"). \
dropDuplicates()

# COMMAND ----------

# If there's no new data, then just let it die.
if atd.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

var_pscr_columns = ["cd_produto_servico" ,"nr_centro_responsabilidade", "cd_tipo_acao"]

pscr = spark.read.parquet(src_pscr). \
select(*var_pscr_columns). \
filter(col("cd_tipo_acao") == 1). \
drop("cd_tipo_acao"). \
dropDuplicates()

# COMMAND ----------

atd = atd.join(pscr, ["cd_produto_servico"], "left"). \
drop("cd_produto_servico"). \
dropDuplicates()

# COMMAND ----------

del pscr

# COMMAND ----------

atd = atd. \
withColumn("fl_excluido", when(col("fl_excluido") == "S", 1).otherwise(0).cast("int")). \
withColumn("nr_centro_responsabilidade", trim(substring(col("nr_centro_responsabilidade"), 3, 15)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data quality

# COMMAND ----------

var_name_mapping = {"cd_atendimento": "cd_atendimento_convenio_ensino_prof", 
                       "cd_atendimento_dr": "cd_atendimento_convenio_ensino_prof_dr", 
                       "cd_convenio": "cd_convenio_ensino_profissional", 
                       "cd_entidade_regional": "cd_entidade_regional", 
                       "cd_pessoa": "cd_pessoa_empresa_atendida", 
                       "nr_centro_responsabilidade": "cd_centro_responsabilidade", 
                       "dt_inicio": "dt_inicio_convenio", 
                       "dt_termino_previsto": "dt_fim_convenio_prevista", 
                       "dt_termino": "dt_fim_convenio", 
                       "dt_atualizacao": "dh_ultima_atualizacao_oltp", 
                       "fl_excluido": "fl_excluido_oltp"
                      }
  
for key in var_name_mapping:
  atd =  atd.withColumnRenamed(key, var_name_mapping[key])

# COMMAND ----------

var_trim_columns = ["cd_atendimento_convenio_ensino_prof_dr", "cd_convenio_ensino_profissional"]
for c in var_trim_columns:
  atd = atd.withColumn(c, trim(col(c)))  

# COMMAND ----------

var_type_map = {"cd_atendimento_convenio_ensino_prof": "int",
                 "cd_atendimento_convenio_ensino_prof_dr": "string",
                 "cd_convenio_ensino_profissional": "string",
                 "cd_entidade_regional": "int",
                 "cd_pessoa_empresa_atendida": "int",
                 "cd_centro_responsabilidade": "string",
                 "dt_inicio_convenio": "date",
                 "dt_fim_convenio_prevista": "date",
                 "dt_fim_convenio": "date",
                 "dh_ultima_atualizacao_oltp": "timestamp",
                 "fl_excluido_oltp": "int"
                }

for c in var_type_map:
  atd = atd.withColumn(c, col(c).cast(var_type_map[c]))

# COMMAND ----------

atd = atd.withColumn("ano_dt_inicio_convenio", year(col("dt_inicio_convenio")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This one is guaranteed to be type timestamp

# COMMAND ----------

#add control fields from trusted_control_field egg
atd = tcf.add_control_fields(atd, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now it gets beautiful because if it is the first load, then we'll union this with an empty DataFrame of the same schema. If not, then the magic of updating partitions happen. Previous data has already the same schema, so, no need to worry.

# COMMAND ----------

if var_first_load is True:
  df_sink = spark.createDataFrame([], schema=atd.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the only thing left is to implement this:
# MAGIC 
# MAGIC Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_atendimento seja encontrada.
# MAGIC 
# MAGIC We must union the existing trusted object with the new data and rank it by the most recent modification, keeping only the most recent one. Also, there's the need to keep control on what was old and what is new. AS it is partitioned by year, case there's no changes in a specific year, we can skip it.
# MAGIC 
# MAGIC Let's get the years in which we've got some changes.

# COMMAND ----------

years_with_change_new = atd.select(col("ano_dt_inicio_convenio")).dropDuplicates()

# COMMAND ----------

var_df_sink_columns = df_sink.columns

# COMMAND ----------

df_sink = df_sink.union(atd.select(var_df_sink_columns))

# COMMAND ----------

df_sink = df_sink.withColumn("row_number", row_number().over(Window.partitionBy("cd_atendimento_convenio_ensino_prof").orderBy(desc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

years_with_change = years_with_change_new. \
union(df_sink.filter(col("row_number") == 2).select("ano_dt_inicio_convenio").dropDuplicates()). \
dropDuplicates()

# COMMAND ----------

df_sink = df_sink.filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Easier now that we kept only years with some change - the new ones and the old ones to overwrite. Now we can limit our space just to it.

# COMMAND ----------

df_sink = df_sink.join(years_with_change, ["ano_dt_inicio_convenio"], "inner").select(var_df_sink_columns).dropDuplicates()

# COMMAND ----------

#df_sink.groupBy("cd_atendimento_convenio_ensino_prof").count().filter(col("count") > 1).show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated

# COMMAND ----------

df_sink.repartition(4).write.partitionBy("ano_dt_inicio_convenio").save(path=sink, format="parquet", mode="overwrite")