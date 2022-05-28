# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_atendimento_tecnologia_inovacao
# MAGIC Tabela/Arquivo Origem	"Principais:
# MAGIC /raw/bdo/bd_basi/tb_atendimento
# MAGIC Relacionadas:
# MAGIC /raw/bdo/bd_basi/tb_produto_servico
# MAGIC /raw/bdo/bd_basi/tb_produto_servico_centro_resp"
# MAGIC Tabela/Arquivo Destino	/trs/evt/atendimento_tecnologia_inovacao
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dt_inicio) || MONTH(dt_inicio)
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações de identificação (ESTÁVEIS) dos atendimentos do serviço de tecnologia e inovação  que são contabilizados em termos de consolidação dos dados de produção SENAI. 
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_atendimento_sti seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira maintenace by Tiago Shin
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
var_tables = {"origins": ["/bdo/bd_basi/tb_atendimento", "/bdo/bd_basi/tb_produto_servico", "/bdo/bd_basi/tb_produto_servico_centro_resp"],"destination": "/evt/atendimento_tecnologia_inovacao"}

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
       'adf_pipeline_name': 'raw_trs_atendimento_tecnologia_inovacao',
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

src_atd, src_pserv, src_pscr = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_atd, src_pserv, src_pscr)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, current_date, dense_rank, desc, trim, when, col, lit, substring, from_json
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from tb_atendimento
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

var_first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dh_ultima_atualizacao_oltp = df_sink.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  var_first_load = True

# COMMAND ----------

print("is first load?", var_first_load)
print("var_max_dh_ultima_atualizacao_oltp", var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC     at.cd_atendimento
# MAGIC   , at.cd_atendimento_dr
# MAGIC   , at.ds_atendimento
# MAGIC   , at.cd_entidade_regional 
# MAGIC   , at.cd_pessoa
# MAGIC   , at.cd_pessoa_unidd_atendto
# MAGIC   , at.cd_produto_servico
# MAGIC   , substr(pscr.nr_centro_responsabilidade, 3, 9)
# MAGIC   , at.dt_inicio 
# MAGIC   , at.dt_termino_previsto
# MAGIC   , at.dt_termino
# MAGIC   , at.fl_origem_recurso_mercado
# MAGIC   , at.fl_origem_recurso_fomento
# MAGIC   , at.fl_origem_recurso_senai
# MAGIC   , at.fl_origem_recurso_outrasent
# MAGIC   , at.fl_atendimento_rede
# MAGIC   , at.dt_atualizacao
# MAGIC   , at.fl_excluido  
# MAGIC FROM
# MAGIC   tb_atendimento at
# MAGIC   INNER JOIN
# MAGIC   tb_produto_servico pro on (at.cd_produto_servico = pro.cd_produto_servico) 
# MAGIC   INNER JOIN
# MAGIC   tb_produto_servico_centro_resp pscr on (pro.cd_produto_superior = pscr.cd_produto_servico)
# MAGIC WHERE
# MAGIC     at.cd_tipo_atendimento = 4
# MAGIC AND at.dt_atualizacao  > #var_max_dh_ultima_atualizacao_oltp
# MAGIC </pre>
# MAGIC *new*
# MAGIC <pre>
# MAGIC /* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from atendimento_tecnologia_inovacao */
# MAGIC /* Ler conjuntamente tb_atendimento, tb_produto_servico, tb_produto_servico_centro_resp da raw (regitros mais recente de ambas) com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC SELECT 
# MAGIC     at.cd_atendimento
# MAGIC   , at.cd_atendimento_dr
# MAGIC   , at.ds_atendimento
# MAGIC   , at.cd_entidade_regional 
# MAGIC   , at.cd_pessoa
# MAGIC   , at.cd_pessoa_unidd_atendto
# MAGIC   , at.cd_produto_servico
# MAGIC   , substr(pscr.nr_centro_responsabilidade, 3, 9)
# MAGIC   , at.dt_inicio 
# MAGIC   , at.dt_termino_previsto
# MAGIC   , CASE WHEN (at.dt_termino IS NULL AND at.cd_tipo_situacao_atendimento IN (2, 3)) 
# MAGIC          THEN DATE(dt_atualizacao) 
# MAGIC 		 ELSE at.dt_termino 
# MAGIC 	END AS dt_termino -- -- incluído em 20-05-2020
# MAGIC   , at.fl_origem_recurso_mercado
# MAGIC   , at.fl_origem_recurso_fomento
# MAGIC   , at.fl_origem_recurso_senai
# MAGIC   , at.fl_origem_recurso_outrasent
# MAGIC   , at.fl_atendimento_rede
# MAGIC   , at.dt_atualizacao
# MAGIC   , at.fl_excluido  
# MAGIC FROM
# MAGIC   tb_atendimento at
# MAGIC   INNER JOIN
# MAGIC   tb_produto_servico pro ON (at.cd_produto_servico = pro.cd_produto_servico) 
# MAGIC   INNER JOIN
# MAGIC   tb_produto_servico_centro_resp pscr ON (pro.cd_produto_superior = pscr.cd_produto_servico)
# MAGIC WHERE
# MAGIC     at.cd_tipo_atendimento = 4
# MAGIC AND at.cd_tipo_situacao_atendimento IS NOT NULL -- incluído em 20-05-2020	
# MAGIC AND at.dt_atualizacao  > #var_max_dh_ultima_atualizacao_oltp
# MAGIC </pre>

# COMMAND ----------

# Columns for tb_atendimento
atd_columns = ["cd_atendimento", "cd_atendimento_dr", "ds_atendimento", "cd_entidade_regional", "cd_pessoa", "cd_pessoa_unidd_atendto", "cd_produto_servico", "dt_inicio", "dt_termino_previsto", "dt_termino", "fl_origem_recurso_mercado", "fl_origem_recurso_fomento", "fl_origem_recurso_senai", "fl_origem_recurso_outrasent", "fl_atendimento_rede", "dt_atualizacao", "fl_excluido", "cd_tipo_situacao_atendimento"]

atd = spark.read.parquet(src_atd)\
.select(*atd_columns)\
.filter((col("cd_tipo_atendimento") == 4) &\
        (col("cd_tipo_situacao_atendimento").isNotNull()) &\
        (col("dt_atualizacao") > var_max_dh_ultima_atualizacao_oltp))

# COMMAND ----------

# If there's no new data, then just let it die.
if atd.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

pserv_columns = ["cd_produto_servico", "cd_produto_superior"]
pserv = spark.read.parquet(src_pserv).select(*pserv_columns).dropDuplicates()

# COMMAND ----------

pscr_columns = ["cd_produto_servico", "nr_centro_responsabilidade"]

pscr = spark.read.parquet(src_pscr). \
select(*pscr_columns). \
withColumn("nr_centro_responsabilidade", substring(col("nr_centro_responsabilidade"), 3, 9).cast("int")). \
withColumnRenamed("cd_produto_servico", "cd_produto_servico_pscr") \
.dropDuplicates()

# COMMAND ----------

df_join = pserv. \
join(pscr, pserv["cd_produto_superior"] == pscr["cd_produto_servico_pscr"], "inner"). \
drop(*["cd_produto_servico_pscr", "cd_produto_superior"]). \
dropDuplicates()

del pserv
del pscr

# COMMAND ----------

df_join = atd.join(df_join, ["cd_produto_servico"], "inner")

del atd

# COMMAND ----------

#df_join.count(), df_join.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modifying columns

# COMMAND ----------

df_join = df_join.withColumn("dt_termino", when((col("dt_termino").isNull()) & (col("cd_tipo_situacao_atendimento").isin(2, 3)),
                                                col("dt_atualizacao").cast("date"))\
                                           .otherwise(col("dt_termino")))

# COMMAND ----------

# MAGIC %md
# MAGIC Time for make-up

# COMMAND ----------

binary_columns = ["fl_origem_recurso_mercado", "fl_origem_recurso_senai", "fl_origem_recurso_fomento", "fl_origem_recurso_outrasent", "fl_atendimento_rede", "fl_excluido"]
for c in binary_columns:
  df_join = df_join.withColumn(c, when(col(c) == "S", 1).otherwise(0))

# COMMAND ----------

var_mapping = {"cd_atendimento": {"name": "cd_atendimento_sti", "type": "int"}, 
               "cd_atendimento_dr": {"name": "cd_atendimento_sti_dr", "type": "int"}, 
               "ds_atendimento": {"name": "ds_atendimento_sti", "type": "string"}, 
               "cd_entidade_regional": {"name": "cd_entidade_regional", "type": "int"}, 
               "cd_pessoa": {"name": "cd_pessoa_atendida_sti", "type": "int"}, 
               "cd_pessoa_unidd_atendto": {"name": "cd_pessoa_unidade_atendimento", "type": "int"}, 
               "cd_produto_servico": {"name": "cd_produto_servico", "type": "int"}, 
               "nr_centro_responsabilidade": {"name": "cd_centro_responsabilidade", "type": "string"}, 
               "fl_origem_recurso_mercado": {"name": "fl_origem_recurso_mercado", "type": "int"},
               "fl_origem_recurso_senai": {"name": "fl_origem_recurso_senai", "type": "int"},
               "fl_origem_recurso_fomento": {"name": "fl_origem_recurso_fomento", "type": "int"},
               "fl_origem_recurso_outrasent": {"name": "fl_origem_recurso_outrasent", "type": "int"},
               "fl_atendimento_rede": {"name": "fl_atendimento_rede", "type": "int"},
               "fl_excluido": {"name": "fl_excluido_oltp", "type": "int"},
               "dt_inicio": {"name": "dt_inicio", "type": "date"},
               "dt_termino_previsto": {"name": "dt_termino_previsto", "type": "date"},
               "dt_termino": {"name": "dt_termino", "type": "date"},
               "dt_atualizacao": {"name": "dh_ultima_atualizacao_oltp", "type": "timestamp"}
              }
  
for cl in var_mapping:
  df_join = df_join.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Adding the time control column, and this is surely TimeStamp type

# COMMAND ----------

#add control fields from trusted_control_field egg
df_join = tcf.add_control_fields(df_join, var_adf)

# COMMAND ----------

# MAGIC %md 
# MAGIC Adding partition columns

# COMMAND ----------

df_join = df_join.withColumn("ano_dt_inicio", year("dt_inicio")).withColumn("mes_dt_inicio", month("dt_inicio"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now it gets beautiful because if it is the first load, then we'll union this with an empty DataFrame of the same schema. If not, then the magic of updating partitions happen. Previous data has already the same schema, so, no need to worry.

# COMMAND ----------

if var_first_load is True:
  df_sink = spark.createDataFrame([], schema=df_join.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC We must union the existing trusted object with the new data and rank it by the most recent modification, keeping only the most recent one. Also, there's the need to keep control on what was old and what is new. AS it is partitioned by year, case there's no changes in a specific year, we can skip it.
# MAGIC 
# MAGIC Let's get the years in which we've got some changes.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Harmonizing schema with the available data in ADLS will always make it work.

# COMMAND ----------

df_sink = df_sink.union(df_join.select(df_sink.columns))
var_df_sink_columns = df_sink.columns

# COMMAND ----------

df_sink = df_sink.withColumn("rank", dense_rank().over(Window\
                                                       .partitionBy("cd_atendimento_sti")\
                                                       .orderBy(desc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

# We need to overwrite also the years with the old data before the update. If this doesn't happen, then we'll have duplicates.
# Year/month with change will contain df_join["ano_dt_inicio", "mes_dt_inicio" ] and df_sink["ano_dt_inicio", "ano_dt_inicio"] for when df_sink["rank"] = 1.

# in this union: left part = new, right part = old
period_with_change = df_join. \
select("ano_dt_inicio", "mes_dt_inicio"). \
dropDuplicates(). \
union(df_sink.filter(col("rank") == 2).select("ano_dt_inicio", "mes_dt_inicio").dropDuplicates()). \
dropDuplicates()

# COMMAND ----------

df_sink = df_sink. \
filter(col("rank") == 1). \
drop("rank"). \
join(period_with_change, ["ano_dt_inicio", "mes_dt_inicio"], "inner"). \
select(var_df_sink_columns).\
dropDuplicates()

# COMMAND ----------

del df_join

# COMMAND ----------

#df_sink.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated. 
# MAGIC 
# MAGIC We need to repartition the file in order to keep small files saved

# COMMAND ----------

df_sink.coalesce(3).write.partitionBy("ano_dt_inicio", "mes_dt_inicio").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

