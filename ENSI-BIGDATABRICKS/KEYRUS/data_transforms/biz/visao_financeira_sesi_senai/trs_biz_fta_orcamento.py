# Databricks notebook source
# MAGIC %md
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_orcamento
# MAGIC Tabela/Arquivo Origem	/trs/evt/orcamento_nacional_realizado
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_orcamento
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores financeiros básicos de receitas e despesas com educação SESI e SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atuaização	trunca as partições com dt_ultima_atualizacao_oltp >= #var_max_dt_ultima_atualizacao_oltp, dando a oportunidade de recuperar registros ignoraddos na carga anterior
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado
# MAGIC 
# MAGIC Dev: Leonardo Mafra
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import json
import re

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES

"""
var_tables = {"origins": ["/mtd/corp/entidade_regional",
                         "/evt/orcamento_nacional_realizado"],
              "destination": "/orcamento/fta_orcamento",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/trs_biz_fta_orcamento"
              }
             }

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/tmp/dev/trs",
    "business": "/tmp/dev/biz"
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'trs_biz_fta_gestao_financeira',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': '3d54fd35ae9c4bfea99c5c140625c87a',
       'adf_trigger_name': 'Manual',
       'adf_trigger_time': '2020-06-09T17:22:07.834217Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {'closing': {'year': 2020, 'dt_closing': '2021-01-26', 'month': 12}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_er, src_or = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_er, src_or)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import trim, col, substring, dense_rank, desc, length, when, concat, lit, from_utc_timestamp, current_timestamp, sum, year, month, date_format, max, year, month
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql.utils import AnalysisException
from datetime import datetime, date
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_parameters = {}
if "closing" in var_user_parameters:
  if "year" and "month" and "dt_closing" in var_user_parameters["closing"] :
    var_parameters["prm_ano_fechamento"] = var_user_parameters["closing"]["year"]
    var_parameters["prm_mes_fechamento"] = var_user_parameters["closing"]["month"]
    splited_date = var_user_parameters["closing"]["dt_closing"].split('-', 2)
    var_parameters["prm_data_corte"] = date(int(splited_date[0]), int(splited_date[1]), int(splited_date[2]))
else:
  var_parameters["prm_ano_fechamento"] = datetime.now().year
  var_parameters["prm_mes_fechamento"] = datetime.now().month
  var_parameters["prm_data_corte"] = datetime.now()
  
print(var_parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading tables

# COMMAND ----------

useful_columns_entidade_regional = ["cd_entidade_regional", "cd_entidade_regional_erp_oltp"]

# COMMAND ----------

df_entidade_regional = spark.read.parquet(src_er).select(*useful_columns_entidade_regional)

# COMMAND ----------

display(df_entidade_regional)

# COMMAND ----------

useful_columns_orcamento_nacional_realizado = ["cd_entidade_regional_erp_oltp", 
                                               "dt_lancamento", 
                                               "cd_conta_contabil", 
                                               "vl_lancamento", 
                                               "dt_ultima_atualizacao_oltp", 
                                               "cd_centro_responsabilidade"
                                              ]

# COMMAND ----------

df_orcamento_nacional_realizado = spark.read. \
parquet(src_or). \
select(*useful_columns_orcamento_nacional_realizado)

# COMMAND ----------

display(df_orcamento_nacional_realizado)

# COMMAND ----------

display(df_orcamento_nacional_realizado.select("dt_ultima_atualizacao_oltp", "dt_lancamento", year("dt_lancamento"), month("dt_lancamento")).dropDuplicates().orderBy(desc("dt_ultima_atualizacao_oltp")).limit(40))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining both tables by cd_entidade_regional_erp_oltp

# COMMAND ----------

df_join = df_orcamento_nacional_realizado.join(df_entidade_regional, ["cd_entidade_regional_erp_oltp"], how='left')

# COMMAND ----------

display(df_join)

# COMMAND ----------

del df_orcamento_nacional_realizado
del df_entidade_regional

# COMMAND ----------

print(var_parameters)

# COMMAND ----------

df = df_join.filter(
  (year(col("dt_lancamento")) == var_parameters["prm_ano_fechamento"]) 
  & (month(col("dt_lancamento")) <= var_parameters["prm_mes_fechamento"]) 
  & (col("dt_ultima_atualizacao_oltp") <= var_parameters["prm_data_corte"])
). \
drop("dt_ultima_atualizacao_oltp", "dt_lancamento")

# COMMAND ----------

display(df)

# COMMAND ----------

print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC If there's no data and it is the first load, we must save the data frame without partitioning, because saving an empty partitioned dataframe does not save the metadata.
# MAGIC When there is no new data in the business and you already have other data from other loads, nothing happens.
# MAGIC And when we have new data, it is normally saved with partitioning.

# COMMAND ----------

if df.count()==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

var_with_columns = {"cd_ano_fechamento": lit(var_parameters["prm_ano_fechamento"]).cast("int"), 
                    "cd_mes_fechamento": lit(var_parameters["prm_mes_fechamento"]).cast("int"), 
                    "dt_fechamento": lit(var_parameters["prm_data_corte"]).cast("date")
                   }

for c in var_with_columns:
  df = df.withColumn(c, lit(var_with_columns[c]))

# COMMAND ----------

df = df.withColumn('cd_entidade_regional', when(col("cd_entidade_regional").isNull(), -98).otherwise(col("cd_entidade_regional")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggreganting values by cd_entidade_regional, cd_centro_responsabilidade

# COMMAND ----------

df = df.groupBy('cd_ano_fechamento', 'cd_mes_fechamento', 'dt_fechamento', 'cd_entidade_regional', 'cd_centro_responsabilidade','cd_conta_contabil')\
.agg(sum('vl_lancamento').alias('vl_lancamento') \
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adjusting schema

# COMMAND ----------

# MAGIC %md
# MAGIC The purpose of redefining the schema again is that after the aggregation, decimal column types changed. So this is a garantee that we are compliant with the specified type.

# COMMAND ----------

var_column_type_map = {"cd_ano_fechamento" : "int", 
                       "cd_mes_fechamento" : "int", 
                       "dt_fechamento" : "date", 
                       "cd_entidade_regional": "int",
                       "cd_centro_responsabilidade" : "string", 
                       "cd_conta_contabil" : "string", 
                       "vl_lancamento" : "decimal(18,6)" }

for c in var_column_type_map:
  df = df.withColumn(c, df[c].cast(var_column_type_map[c]))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding columns for partition and current timestamp

# COMMAND ----------

print(var_adf)

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")