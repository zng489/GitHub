# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	biz_biz_fta_gestao_ensino_profissional_kpi_pivot
# MAGIC Tabela/Arquivo Origem	/biz/producao/fte_producao_ensino_profissional /biz/producao/fta_convenio_ensino_profissional
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_gestao_ensino_profissional_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores físicos caculados sobre os indicadores básicos, para atendender a Visão Educação SENAI modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado, que ocorre às 20:00, e da biz fta_gestao_financeira que acontece na sequência
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC      05/08/20 - Marcela - Sumarizar fte_producao_ensino_profissional antes de fazer o union
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables = {"origins": ["/producao/fte_producao_ensino_profissional",
                         "/producao/fta_convenio_ensino_profissional"],
              "destination": "/producao/fta_gestao_ensino_profissional_kpi_pivot",
              "databricks": {
                "notebook": "/biz/educacao_senai/biz_biz_fta_gestao_ensino_profissional_kpi_pivot"
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
       'adf_pipeline_name': 'biz_biz_fta_gestao_ensino_profissional_kpi_pivot',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2013, "month": 12, "dt_closing": "2020-03-02"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_prod, src_conv = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]]
print(src_prod, src_conv)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, udf, sum, from_utc_timestamp, current_timestamp, create_map, explode
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame
from typing import Iterable 
from itertools import chain
from datetime import date, datetime
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
# MAGIC ### Looking documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha:
# MAGIC 
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_metrica, vl_metrica 
# MAGIC FROM fta_convenio_ensino_profissional UNION fta_producao_ensino_profissional , 
# MAGIC nesta tabela fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional,
# MAGIC cd_centro_responsabilidade uma vez que sua granularidade vai até cd_atendimento_matricula_ensino_prof
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC ONDE:
# MAGIC cd_metrica = 'qt_matricula_ensino_prof'                 e vl_metrica = qt_matricula_ensino_prof
# MAGIC cd_metrica = 'qt_matricula_ensino_prof_gratuidade_reg'  e vl_metrica = qt_matricula_ensino_prof_gratuidade_reg
# MAGIC cd_metrica = 'qt_matricula_indireta'                    e vl_metrica = qt_matricula_indireta
# MAGIC cd_metrica = 'qt_matricula_ensino_prof_total'           e vl_metrica = qt_matricula_ensino_prof + qt_matricula_indireta_concluinte
# MAGIC cd_metrica = 'qt_hr_escolar'                            e vl_metrica = qt_hr_escolar
# MAGIC cd_metrica = 'qt_hr_escolar_gratuidade_reg'             e vl_metrica = qt_hr_escolar_gratuidade_reg
# MAGIC cd_metrica = 'qt_matricula_ensino_prof_concluinte'      e vl_metrica = SUM(qt_matricula_ensino_prof_concluinte)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and filter by partition columns

# COMMAND ----------

# MAGIC %md
# MAGIC Note: filtering here optimizes reading operation by accessing only the relevant partitions

# COMMAND ----------

var_columns_fta_producao_ensino_profissional_mes = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "qt_matricula_ensino_prof", "qt_matricula_ensino_prof_gratuidade_reg", "qt_hr_escolar", "qt_hr_escolar_gratuidade_reg", "qt_matricula_ensino_prof_concluinte"]

# COMMAND ----------

df_prod = spark.read.parquet(src_prod)\
.select(*var_columns_fta_producao_ensino_profissional_mes)\
.filter((col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(sum("qt_matricula_ensino_prof").cast("int").alias("qt_matricula_ensino_prof"),     
     sum("qt_matricula_ensino_prof_gratuidade_reg").cast("int").alias("qt_matricula_ensino_prof_gratuidade_reg"),     
     sum("qt_hr_escolar").cast("int").alias("qt_hr_escolar"),
     sum("qt_hr_escolar_gratuidade_reg").cast("int").alias("qt_hr_escolar_gratuidade_reg"),
     sum("qt_matricula_ensino_prof_concluinte").cast("int").alias("qt_matricula_ensino_prof_concluinte"))

# COMMAND ----------

var_columns_fta_convenio_ensino_profissional_mes = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "qt_matricula_indireta"]

# COMMAND ----------

df_conv = spark.read.parquet(src_conv)\
.select(*var_columns_fta_convenio_ensino_profissional_mes)\
.filter((col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union tables

# COMMAND ----------

# MAGIC %md
# MAGIC Note: I know that it's possible to perform the same operation in a simpler way using full join. But it happens that declaring null columns and union the tables are much more faster, so I deliberately choose this method.

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"qt_matricula_indireta": lit(0)}

#Apply transformations defined in column_map
for c in var_column_map:
  df_prod = df_prod.withColumn(c, lit(var_column_map[c]))

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"qt_matricula_ensino_prof": lit(0),                  
                  "qt_matricula_ensino_prof_gratuidade_reg": lit(0),                   
                  "qt_hr_escolar": lit(0), 
                  "qt_hr_escolar_gratuidade_reg": lit(0),
                  "qt_matricula_ensino_prof_concluinte": lit(0)}

#Apply transformations defined in column_map
for c in var_column_map:
  df_conv = df_conv.withColumn(c, lit(var_column_map[c]))

# COMMAND ----------

df = df_prod.union(df_conv.select(df_prod.columns))

# COMMAND ----------

if df.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

del df_conv
del df_prod

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arithmetic operations

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"qt_matricula_ensino_prof_total": col("qt_matricula_ensino_prof") + col("qt_matricula_indireta")}

#Apply transformations defined in column_map
for c in var_column_map:
  df = df.withColumn(c, lit(var_column_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Melt quantity columns

# COMMAND ----------

var_qt_columns = [column for column in df.columns if column.startswith('qt_')]

# COMMAND ----------

def melt(df: DataFrame, id_vars: list, value_vars: list, var_name="variable", value_name="value"):
    _vars_and_vals = create_map(
        list(chain.from_iterable([
            [lit(c), col(c)] for c in value_vars]
        ))
    )

    _tmp = df.select(*id_vars, explode(_vars_and_vals)) \
        .withColumnRenamed('key', var_name) \
        .withColumnRenamed('value', value_name)

    return _tmp

# COMMAND ----------

var_keep_columns = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade"]

# COMMAND ----------

df = melt(df, var_keep_columns, var_qt_columns, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

# MAGIC %md
# MAGIC Fill null values in value columns with 0

# COMMAND ----------

df = df.fillna(0, ["vl_metrica"])

# COMMAND ----------

# MAGIC %md
# MAGIC Add load timestamp

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

#display(df)

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls
# MAGIC 
# MAGIC It's a small dataframe, we can keep it in only 1 file

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")