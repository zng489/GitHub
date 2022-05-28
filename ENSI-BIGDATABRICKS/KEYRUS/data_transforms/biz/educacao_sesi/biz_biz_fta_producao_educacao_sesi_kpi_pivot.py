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
# MAGIC Processo	biz_biz_fta_producao_educacao_sesi_kpi_pivot
# MAGIC Tabela/Arquivo Origem	/biz/producao/fte_producao_educacao_sesi
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_producao_educacao_sesi_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores físicos caculados sobre os indicadores básicos, para atendender a Visão Educação SENAI modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado, que ocorre às 20:00, e da biz fta_gestao_financeira que acontece na sequência
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
var_tables = {"origins": ["/producao/fte_producao_educacao_sesi"],
              "destination": "/producao/fta_producao_educacao_sesi_kpi_pivot",
              "databricks": {
                "notebook": "/biz/educacao_sesi/biz_biz_fta_producao_educacao_sesi_kpi_pivot"
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
       'adf_pipeline_name': 'biz_biz_fta_producao_educacao_sesi_kpi_pivot',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing":{"year":2019,"month":12,"dt_closing":"2020-05-28"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_pro = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]][0]
print(src_pro)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, udf, sum, from_utc_timestamp, current_timestamp, create_map, explode, from_json, when
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame
from typing import Iterable 
from itertools import chain
from datetime import date, datetime
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Looking documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_metrica, vl_metrica 
# MAGIC FROM fte_producao_educacao_sesi, fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade uma vez que sua granularidade vai até cd_matricula_ensino_sesi
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC ONDE:
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi'                  e vl_metrica = SUM(qt_matricula_educacao_sesi)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_gratuidade_reg'   e vl_metrica = SUM(qt_matricula_educacao_sesi_gratuidade_reg)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_ebep'             e vl_metrica = SUM(qt_matricula_educacao_sesi_ebep)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_foco_industria'   e vl_metrica = SUM(qt_matricula_educacao_sesi_foco_industria)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_transf_turma'     e vl_metrica = SUM(qt_matricula_educacao_sesi_transf_turma)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_transf_interna'   e vl_metrica = SUM(qt_matricula_educacao_sesi_transf_interna)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_saldo'            e vl_metrica = SUM(qt_matricula_educacao_sesi - (qt_matricula_educacao_sesi_transf_turma + qt_matricula_educacao_sesi_transf_interna))
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_turma_concluida'e vl_metrica = SUM(qt_matricula_educacao_sesi_turma_concluida)
# MAGIC cd_metrica = 'qt_matricula_educacao_sesi_concluinte'     e vl_metrica = SUM(qt_matricula_educacao_sesi_concluinte)
# MAGIC cd_metrica = 'qt_hr_aula'                                  e vl_metrica = SUM(qt_hr_aula)
# MAGIC cd_metrica = 'qt_hr_aula_gratuidade_reg'                   e vl_metrica = SUM(qt_hr_aula_gratuidade_reg)
# MAGIC cd_metrica = 'qt_hr_reconhec_saberes'                      e vl_metrica = SUM(qt_hr_reconhec_saberes)
# MAGIC cd_metrica = 'qt_hr_recon_sab_gratuidade_reg'              e vl_metrica = SUM(qt_hr_recon_sab_gratuidade_reg)
# MAGIC cd_metrica = 'qt_matricula_educ_basica_contnd'           e vl_metrica = SUM(qt_matricula_educacao_sesi - (qt_matricula_educacao_sesi_transf_turma + qt_matricula_educacao_sesi_transf_interna)) WHEN cd_centro_responsabilidade  = '30301%' ou '3030201%'
# MAGIC cd_metrica = 'qt_matric_educ_basica_contnd_gratuidade_reg' e vl_metrica = SUM(qt_matric_educ_basica_contnd_gratuidade_reg) WHEN cd_centro_responsabilidade  = '30301%' ou '3030201%'
# MAGIC cd_metrica = 'qt_hr_aula_educ_basica_contnd'               e vl_metrica = SUM(qt_hr_aula + qt_hr_reconhec_saberes) WHEN cd_centro_responsabilidade  = '30301%' ou '3030201%'
# MAGIC cd_metrica = 'qt_hr_aula_educ_basica_contnd_gratuidade_reg'e vl_metrica = SUM(qt_hr_aula + qt_hr_reconhec_saberes) WHEN cd_centro_responsabilidade  = '30301%' ou '3030201%' 
# MAGIC cd_metrica = 'qt_hr_aula_gratuidade_reg_prod_estrategico'  e vl_metrica = SUM(qt_hr_aula_gratuidade_reg + qt_hr_recon_sab_gratuidade_reg) WHEN cd_centro_responsabilidade IN ('303010201', '303010202', '303010302') ou (cd_centro_responsabilidade = '303010411%' e cd_tipo_vinculo_matricula = 1)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get parameters prm_ano_fechamento, prm_mes_fechamento and prm_data_corte

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
# MAGIC ### Load and filter by partition columns

# COMMAND ----------

# MAGIC %md
# MAGIC Note: filtering here optimizes reading operation by accessing only the relevant partitions

# COMMAND ----------

var_columns_fta_producao_educacao_sesi = ['cd_entidade_regional', 'cd_centro_responsabilidade', 'cd_tipo_vinculo_matricula', 'qt_matricula_educacao_sesi', 'qt_matricula_educacao_sesi_gratuidade_reg', 'qt_matricula_educacao_sesi_ebep', 'qt_matricula_educacao_sesi_foco_industria', 'qt_matricula_educacao_sesi_transf_turma', 'qt_matricula_educacao_sesi_transf_interna', 'qt_matricula_educacao_sesi_turma_concluida', 'qt_matricula_educacao_sesi_concluinte', 'qt_hr_aula', 'qt_hr_aula_gratuidade_reg', 'qt_hr_reconhec_saberes', 'qt_hr_recon_sab_gratuidade_reg', 'dh_insercao_biz', 'kv_process_control', 'dt_fechamento', 'cd_ano_fechamento', 'cd_mes_fechamento']

# COMMAND ----------

df = spark.read.parquet(src_pro)\
.select(*var_columns_fta_producao_educacao_sesi)\
.filter((col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_tipo_vinculo_matricula")\
.agg(sum("qt_matricula_educacao_sesi").cast("int").alias("qt_matricula_educacao_sesi"),
     sum("qt_matricula_educacao_sesi_gratuidade_reg").cast("int").alias("qt_matricula_educacao_sesi_gratuidade_reg"),
     sum("qt_matricula_educacao_sesi_ebep").cast("int").alias("qt_matricula_educacao_sesi_ebep"),
     sum("qt_matricula_educacao_sesi_foco_industria").cast("int").alias("qt_matricula_educacao_sesi_foco_industria"),
     sum("qt_matricula_educacao_sesi_transf_turma").cast("int").alias("qt_matricula_educacao_sesi_transf_turma"),
     sum("qt_matricula_educacao_sesi_transf_interna").cast("int").alias("qt_matricula_educacao_sesi_transf_interna"),     
     sum("qt_matricula_educacao_sesi_turma_concluida").cast("int").alias("qt_matricula_educacao_sesi_turma_concluida"),
     sum("qt_matricula_educacao_sesi_concluinte").cast("int").alias("qt_matricula_educacao_sesi_concluinte"),
     sum("qt_hr_aula").cast("int").alias("qt_hr_aula"),
     sum("qt_hr_aula_gratuidade_reg").cast("int").alias("qt_hr_aula_gratuidade_reg"),
     sum("qt_hr_reconhec_saberes").cast("int").alias("qt_hr_reconhec_saberes"),
     sum("qt_hr_recon_sab_gratuidade_reg").cast("int").alias("qt_hr_recon_sab_gratuidade_reg"))

# COMMAND ----------

if df.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new columns

# COMMAND ----------

df = df.withColumn("qt_matricula_educacao_sesi_saldo", col("qt_matricula_educacao_sesi") - (col("qt_matricula_educacao_sesi_transf_turma") + col("qt_matricula_educacao_sesi_transf_interna")))\
.withColumn("qt_matricula_educ_basica_contnd", 
            when(col("cd_centro_responsabilidade").startswith("30301") | col("cd_centro_responsabilidade").startswith("3030201"), 
                 col("qt_matricula_educacao_sesi") - 
                 (col("qt_matricula_educacao_sesi_transf_turma") + 
                  col("qt_matricula_educacao_sesi_transf_interna")))\
            .otherwise(lit(0)))\
.withColumn("qt_matric_educ_basica_contnd_gratuidade_reg", 
            when(col("cd_centro_responsabilidade").startswith("30301") | col("cd_centro_responsabilidade").startswith("3030201"), 
                 col("qt_matricula_educacao_sesi_gratuidade_reg"))\
            .otherwise(lit(0)))\
.withColumn("qt_hr_aula_educ_basica_contnd", 
            when(col("cd_centro_responsabilidade").startswith("30301") | col("cd_centro_responsabilidade").startswith("3030201"), 
                 col("qt_hr_aula") +
                 col("qt_hr_reconhec_saberes"))\
            .otherwise(lit(0)))\
.withColumn("qt_hr_aula_educ_basica_contnd_gratuidade_reg", 
            when(col("cd_centro_responsabilidade").startswith("30301") | col("cd_centro_responsabilidade").startswith("3030201"), 
                 col("qt_hr_aula_gratuidade_reg") +
                 col("qt_hr_recon_sab_gratuidade_reg"))\
            .otherwise(lit(0)))\
.withColumn("qt_hr_aula_gratuidade_reg_prod_estrategico", 
            when((col("cd_centro_responsabilidade").isin("303010201",  "303010202", "303010302")) |\
                 ((col("cd_centro_responsabilidade") == "303010411") &\
                  (col("cd_tipo_vinculo_matricula") == 1)), 
                 col("qt_hr_aula_gratuidade_reg") +
                 col("qt_hr_recon_sab_gratuidade_reg"))\
            .otherwise(lit(0)))\
.drop("cd_tipo_vinculo_matricula")

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
# MAGIC Change types

# COMMAND ----------

df = df.withColumn("vl_metrica", col("vl_metrica").cast("decimal(18,6)"))

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

# COMMAND ----------

