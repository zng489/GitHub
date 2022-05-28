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
# MAGIC Processo	biz_biz_fta_producao_tecnol_inovacao_kpi_pivot (versao 02.0)
# MAGIC Tabela/Arquivo Origem	/biz/producao/fte_producao_tecnologia_inovacao
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_producao_tecnol_inovacao_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores físicos caculados  sobre os indicadores básicos de Serviço de Tecnologia e Inovação do SENAI modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Tiago Shin / Marcela
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

"""var_tables = {"origins": ["/producao/fte_producao_tecnologia_inovacao"],
              "destination": "/producao/fta_producao_tecnol_inovacao_kpi_pivot",
              "databricks": {
                "notebook": "/biz/sti_senai/biz_biz_fta_producao_tecnol_inovacao_kpi_pivot"
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
       'adf_pipeline_name': 'biz_biz_fta_producao_tecnol_inovacao_kpi_pivot',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 4, "dt_closing": "2020-05-26"}}
"""

# COMMAND ----------

src_fta = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]][0]
print(src_fta)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, udf, sum, from_utc_timestamp, current_timestamp, create_map, explode, count
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
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_documento_empresa_atendida_calc, 
# MAGIC cd_metrica, vl_metrica 
# MAGIC FROM fta_producao_tecnologia_inovacao, nesta tabela fazendo o SUM(valor) com GROUP BY  por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, 
# MAGIC cd_entidade_regional, cd_centro_responsabilidade, cd_documento_empresa_atendida_calc uma vez que sua granularidade vai até cd_atendimento_sti
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC ONDE:
# MAGIC cd_metrica = 'qt_atendimento_sti' 	e vl_metrica = COUNT(cd_atendimento_sti)
# MAGIC cd_metrica = 'qt_hora_realizada' 	e vl_metrica = SUM(qt_hora_realizada)
# MAGIC cd_metrica = 'qt_ensaio_realizado' 	e vl_metrica = SUM(qt_ensaio_realizado)
# MAGIC cd_metrica = 'qt_calibracao_realizada' 	e vl_metrica = SUM(qt_calibracao_realizada)
# MAGIC cd_metrica = 'qt_material_realizado' 	e vl_metrica = SUM(qt_material_realizado)
# MAGIC cd_metrica = 'qt_relatorio_realizado' 	e vl_metrica = SUM(qt_relatorio_realizado)
# MAGIC cd_metrica = 'qt_certificado_realizado' e vl_metrica = SUM(qt_certificado_realizado)
# MAGIC cd_metrica = 'vl_producao' 		e vl_metrica = SUM(vl_producao)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and filter by partition columns

# COMMAND ----------

# MAGIC %md
# MAGIC Note: filtering here optimizes reading operation by accessing only the relevant partitions

# COMMAND ----------

var_columns_fta_producao_tecnologia_inovacao = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_documento_empresa_atendida_calc", "cd_atendimento_sti", "qt_hora_realizada", "qt_ensaio_realizado", "qt_calibracao_realizada", "qt_material_realizado", "qt_relatorio_realizado", "qt_certificado_realizado", "vl_producao"]

# COMMAND ----------

df = spark.read.parquet(src_fta)\
.select(*var_columns_fta_producao_tecnologia_inovacao)\
.filter((col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_documento_empresa_atendida_calc")\
.agg(count("cd_atendimento_sti").cast("int").alias("qt_atendimento_sti"),
     sum("qt_hora_realizada").cast("int").alias("qt_hora_realizada"),
     sum("qt_ensaio_realizado").cast("int").alias("qt_ensaio_realizado"),
     sum("qt_calibracao_realizada").cast("int").alias("qt_calibracao_realizada"),
     sum("qt_material_realizado").cast("int").alias("qt_material_realizado"),
     sum("qt_relatorio_realizado").cast("int").alias("qt_relatorio_realizado"),
     sum("qt_certificado_realizado").cast("int").alias("qt_certificado_realizado"),
     sum("vl_producao").cast("decimal(18,4)").alias("vl_producao"))

# COMMAND ----------

if df.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Melt quantity columns

# COMMAND ----------

var_qt_columns = [column for column in df.columns if column.startswith('qt_') or column.startswith('vl_')]

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

var_keep_columns = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_documento_empresa_atendida_calc"]

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

# COMMAND ----------

