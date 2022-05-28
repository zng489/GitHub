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
# MAGIC Processo	biz_biz_fta_kpi_escola_sesi_nivel_avancado_kpi_pivot
# MAGIC Tabela/Arquivo Origem	"/biz/orcamento/fta_kpi_escola_sesi_nivel_avancado
# MAGIC "
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_kpi_escola_sesi_nivel_avancado_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção que não possuem origem sistêmica.
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
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
from pyspark.sql.utils import AnalysisException

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
var_tables = {"origins": ["/orcamento/fta_kpi_escola_sesi_nivel_avancado"],
              "destination": "/orcamento/fta_kpi_escola_sesi_nivel_avancado_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_kpi_escola_sesi_nivel_avancado_kpi_pivot"
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
    "business": "/biz"
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'biz_biz_fta_gestao_financeira_kpi_pivot',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 1, "dt_closing": "2020-12-01"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

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

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from typing import Iterable 
from itertools import chain
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

columns = [
   'cd_ano_fechamento',
   'cd_mes_fechamento',
   'dt_fechamento',
   'cd_centro_responsabilidade',
   'cd_entidade_regional',
   'sg_disciplina_avaliada',
   'cd_produto_servico_educacao_sesi',
   'cd_periodicidade',
   'cd_mes_referencia',
   'cd_ano_referencia',
   'dh_referencia',
   'qt_calc_escola_avaliada',
   'qt_calc_escola_avaliada_avancado'
]

# COMMAND ----------

df = spark.read.parquet(src_fta).select(*columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Looking documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha:
# MAGIC 
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_periodicidade, 
# MAGIC cd_ano_referencia, cd_mes_referencia, dh_referencia,cd_metrica, vl_metrica 
# MAGIC FROM fta_kpi_escola_sesi_nivel_avancado
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC ONDE:
# MAGIC cd_metrica = 'pc_calc_escola_nivel_avancado_5mat'   e vl_metrica = qt_calc_escola_avaliada_avancado / qt_calc_escola_avaliada 
# MAGIC               WHEN sg_disciplina_avaliada = ''Matemática" AND cd_centro_responsabilidade = 303010201 and c.cd_produto_servico_educacao_sesi = 48
# MAGIC cd_metrica = 'pc_calc_escola_nivel_avancado_5port'  e vl_metrica = qt_calc_escola_avaliada_avancado / qt_calc_escola_avaliada 
# MAGIC               WHEN sg_disciplina_avaliada = ''Português" AND cd_centro_responsabilidade = 303010201 and c.cd_produto_servico_educacao_sesi = 48
# MAGIC cd_metrica = 'pc_calc_escola_nivel_avancado_9mat'   e vl_metrica = qt_calc_escola_avaliada_avancado / qt_calc_escola_avaliada 
# MAGIC               WHEN sg_disciplina_avaliada = ''Matemática" AND cd_centro_responsabilidade = 303010202 and c.cd_produto_servico_educacao_sesi = 52
# MAGIC cd_metrica = 'pc_calc_escola_nivel_avancado_9port'  e vl_metrica = qt_calc_escola_avaliada_avancado / qt_calc_escola_avaliada
# MAGIC               WHEN sg_disciplina_avaliada = ''Português" AND cd_centro_responsabilidade = 303010202 and c.cd_produto_servico_educacao_sesi = 52				
# MAGIC ```

# COMMAND ----------

df = df.filter(((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) & (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"])))

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

df = df\
.withColumn('pc_calc_escola_nivel_avancado_5mat',
            f.when((f.col('sg_disciplina_avaliada') == 'Matemática') &\
                   (f.col('cd_centro_responsabilidade') == '303010201') &\
                   (f.col('cd_produto_servico_educacao_sesi') == 48),
                   f.col("qt_calc_escola_avaliada_avancado") / f.col("qt_calc_escola_avaliada"))\
            .otherwise(f.lit(None)))\
.withColumn('pc_calc_escola_nivel_avancado_9mat',
            f.when((f.col('sg_disciplina_avaliada') == 'Matemática') &\
                   (f.col('cd_centro_responsabilidade') == '303010202') &\
                   (f.col('cd_produto_servico_educacao_sesi') == 52),
                   f.col("qt_calc_escola_avaliada_avancado") / f.col("qt_calc_escola_avaliada"))\
            .otherwise(f.lit(None)))\
.withColumn('pc_calc_escola_nivel_avancado_5port',
            f.when((f.col('sg_disciplina_avaliada') == 'Português') &\
                   (f.col('cd_centro_responsabilidade') == '303010201') &\
                   (f.col('cd_produto_servico_educacao_sesi') == 48),
                   f.col("qt_calc_escola_avaliada_avancado") / f.col("qt_calc_escola_avaliada"))\
            .otherwise(f.lit(None)))\
.withColumn('pc_calc_escola_nivel_avancado_9port',
            f.when((f.col('sg_disciplina_avaliada') == 'Português') &\
                   (f.col('cd_centro_responsabilidade') == '303010202') &\
                   (f.col('cd_produto_servico_educacao_sesi') == 52),
                   f.col("qt_calc_escola_avaliada_avancado") / f.col("qt_calc_escola_avaliada"))\
            .otherwise(f.lit(None)))\
.drop('qt_calc_escola_avaliada', 'qt_calc_escola_avaliada_avancado')


# COMMAND ----------

var_pc_columns = [column for column in df.columns if column.startswith('pc_')]

# COMMAND ----------

def melt(df: DataFrame, id_vars: list, value_vars: list, var_name="variable", value_name="value"):
    _vars_and_vals = f.create_map(
        list(chain.from_iterable([
            [f.lit(c), f.col(c)] for c in value_vars]
        ))
    )

    _tmp = df.select(*id_vars, f.explode(_vars_and_vals)) \
        .withColumnRenamed('key', var_name) \
        .withColumnRenamed('value', value_name)

    return _tmp

# COMMAND ----------

var_keep_columns = ['cd_ano_fechamento', 'cd_mes_fechamento', 'dt_fechamento','cd_entidade_regional', 'cd_centro_responsabilidade', 'cd_periodicidade', 'cd_ano_referencia', 'cd_mes_referencia', 'dh_referencia']

# COMMAND ----------

df = melt(df, var_keep_columns, var_pc_columns, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df = df.filter(f.col("vl_metrica").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

#df.count()

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")