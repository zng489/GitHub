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
# MAGIC Processo	biz_biz_fta_gestao_financeira_kpi_pivot (versao 03)
# MAGIC Tabela/Arquivo Origem	/biz/orcamento/fta_gestao_financeira
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores financeiros caculados  sobre os indicadores básicos, para atendender a Visão Financeira SESI e SENAI  modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado, que ocorre às 20:00, e da biz fta_gestao_financeira que acontece na sequência
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
var_tables = {"origins": ["/orcamento/fta_gestao_financeira"],
              "destination": "/orcamento/fta_gestao_financeira_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_kpi_pivot"
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
       'adf_pipeline_name': 'biz_biz_fta_gestao_financeira_kpi_pivot',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2013, "month": 12, "dt_closing": "2020-02-03"}}
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
   'vl_receita_total',
   'vl_receita_corrente',
   'vl_receita_servico',
   'vl_receita_convenio',
   'vl_receita_contribuicao_direta',
   'vl_receita_contribuicao_indireta',
   'vl_receita_subvencao_ordinaria',
   'vl_receita_auxilio_minimo',
   'vl_receita_subvencao_especial',
   'vl_receita_auxilio_especial',
   'vl_receita_financeira',
   'vl_receita_apoio_financeiro',
   'vl_receita_industrial',
   'vl_receita_subvencao_auxilio_extraordinario',
   'vl_receita_projeto_estrategico',
   'vl_despesa_total',
   'vl_despesa_corrente',
   'vl_despesa_capital',
   'vl_despesa_pessoal',
   'vl_receita_outros',
   'vl_despesa_total_negocio',
   'vl_despesa_pessoal_negocio',
   'vl_despesa_pessoal_gestao',
   'vl_despesa_ocupacao_utilidade',
   'vl_despesa_material',
   'vl_despesa_transporte_viagem',
   'vl_despesa_material_distribuicao_gratuita',
   'vl_despesa_servico_terceiro',
   'vl_despesa_arrendamento_mercantil',
   'vl_despesa_financeira',
   'vl_despesa_imposto_taxa_contrib',
   'vl_despesa_diversa',
   'vl_despesa_arrecadacao_indireta',
   'vl_despesa_contrib_transf_reg',
   'vl_despesa_subvencao_aux_reg',
   'vl_despesa_convenio',
   'vl_despesa_apoio_financeiro',
   'vl_despesa_auxilio_a_terceiro',
   'vl_despesa_contrib_associativa_filiacao',
   'vl_despesa_investimento',
   'vl_despesa_inversao_financeira',
   'vl_despesa_subvencao_auxilio',
   'vl_despesa_amortizacao'
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
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_metrica, vl_metrica FROM fta_gestao_financeira onde
# MAGIC os mapeamentos diretos ou através de fórmulas (vl_metrica= <cálculo da métrica>) encontram se detalhados abaixo (a partir da linha 30) para a leitura
# MAGIC SELECT ...
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
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

df = df.withColumn('vl_resultado_orcamentario', f.col('vl_receita_total') - f.col('vl_despesa_total'))\
.withColumn('vl_resultado_operacional', f.col('vl_receita_corrente') - f.col('vl_despesa_corrente'))\
.withColumn('vl_receita_contribuicao_compulsoria', f.col('vl_receita_contribuicao_direta') + f.col('vl_receita_contribuicao_indireta') + \
            f.col('vl_receita_auxilio_minimo') + f.col('vl_receita_auxilio_especial') + f.col('vl_receita_subvencao_ordinaria') +\
            f.col('vl_receita_subvencao_especial'))\
.withColumn('vl_receita_servico_convenio', f.col('vl_receita_servico') + f.col('vl_receita_convenio'))\
.withColumn("vl_despesa_transf_reg_arrecad_indireta", f.col("vl_despesa_contrib_transf_reg") + f.col("vl_despesa_arrecadacao_indireta"))\
.withColumn("vl_despesa_transferencia_reg", f.col("vl_despesa_contrib_transf_reg") + f.col("vl_despesa_subvencao_aux_reg") + f.col("vl_despesa_subvencao_auxilio"))\
.withColumn("vl_receita_industrial_outros", f.col("vl_receita_industrial") + f.col("vl_receita_outros"))\
.withColumn("vl_receita_servico_industrial_outra", 
            f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_industrial") + f.col("vl_receita_apoio_financeiro") + 
            (f.col("vl_receita_outros") + f.col("vl_receita_subvencao_auxilio_extraordinario")))

# COMMAND ----------

var_vl_columns = [column for column in df.columns if column.startswith('vl_')]

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

var_keep_columns = ['cd_ano_fechamento', 'cd_mes_fechamento', 'dt_fechamento','cd_entidade_regional',  'cd_centro_responsabilidade']

# COMMAND ----------

df = melt(df, var_keep_columns, var_vl_columns, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

"""
Use this only if transforming the description at cd_metrica in upper case text is needed. Otherwise, it will keep the values with underlines instead of spaces.
@f.udf()
def cammel_case_values(column_value):
  values = [value[0].upper() + value[1:] for value in column_value.split('_')]
  values = ' '.join(values)
  return values
df = df.withColumn('cd_metrica', cammel_case_values(f.col('cd_metrica')))
"""

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