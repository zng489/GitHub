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
# MAGIC Processo	biz_biz_fta_gestao_financeira_saude_seguranca_indus_kpi_pivot
# MAGIC Tabela/Arquivo Origem	"/biz/orcamento/fta_receita_servico_convenio_rateada_negocio
# MAGIC /biz/orcamento/fta_gestao_financeira_saude_seguranca_indus
# MAGIC /biz/corporativo/dim_hierarquia_entidade_regional"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_saude_seguranca_indus_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas de Negócios rateadas de SSI do SESI modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado, que ocorre às 20:00, e da biz fta_gestao_financeira que acontece na sequência
# MAGIC 
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
var_tables = {"origins": ["/orcamento/fta_gestao_financeira",                                                    
                          "/orcamento/fta_gestao_financeira_saude_seguranca_indus",
                          "/corporativo/dim_hierarquia_entidade_regional"], 
              "destination": "/orcamento/fta_gestao_financeira_saude_seguranca_indus_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_saude_seguranca_indus_kpi_pivot"
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

var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_fta_fin, src_fta_ssi, src_dim_reg  = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]]
print(src_fta_fin, src_fta_ssi, src_dim_reg)

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

# MAGIC %md
# MAGIC ```
# MAGIC --Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha: cd_metrica,  vl_metrica , os mapeamentos diretos ou através de fórmulas (vl_metrica= <cálculo da métrica>) encontram se detalhados abaixo (a partir da linha 30) para a leitura
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_ssi' AS cd_metrica,
# MAGIC SUM(vl_despcor_ssi_rateada + vl_despcor_etd_gestao_outros_ssi_rateada + vl_despcor_suporte_negocio_ssi_rateada) AS  vl_metrica
# MAGIC FROM fta_gestao_financeira_saude_seguranca_indus o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_ssi' AS cd_metrica,
# MAGIC SUM(vl_despcor_ssi_rateada) AS  vl_metrica
# MAGIC FROM fta_gestao_financeira_saude_seguranca_indus o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_etd_gestao_outros_ssi' AS cd_metrica,
# MAGIC SUM(vl_despcor_etd_gestao_outros_ssi_rateada) AS  vl_metrica
# MAGIC FROM fta_gestao_financeira_saude_seguranca_indus o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_suporte_negocio_ssi' AS cd_metrica,
# MAGIC SUM(vl_despcor_suporte_negocio_ssi_rateada) AS  vl_metrica
# MAGIC FROM fta_gestao_financeira_saude_seguranca_indus o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_desptot_ssi_total' AS cd_metrica,
# MAGIC sum(vl_despesa_corrente + vl_desptot_etd_gestao_outros_rateada + vl_desptot_suporte_negocio_rateada + vl_desptot_indireta_gestao_rateada + vl_desptot_indireta_desenv_institucional_rateada + vl_desptot_indireta_apoio_rateada) AS vl_metrica
# MAGIC FROM fta_despesa_rateada_negocio o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade like '304%'
# MAGIC AND cd_entidade_nacional = 2
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional, 
# MAGIC e.sg_entidade_regional, 
# MAGIC o.cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional, 
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_ssi' as cd_metrica,
# MAGIC SUM(vl_receita_servico + vl_receita_convenio) as vl_metrica
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade like '304%'
# MAGIC AND cd_centro_responsabilidade NOT IN (304100101, 304110101) -- incluído em 20/01/2021 
# MAGIC AND cd_entidade_nacional = 2
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC e.sg_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira_saude_seguranca_indus

# COMMAND ----------

df_fta_ssi = spark.read.parquet(src_fta_ssi)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", 
        "vl_despcor_ssi_rateada", "vl_despcor_etd_gestao_outros_ssi_rateada", "vl_despcor_suporte_negocio_ssi_rateada",
        "vl_despcor_indireta_gestao_ssi_rateada", "vl_despcor_indireta_desenv_institucional_ssi_rateada", "vl_despcor_indireta_apoio_ssi_rateada")\
.filter(((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) & (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"])))\
.withColumn("vl_despcor_direta_ssi",
            (f.col("vl_despcor_ssi_rateada") + 
             f.col("vl_despcor_etd_gestao_outros_ssi_rateada") + 
             f.col("vl_despcor_suporte_negocio_ssi_rateada")))\
.withColumn("vl_despcor_ssi",
            f.when(f.col("cd_centro_responsabilidade").startswith("30403"), f.col("vl_despcor_ssi_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_etd_gestao_outros_ssi",
            f.when(f.col("cd_centro_responsabilidade").startswith("30407"), f.col("vl_despcor_etd_gestao_outros_ssi_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_suporte_negocio_ssi",
            f.when(f.col("cd_centro_responsabilidade").startswith("30408"), f.col("vl_despcor_suporte_negocio_ssi_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_ssi_total",
            (f.col("vl_despcor_ssi_rateada") +
             f.col("vl_despcor_etd_gestao_outros_ssi_rateada") +
             f.col("vl_despcor_suporte_negocio_ssi_rateada") +
             f.col("vl_despcor_indireta_gestao_ssi_rateada") +
             f.col("vl_despcor_indireta_desenv_institucional_ssi_rateada") +
             f.col("vl_despcor_indireta_apoio_ssi_rateada")))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("vl_despcor_ssi").cast("decimal(18,6)").alias("vl_despcor_ssi"),
     f.sum("vl_despcor_etd_gestao_outros_ssi").cast("decimal(18,6)").alias("vl_despcor_etd_gestao_outros_ssi"),
     f.sum("vl_despcor_suporte_negocio_ssi").cast("decimal(18,6)").alias("vl_despcor_suporte_negocio_ssi"),
     f.sum("vl_despcor_direta_ssi").cast("decimal(18,6)").alias("vl_despcor_direta_ssi"),
     f.sum("vl_despcor_ssi_total").cast("decimal(18,6)").alias("vl_despcor_ssi_total"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_entidade_regional (aux)

# COMMAND ----------

df_dim_reg = spark.read.parquet(src_dim_reg)\
.select("cd_entidade_regional", "cd_entidade_nacional")\
.filter(f.col("cd_entidade_nacional") == 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira

# COMMAND ----------

df_fta_fin = spark.read.parquet(src_fta_fin)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_servico", "vl_receita_convenio")\
.filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]) &\
        (f.col("cd_centro_responsabilidade").startswith("304")) &\
        ~(f.col("cd_centro_responsabilidade").isin(304100101, 304110101)))\
.join(df_dim_reg, ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_servico_convenio_ssi",
            (f.col("vl_receita_servico") +
             f.col("vl_receita_convenio")))\
.drop("vl_receita_servico", "vl_receita_convenio", "cd_entidade_nacional")

# COMMAND ----------

if (df_fta_ssi.count() + df_fta_fin.count())==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

var_vl_columns_ssi = [column for column in df_fta_ssi.columns if column.startswith('vl_')]
var_vl_columns_fin = [column for column in df_fta_fin.columns if column.startswith('vl_')]

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

df_fta_ssi = melt(df_fta_ssi, var_keep_columns, var_vl_columns_ssi, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df_fta_fin = melt(df_fta_fin, var_keep_columns, var_vl_columns_fin, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df = df_fta_ssi\
.union(df_fta_fin.select(df_fta_ssi.columns))

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

# COMMAND ----------

