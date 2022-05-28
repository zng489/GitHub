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
# MAGIC Processo	biz_biz_fta_gestao_financeira_educ_basica_contnd _kpi_pivot
# MAGIC Tabela/Arquivo Origem	"/biz/orcamento/fta_gestao_financeira
# MAGIC /biz/orcamento/fta_despesa_rateada_negocio
# MAGIC /biz/orcamento/fta_receita_servico_convenio_rateada_negocio
# MAGIC /biz/orcamento/fta_gestao_financeira_educacao_basica_contnd
# MAGIC /biz/corporativo/dim_hierarquia_entidade_regional"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_educ_basica_contnd _kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas de Negócios rateadas por hora escolar para Educação Básica e Continuada SESI modelados para atender o consumo via Tableau
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
                          "/orcamento/fta_despesa_rateada_negocio",
                          "/orcamento/fta_receita_servico_convenio_rateada_negocio",
                          "/orcamento/fta_gestao_financeira_educacao_basica_contnd",
                          "/corporativo/dim_hierarquia_entidade_regional"],
              "destination": "/orcamento/fta_gestao_financeira_educ_basica_contnd_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_educ_basica_contnd_kpi_pivot"
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

src_fta_fin, src_fta_des, src_fta_rec, src_fta_edu, src_dim_reg  = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]]
print(src_fta_fin, src_fta_des, src_fta_rec, src_fta_edu, src_dim_reg)

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
# MAGIC Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha: cd_metrica,  vl_metrica , os mapeamentos diretos ou através de fórmulas (vl_metrica= <cálculo da métrica>) encontram se detalhados abaixo (a partir da linha 30) para a leitura
# MAGIC 
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade, cd_metrica, vl_metrica FROM fta_gestao_financeira_educacao_basica_contnd a
# MAGIC nesta tabela fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade uma vez que sua granularidade vai até cd_conta_contabil
# MAGIC LEFT JOIN fta_gestao_financeira b ON b.cd_ano_fechamento = a.cd_ano_fechamento AND b.cd_mes_fechamento = cd_mes_fechamento AND b.cd_entidade_regional = a.cd_entidade_regional AND b.cd_centro_responsabilidade = a.cd_centro_responsabilidade
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento  
# MAGIC nesta tabela fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional, cd_centro_responsabilidade uma vez que sua granularidade vai até cd_conta_contabil
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT a.cd_ano_fechamento, a.cd_mes_fechamento, a.dt_fechamento, a.cd_entidade_regional, a.cd_centro_responsabilidade, a.cd_metrica, a.vl_metrica FROM fta_receita_servico_convenio_rateada_negocio a
# MAGIC INNER JOIN dim_hierarquia_entidade_regional b ON b.cd_entidade_regional = a.cd_entidade_regional
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento AND b.cd_entidade_nacional = 2 AND cd_centro_responsabilidade  like '303%'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT a.cd_ano_fechamento, a.cd_mes_fechamento, a.dt_fechamento, a.cd_entidade_regional, a.cd_centro_responsabilidade, a.cd_metrica, a.vl_metrica FROM fta_despesa_rateada_negocio a
# MAGIC INNER JOIN dim_hierarquia_entidade_regional b ON b.cd_entidade_regional = a.cd_entidade_regional
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento AND b.cd_entidade_nacional = 2 AND cd_centro_responsabilidade  like '303%'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira (aux)

# COMMAND ----------

df_fta_fin = spark.read.parquet(src_fta_fin)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_servico",
        "vl_receita_convenio", "vl_receita_industrial", "vl_receita_apoio_financeiro", "vl_receita_outros", "vl_receita_subvencao_auxilio_extraordinario")\
.filter(((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) & (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"])))\
.fillna(0, subset=["vl_receita_servico",
                   "vl_receita_convenio",
                   "vl_receita_industrial",
                   "vl_receita_apoio_financeiro",
                   "vl_receita_outros",
                   "vl_receita_subvencao_auxilio_extraordinario"])\
.withColumn("vl_receita_servico_industrial_outra",
            f.col("vl_receita_servico") +
            f.col("vl_receita_convenio") +
            f.col("vl_receita_industrial") +
            f.col("vl_receita_apoio_financeiro") +
            (f.col("vl_receita_outros") -
             f.col("vl_receita_subvencao_auxilio_extraordinario")))\
.drop("vl_receita_servico", "vl_receita_convenio", "vl_receita_industrial", "vl_receita_apoio_financeiro", "vl_receita_outros", "vl_receita_subvencao_auxilio_extraordinario")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira_educacao_basica_contnd

# COMMAND ----------

df_fta_edu = spark.read.parquet(src_fta_edu)\
.filter(((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) & (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"])))\
.withColumn("vl_despcor_direta_educ_basica_contnd", 
            (f.col("vl_despcor_educ_basica_contnd") + 
             f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
             f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
             f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada")))\
.withColumn("vl_despcor_direta_educ_basica_contnd_gratuidade",
            (f.col("vl_despcor_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_vira_vida_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_etd_gestao_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade")))\
.withColumn("vl_despcor_educ_basica_contnd_total",
            (f.col("vl_despcor_educ_basica_contnd") + 
             f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
             f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
             f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") +
             f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +
             f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") +
             f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada")))\
.withColumn("vl_despcor_educ_basica_contnd_total_cta_pessoal", 
            f.when(f.col("cd_conta_contabil").startswith("310101"),
                   (f.col("vl_despcor_educ_basica_contnd") + 
                    f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_educ_basica_contnd_total_cta_material", 
            f.when(f.col("cd_conta_contabil").startswith("310103"),
                   (f.col("vl_despcor_educ_basica_contnd") + 
                    f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_educ_basica_contnd_total_cta_servterc", 
            f.when(f.col("cd_conta_contabil").startswith("310106"),
                   (f.col("vl_despcor_educ_basica_contnd") + 
                    f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_educ_basica_contnd_total_cta_outra", 
            f.when(~(f.col("cd_conta_contabil").startswith("310101") | f.col("cd_conta_contabil").startswith("310103") | f.col("cd_conta_contabil").startswith("310106")),
                   (f.col("vl_despcor_educ_basica_contnd") + 
                    f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") + 
                    f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") +
                    f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_desptot_educ_basica_contnd_total",
            (f.col("vl_desptot_educ_basica_contnd") + 
             f.col("vl_desptot_vira_vida_educ_basica_contnd_rateada") + 
             f.col("vl_desptot_etd_gestao_educ_basica_contnd_rateada") + 
             f.col("vl_desptot_suporte_negocio_educ_basica_contnd_rateada") +
             f.col("vl_desptot_indireta_gestao_educ_basica_contnd_rateada") +
             f.col("vl_desptot_indireta_apoio_educ_basica_contnd_rateada") +
             f.col("vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada")))\
.withColumn("vl_desptot_educ_basica_contnd_total_gratuidade",
            (f.col("vl_desptot_educ_basica_contnd_gratuidade") + 
             f.col("vl_desptot_vira_vida_educ_basica_contnd_gratuidade") + 
             f.col("vl_desptot_etd_gestao_educ_basica_contnd_gratuidade") + 
             f.col("vl_desptot_suporte_negocio_educ_basica_contnd_gratuidade") +
             f.col("vl_desptot_indireta_gestao_educ_basica_contnd_gratuidade") +
             f.col("vl_desptot_indireta_apoio_educ_basica_contnd_gratuidade") +
             f.col("vl_desptot_indireta_desenv_inst_educ_basica_contnd_gratuidade")))\
.withColumn("vl_despcor_educ_basica_contnd_total_gratuidade",
            (f.col("vl_despcor_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_vira_vida_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_etd_gestao_educ_basica_contnd_gratuidade") + 
             f.col("vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade") +
             f.col("vl_despcor_indireta_gestao_educ_basica_contnd_gratuidade") +
             f.col("vl_despcor_indireta_apoio_educ_basica_contnd_gratuidade") +
             f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_gratuidade")))\
.drop("vl_desptot_vira_vida_educ_basica_contnd_rateada",
      "vl_desptot_etd_gestao_educ_basica_contnd_rateada",
      "vl_desptot_suporte_negocio_educ_basica_contnd_rateada",
      "vl_desptot_indireta_gestao_educ_basica_contnd_rateada",
      "vl_desptot_indireta_apoio_educ_basica_contnd_rateada",
      "vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada",
      "vl_desptot_educ_basica_contnd",
      "vl_desptot_educ_basica_contnd_gratuidade",
      "vl_desptot_vira_vida_educ_basica_contnd_gratuidade",
      "vl_desptot_etd_gestao_educ_basica_contnd_gratuidade",
      "vl_desptot_suporte_negocio_educ_basica_contnd_gratuidade",
      "vl_desptot_indireta_gestao_educ_basica_contnd_gratuidade",
      "vl_desptot_indireta_apoio_educ_basica_contnd_gratuidade",
      "vl_desptot_indireta_desenv_inst_educ_basica_contnd_gratuidade",
      "vl_despcor_educ_basica_contnd_gratuidade",
      "vl_despcor_vira_vida_educ_basica_contnd_gratuidade",
      "vl_despcor_etd_gestao_educ_basica_contnd_gratuidade",
      "vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade",
      "vl_despcor_indireta_gestao_educ_basica_contnd_gratuidade",
      "vl_despcor_indireta_apoio_educ_basica_contnd_gratuidade",
      "vl_despcor_indireta_desenv_inst_educ_basica_contnd_gratuidade",
      "cd_conta_contabil",
      "dh_insercao_biz",
      "kv_process_control")\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("vl_despcor_educ_basica_contnd").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd"),
     f.sum("vl_despcor_vira_vida_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_vira_vida_educ_basica_contnd"),
     f.sum("vl_despcor_etd_gestao_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_etd_gestao_educ_basica_contnd"),
     f.sum("vl_despcor_suporte_negocio_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_suporte_negocio_educ_basica_contnd"),
     f.sum("vl_despcor_indireta_gestao_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_indireta_gestao_educ_basica_contnd"),
     f.sum("vl_despcor_indireta_apoio_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_indireta_apoio_educ_basica_contnd"),
     f.sum("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada").cast("decimal(18,6)").alias("vl_despcor_indireta_desenv_inst_educ_basica_contnd"),
     f.sum("vl_despcor_direta_educ_basica_contnd").cast("decimal(18,6)").alias("vl_despcor_direta_educ_basica_contnd"),
     f.sum("vl_despcor_direta_educ_basica_contnd_gratuidade").cast("decimal(18,6)").alias("vl_despcor_direta_educ_basica_contnd_gratuidade"),
     f.sum("vl_despcor_educ_basica_contnd_total").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total"),     
     f.sum("vl_despcor_educ_basica_contnd_total_cta_pessoal").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total_cta_pessoal"),
     f.sum("vl_despcor_educ_basica_contnd_total_cta_material").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total_cta_material"),
     f.sum("vl_despcor_educ_basica_contnd_total_cta_servterc").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total_cta_servterc"),
     f.sum("vl_despcor_educ_basica_contnd_total_cta_outra").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total_cta_outra"),
     f.sum("vl_desptot_educ_basica_contnd_total").cast("decimal(18,6)").alias("vl_desptot_educ_basica_contnd_total"),
     f.sum("vl_desptot_educ_basica_contnd_total_gratuidade").cast("decimal(18,6)").alias("vl_desptot_educ_basica_contnd_total_gratuidade"),
     f.sum("vl_despcor_educ_basica_contnd_total_gratuidade").cast("decimal(18,6)").alias("vl_despcor_educ_basica_contnd_total_gratuidade"))\
.join(df_fta_fin, ["cd_ano_fechamento", "cd_mes_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0, subset=["vl_receita_servico_industrial_outra",
                   "vl_despcor_educ_basica_contnd",
                   "vl_despcor_vira_vida_educ_basica_contnd",
                   "vl_despcor_etd_gestao_educ_basica_contnd",
                   "vl_despcor_suporte_negocio_educ_basica_contnd",
                   "vl_despcor_indireta_gestao_educ_basica_contnd",
                   "vl_despcor_indireta_apoio_educ_basica_contnd",
                   "vl_despcor_indireta_desenv_inst_educ_basica_contnd",
                   "vl_despcor_direta_educ_basica_contnd",
                   "vl_despcor_direta_educ_basica_contnd_gratuidade",
                   "vl_despcor_educ_basica_contnd_total",
                   "vl_despcor_educ_basica_contnd_total_cta_pessoal",
                   "vl_despcor_educ_basica_contnd_total_cta_material",
                   "vl_despcor_educ_basica_contnd_total_cta_servterc",
                   "vl_despcor_educ_basica_contnd_total_cta_outra",
                   "vl_desptot_educ_basica_contnd_total",
                   "vl_desptot_educ_basica_contnd_total_gratuidade",
                   "vl_despcor_educ_basica_contnd_total_gratuidade"])\
.withColumn("vl_desptot_educ_basica_contnd_total_liq", 
            f.when(f.col("vl_desptot_educ_basica_contnd_total") > f.col("vl_receita_servico_industrial_outra"),
                   f.col("vl_desptot_educ_basica_contnd_total") -
                   f.col("vl_receita_servico_industrial_outra"))\
            .otherwise(f.lit(0)))\
.drop("vl_receita_servico_industrial_outra")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_entidade_regional (aux)

# COMMAND ----------

df_dim_reg = spark.read.parquet(src_dim_reg)\
.select("cd_entidade_regional", "cd_entidade_nacional")\
.filter(f.col("cd_entidade_nacional") == 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_receita_servico_convenio_rateada_negocio

# COMMAND ----------

df_fta_rec = spark.read.parquet(src_fta_rec)\
.filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]) &\
        (f.col("cd_centro_responsabilidade").startswith("303")))\
.join(df_dim_reg, ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_servico_convenio_educacao_sesi_total",
            (f.col("vl_receita_servico_convenio") + 
             f.col("vl_receita_vira_vida_rateada") +              
             f.col("vl_receita_etd_gestao_educacao_rateada") +
             f.col("vl_receita_suporte_negocio_rateada") +
             f.col("vl_receita_indireta_gestao_rateada") +
             f.col("vl_receita_indireta_desenv_institucional_rateada") +             
             f.col("vl_receita_indireta_apoio_rateada")))\
.drop("vl_receita_servico_convenio",
      "vl_receita_vira_vida_rateada",
      "vl_receita_olimpiada_rateada",
      "vl_receita_etd_gestao_educacao_rateada",
      "vl_receita_etd_gestao_outros_rateada",
      "vl_receita_suporte_negocio_rateada",
      "vl_receita_indireta_gestao_rateada",
      "vl_receita_indireta_desenv_institucional_rateada",
      "vl_receita_indireta_apoio_rateada",      
      "cd_entidade_nacional",
      "dh_insercao_biz",
      "kv_process_control")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_despesa_rateada_negocio

# COMMAND ----------

df_fta_des = spark.read.parquet(src_fta_des)\
.filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]) &\
        (f.col("cd_centro_responsabilidade").startswith("303")))\
.join(df_dim_reg, ["cd_entidade_regional"], "inner")\
.withColumn("vl_despcor_direta_educacao_sesi",
            (f.col("vl_despesa_corrente") + 
             f.col("vl_despcor_vira_vida_rateada") +              
             f.col("vl_despcor_etd_gestao_educacao_rateada") +
             f.col("vl_despcor_suporte_negocio_rateada")))\
.withColumn("vl_despcor_educacao_sesi_total",
            (f.col("vl_despesa_corrente") + 
             f.col("vl_despcor_vira_vida_rateada") +              
             f.col("vl_despcor_etd_gestao_educacao_rateada") +
             f.col("vl_despcor_suporte_negocio_rateada") +
             f.col("vl_despcor_indireta_gestao_rateada") +              
             f.col("vl_despcor_indireta_desenv_institucional_rateada") +
             f.col("vl_despcor_indireta_apoio_rateada")))\
.withColumn("vl_desptot_direta_educacao_sesi",
            (f.col("vl_despesa_total") + 
             f.col("vl_desptot_vira_vida_rateada") +              
             f.col("vl_desptot_etd_gestao_educacao_rateada") +
             f.col("vl_desptot_suporte_negocio_rateada")))\
.withColumnRenamed("vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_gestao_educacao_sesi")\
.withColumnRenamed("vl_desptot_indireta_apoio_rateada", "vl_desptot_indireta_apoio_educacao_sesi")\
.withColumnRenamed("vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_desenv_inst_educacao_sesi")\
.drop("vl_despesa_corrente",
      "vl_despcor_vira_vida_rateada",
      "vl_despcor_olimpiada_rateada",
      "vl_despcor_etd_gestao_educacao_rateada",
      "vl_despcor_etd_gestao_outros_rateada",
      "vl_despcor_suporte_negocio_rateada",
      "vl_despcor_indireta_gestao_rateada",
      "vl_despcor_indireta_desenv_institucional_rateada",
      "vl_despcor_indireta_apoio_rateada",      
      "vl_despesa_total",
      "vl_desptot_vira_vida_rateada",
      "vl_desptot_olimpiada_rateada",
      "vl_desptot_etd_gestao_educacao_rateada",
      "vl_desptot_etd_gestao_outros_rateada",
      "vl_desptot_suporte_negocio_rateada",        
      "cd_entidade_nacional",
      "dh_insercao_biz",
      "kv_process_control")

# COMMAND ----------

if (df_fta_edu.count() + df_fta_des.count() + df_fta_rec.count())==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

var_vl_columns_edu = [column for column in df_fta_edu.columns if column.startswith('vl_')]
var_vl_columns_rec = [column for column in df_fta_rec.columns if column.startswith('vl_')]
var_vl_columns_des = [column for column in df_fta_des.columns if column.startswith('vl_')]

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

df_fta_edu = melt(df_fta_edu, var_keep_columns, var_vl_columns_edu, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df_fta_rec = melt(df_fta_rec, var_keep_columns, var_vl_columns_rec, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df_fta_des = melt(df_fta_des, var_keep_columns, var_vl_columns_des, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df = df_fta_edu\
.union(df_fta_rec.select(df_fta_edu.columns))\
.union(df_fta_des.select(df_fta_edu.columns))

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

# COMMAND ----------

