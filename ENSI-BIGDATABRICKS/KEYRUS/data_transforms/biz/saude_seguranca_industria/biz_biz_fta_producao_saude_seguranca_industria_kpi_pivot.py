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
# MAGIC Processo	biz_biz_fta_producao_saude_seguranca_industria_kpi_pivot
# MAGIC Tabela/Arquivo Origem	/biz/producao/fte_producao_saude_seguranca_industria
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_producao_saude_seguranca_industria
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção de pessoas atendidas pelos serviços de saúde e segurança do trabalhador.
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import json
import re
from datetime import datetime, timedelta, date
import collections

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))


# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables = {"origins": ["/producao/fte_producao_saude_seguranca_industria"],
              "destination": "/producao/fta_producao_saude_seguranca_industria_kpi_pivot",
              "databricks": {
                "notebook": "/biz/saude_seguranca_industria/biz_biz_fta_producao_saude_seguranca_industria_kpi_pivot"
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
       'adf_pipeline_name': 'biz_biz_fta_producao_saude_seguranca_trabalhador_kpi_pivot',
       'adf_pipeline_run_id': 'development',
       'adf_trigger_id': 'development',
       'adf_trigger_name': 'thomaz',
       'adf_trigger_time': '2020-07-31T13:15:00.0000000Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 6, "dt_closing": "2020-07-17"}}
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

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, first, desc, asc, count, coalesce, create_map, explode
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf
from pyspark.sql import DataFrame
from typing import Iterable 
from itertools import chain


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
# MAGIC ### Looking documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Efetuar um pivoteamento aonde cada valor corresponda a uma metrica por linha
# MAGIC 
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional,  cd_porte_empresa, cd_metrica, vl_metrica 
# MAGIC FROM fte_producao_saude_seguranca_industria, fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, cd_entidade_regional,  cd_porte_empresa uma vez que sua granularidade 
# MAGIC vai até cd_documento_empresa_atendida_calc, cd_cpf_pessoa_atendida
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT cd_ano_fechamento,cd_mes_fechamento, dt_fechamento, -2,  cd_porte_empresa, cd_metrica, vl_metrica 
# MAGIC FROM fte_producao_saude_seguranca_industria, fazendo o SUM(valor) com GROUP BY por cd_ano_fechamento,cd_mes_fechamento, dt_fechamento,-2,  cd_porte_empresa uma vez que sua granularidade 
# MAGIC vai até cd_documento_empresa_atendida_calc, cd_cpf_pessoa_atendida
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC 
# MAGIC ---- Métrica de Empresas / Indústrias
# MAGIC 
# MAGIC ONDE:
# MAGIC 
# MAGIC cd_metrica = 'qt_empresa_atendida'    
# MAGIC vl_metrica =
# MAGIC WHEN 
# MAGIC     fl_empresa_cadastro_especifico_inss = 0     
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade LIKE'304%' 
# MAGIC and cd_centro_responsabilidade NOT LIKE'30412%'
# MAGIC and cd_centro_responsabilidade NOT IN ('304070110')
# MAGIC THEN
# MAGIC COUNT(distinct cd_documento_empresa_atendida_calc)
# MAGIC 
# MAGIC cd_metrica = 'qt_industria_atendida' 		 
# MAGIC vl_metrica =
# MAGIC WHEN fl_empresa_cadastro_especifico_inss = 0
# MAGIC and cd_porte_empresa  IS NOT NULL   
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and fl_industria = 1
# MAGIC and cd_centro_responsabilidade LIKE'304%'  
# MAGIC and cd_centro_responsabilidade not like '30412%'
# MAGIC and cd_centro_responsabilidade not in ('304070110')
# MAGIC THEN
# MAGIC COUNT(distinct cd_documento_empresa_atendida_calc)
# MAGIC 
# MAGIC cd_metrica = 'qt_industria_atendida_sst_prom_saude' 
# MAGIC WHEN
# MAGIC  fl_empresa_cadastro_especifico_inss = 0
# MAGIC and cd_porte_empresa  IS NOT NULL   
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and fl_industria = 1
# MAGIC and cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,
# MAGIC 304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,
# MAGIC 304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC SUM(qt_industria_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_industria_atendida_sst' 
# MAGIC WHEN cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)
# MAGIC THEN
# MAGIC SUM(qt_industria_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_indústria_atendida_promocao_saude'  
# MAGIC WHEN cd_centro_responsabilidade in (304030114,304030117,304030119,304030120,304030121,
# MAGIC 304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC SUM(qt_industria_atendida)
# MAGIC 
# MAGIC ----- Metricas que existem na fte:
# MAGIC 
# MAGIC WHEN
# MAGIC cd_metrica = 'qt_vacina_aplicada' 
# MAGIC and cd_centro_responsabilidade = '304030119'
# MAGIC THEN
# MAGIC SUM(vl_metrica)
# MAGIC 
# MAGIC WHEN
# MAGIC cd_metrica = 'qt_pessoa_beneficiada_contrato'
# MAGIC and cd_centro_responsabilidade like '30401%' 
# MAGIC THEN	
# MAGIC SUM(vl_metrica)
# MAGIC 
# MAGIC ---- Métrica de Trabalhadores     ---- Não SUMARIZAR POR PORTE ---- ATRIBUIR CD_PORTE_EMPRESA = -99
# MAGIC cd_metrica = 'qt_pessoa_atendida'    --- atualmente somente SESI VIVA+ cr 304070110
# MAGIC vl_metrica =
# MAGIC WHEN STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and fl_industria = 1  -- Indústria     
# MAGIC and cd_porte_empresa IS NOT NULL  
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL  
# MAGIC and cd_centro_responsabilidade  IN ('304070110')  
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC ----------------------------------------------
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido'    
# MAGIC vl_metrica =
# MAGIC WHEN cd_porte_empresa IS NOT NULL  
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade LIKE'304%'
# MAGIC and cd_centro_responsabilidade NOT LIKE'30412%'
# MAGIC and cd_centro_responsabilidade NOT IN ('304070110')
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_industria'    
# MAGIC vl_metrica =
# MAGIC WHEN fl_industria = 1  -- Indústria
# MAGIC and cd_porte_empresa IS NOT NULL  
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade LIKE'304%'
# MAGIC and cd_centro_responsabilidade NOT LIKE'30412%'
# MAGIC and cd_centro_responsabilidade NOT IN ('304070110')
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_nao_industria'    
# MAGIC vl_metrica =
# MAGIC WHEN fl_industria = 0 -- Não indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade LIKE'304%'
# MAGIC and cd_centro_responsabilidade NOT LIKE'30412%'
# MAGIC and cd_centro_responsabilidade NOT IN ('304070110')
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_industria_SST_prom_saude'    
# MAGIC WHEN fl_industria = 1 -- Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,
# MAGIC 304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,
# MAGIC 304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_industria_SST' 
# MAGIC WHEN fl_industria = 1 -- Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and  cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_industria_promocao_saude'  
# MAGIC WHEN fl_industria = 1 -- Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade in (304030114,304030117,304030119,304030120,304030121,
# MAGIC 304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_nao_industria_SST_prom_saude'    
# MAGIC WHEN fl_industria = 0 -- Não Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and  cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,
# MAGIC 304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,
# MAGIC 304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_nao_industria_SST' 
# MAGIC WHEN fl_industria = 0 -- Não Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade in (304010201,304010202,304010203,304010204,304010205,
# MAGIC 304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
# MAGIC 304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC 
# MAGIC cd_metrica = 'qt_trabalhador_atendido_nao_industria_promocao_saude'  
# MAGIC WHEN fl_industria = 0 -- Não Indústria
# MAGIC and cd_porte_empresa IS NOT NULL   
# MAGIC and STR(cd_cpf_pessoa_atendida) > '0' 
# MAGIC and cd_cpf_pessoa_atendida IS NOT NULL 
# MAGIC and cd_clientela_oltp = 50  
# MAGIC and cd_documento_empresa_atendida_calc IS NOT NULL
# MAGIC and cd_centro_responsabilidade in (304030114,304030117,304030119,304030120,304030121,
# MAGIC 304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)
# MAGIC THEN
# MAGIC COUNT(distinct cd_cpf_pessoa_atendida)
# MAGIC ```

# COMMAND ----------

var_columns_fta_producao_ssi = ['cd_entidade_regional',
                                'cd_centro_responsabilidade',
                                'cd_documento_empresa_atendida_calc',
                                'fl_empresa_cadastro_especifico_inss',
                                'cd_porte_empresa',
                                'fl_industria',
                                'cd_clientela_oltp',
                                'cd_cpf_pessoa_atendida',
                                'cd_metrica',
                                'vl_metrica',
                                'dt_fechamento',
                                'cd_ano_fechamento',
                                'cd_mes_fechamento']

# COMMAND ----------

df_fta_ssi = spark.read.parquet(src_pro)\
.select(*var_columns_fta_producao_ssi)\
.filter((col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))\
.coalesce(4)\
.cache()

df_fta_ssi.count()

# COMMAND ----------

if df_fta_ssi.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

#Metrics that already exist by "cd_entidade_regional" (filtering by "qt_vacina_aplicada" and "qt_pessoa_beneficiada_contrato")
df_metrics_alredy_exists__ers = df_fta_ssi\
.filter((( col("cd_metrica") == "qt_vacina_aplicada") &\
         (col("cd_centro_responsabilidade") == "304030119")) |\
        (( col("cd_metrica") == "qt_pessoa_beneficiada_contrato") &\
         (col("cd_centro_responsabilidade").startswith("30401"))))\
.drop("cd_documento_empresa_atendida_calc", "fl_empresa_cadastro_especifico_inss", "fl_industria", "cd_clientela_oltp", "cd_cpf_pessoa_atendida", "cd_centro_responsabilidade")\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa", "cd_metrica")\
.agg(sum("vl_metrica").cast("int").alias("vl_metrica"))\
.coalesce(1)\
.cache()

df_metrics_alredy_exists__ers.count()

# COMMAND ----------

#Metrics that already exist fixing -2 to "cd_entidade_regional" 
df_metrics_alredy_exists__er_2 = df_metrics_alredy_exists__ers\
.withColumn("cd_entidade_regional", lit(-2))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa", "cd_metrica")\
.agg(sum("vl_metrica").cast("int").alias("vl_metrica"))\
.coalesce(1)\
.cache()

df_metrics_alredy_exists__er_2.count()

# COMMAND ----------

#Union of metrics that already exists (by cd_entidade_regional  and by cd_entidade_regional = -2)
df_metrics_alredy_exists = df_metrics_alredy_exists__ers.union(df_metrics_alredy_exists__er_2.select(df_metrics_alredy_exists__ers.columns))\
.coalesce(1)\
.cache()

df_metrics_alredy_exists.count()

# COMMAND ----------

df_metrics_alredy_exists__ers = df_metrics_alredy_exists__ers.unpersist()
df_metrics_alredy_exists__er_2 = df_metrics_alredy_exists__er_2.unpersist()

# COMMAND ----------

#Calculating new company and industry metrics
df_new_metrics_emp_ind = df_fta_ssi\
.select("cd_entidade_regional",
        "cd_centro_responsabilidade",         
        "cd_documento_empresa_atendida_calc",
        "fl_empresa_cadastro_especifico_inss",
        "cd_porte_empresa",   
        "fl_industria",
        "cd_clientela_oltp",
        "cd_ano_fechamento",
        "cd_mes_fechamento",
        "dt_fechamento")\
.distinct()\
.withColumn("qt_empresa_atendida",
            when((col("fl_empresa_cadastro_especifico_inss") == 0) &\
                 ~(col("cd_porte_empresa").isNull()) &\
                 ~(col("cd_documento_empresa_atendida_calc").isNull()) &\
                 (col("cd_centro_responsabilidade").startswith("304")) &\
                 ~(col("cd_centro_responsabilidade").startswith("30412")) &\
                 ~(col("cd_centro_responsabilidade").isin("304070110")),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_industria_atendida",
            when((col("fl_empresa_cadastro_especifico_inss") == 0) &\
                 ~(col("cd_porte_empresa").isNull()) &\
                 ~(col("cd_documento_empresa_atendida_calc").isNull()) &\
                 (col("fl_industria")==1) &\
                 (col("cd_centro_responsabilidade").startswith("304")) &\
                 ~(col("cd_centro_responsabilidade").startswith("30412")) &\
                 ~(col("cd_centro_responsabilidade").isin("304070110")),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_industria_atendida_sst_prom_saude",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,304030114,304030117,304030119,304030120,304030121,
                                                         304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)) &\
                 (col("qt_industria_atendida") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_industria_atendida_sst",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)) &\
                 (col("qt_industria_atendida") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_indústria_atendida_promocao_saude",
            when((col("cd_centro_responsabilidade").isin(304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,
                                                        304070106,304070108)) &\
                 (col("qt_industria_atendida") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_pessoa_atendida", lit(0))\
.withColumn("qt_trabalhador_atendido", lit(0))\
.withColumn("qt_trabalhador_atendido_industria", lit(0))\
.withColumn("qt_trabalhador_atendido_nao_industria", lit(0))\
.withColumn("qt_trabalhador_atendido_industria_sst_prom_saude", lit(0))\
.withColumn("qt_trabalhador_atendido_industria_sst", lit(0))\
.withColumn("qt_trabalhador_atendido_industria_promocao_saude", lit(0))\
.withColumn("qt_trabalhador_atendido_nao_industria_sst_prom_saude", lit(0))\
.withColumn("qt_trabalhador_atendido_nao_industria_sst", lit(0))\
.withColumn("qt_trabalhador_atendido_nao_industria_promocao_saude", lit(0))\
.drop("fl_empresa_cadastro_especifico_inss", "fl_industria", "cd_clientela_oltp", "cd_centro_responsabilidade")\
.coalesce(1)\
.cache()

df_new_metrics_emp_ind.count()

# COMMAND ----------

#New company and industry metrics by "cd_entidade_regional" (distinct by "cd_documento_empresa_atendida_calc")
df_new_metrics_emp_ind__ers = df_new_metrics_emp_ind\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento","cd_entidade_regional", "cd_porte_empresa", "cd_documento_empresa_atendida_calc")\
.agg(max("qt_empresa_atendida").cast("int").alias("qt_empresa_atendida"),
     max("qt_industria_atendida").cast("int").alias("qt_industria_atendida"),
     max("qt_industria_atendida_sst_prom_saude").cast("int").alias("qt_industria_atendida_sst_prom_saude"),
     max("qt_industria_atendida_sst").cast("int").alias("qt_industria_atendida_sst"),
     max("qt_indústria_atendida_promocao_saude").cast("int").alias("qt_indústria_atendida_promocao_saude"),
     max("qt_pessoa_atendida").cast("int").alias("qt_pessoa_atendida"),    
     max("qt_trabalhador_atendido").cast("int").alias("qt_trabalhador_atendido"),    
     max("qt_trabalhador_atendido_industria").cast("int").alias("qt_trabalhador_atendido_industria"),
     max("qt_trabalhador_atendido_nao_industria").cast("int").alias("qt_trabalhador_atendido_nao_industria"),
     max("qt_trabalhador_atendido_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_industria_sst").cast("int").alias("qt_trabalhador_atendido_industria_sst"),
     max("qt_trabalhador_atendido_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_industria_promocao_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst"),
     max("qt_trabalhador_atendido_nao_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_promocao_saude"))\
.drop("cd_documento_empresa_atendida_calc")\
.coalesce(4)\
.cache()

df_new_metrics_emp_ind__ers.count()

# COMMAND ----------

#New company and industry metrics fixing -2 to "cd_entidade_regional" (distinct by "cd_documento_empresa_atendida_calc")
df_new_metrics_emp_ind__er_2 = df_new_metrics_emp_ind\
.withColumn("cd_entidade_regional", lit(-2))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa", "cd_documento_empresa_atendida_calc")\
.agg(max("qt_empresa_atendida").cast("int").alias("qt_empresa_atendida"),
     max("qt_industria_atendida").cast("int").alias("qt_industria_atendida"),
     max("qt_industria_atendida_sst_prom_saude").cast("int").alias("qt_industria_atendida_sst_prom_saude"),
     max("qt_industria_atendida_sst").cast("int").alias("qt_industria_atendida_sst"),
     max("qt_indústria_atendida_promocao_saude").cast("int").alias("qt_indústria_atendida_promocao_saude"),
     max("qt_pessoa_atendida").cast("int").alias("qt_pessoa_atendida"),    
     max("qt_trabalhador_atendido").cast("int").alias("qt_trabalhador_atendido"),    
     max("qt_trabalhador_atendido_industria").cast("int").alias("qt_trabalhador_atendido_industria"),
     max("qt_trabalhador_atendido_nao_industria").cast("int").alias("qt_trabalhador_atendido_nao_industria"),
     max("qt_trabalhador_atendido_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_industria_sst").cast("int").alias("qt_trabalhador_atendido_industria_sst"),
     max("qt_trabalhador_atendido_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_industria_promocao_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst"),
     max("qt_trabalhador_atendido_nao_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_promocao_saude"))\
.drop("cd_documento_empresa_atendida_calc")\
.coalesce(4)\
.cache()

df_new_metrics_emp_ind__er_2.count()

# COMMAND ----------

df_new_metrics_emp_ind = df_new_metrics_emp_ind.unpersist()

# COMMAND ----------

#Calculating new worker metrics (fixing -99 to "cd_porte_empresa")
df_new_metrics_trab = df_fta_ssi\
.select("cd_entidade_regional",
        "cd_centro_responsabilidade",         
        "cd_documento_empresa_atendida_calc",
        "fl_empresa_cadastro_especifico_inss",
        "cd_porte_empresa",   
        "fl_industria",
        "cd_clientela_oltp",
        "cd_cpf_pessoa_atendida",
        "cd_ano_fechamento",
        "cd_mes_fechamento",
        "dt_fechamento")\
.distinct()\
.withColumn("qt_empresa_atendida", lit(0))\
.withColumn("qt_industria_atendida", lit(0))\
.withColumn("qt_industria_atendida_sst_prom_saude", lit(0))\
.withColumn("qt_industria_atendida_sst", lit(0))\
.withColumn("qt_indústria_atendida_promocao_saude", lit(0))\
.withColumn("qt_pessoa_atendida",
            when((col("cd_cpf_pessoa_atendida") > 0) &\
                 ~(col("cd_cpf_pessoa_atendida").isNull()) &\
                 (col("fl_industria")==1) &\
                 ~(col("cd_porte_empresa").isNull()) &\
                 (col("cd_clientela_oltp")==50) &\
                 ~(col("cd_documento_empresa_atendida_calc").isNull()) &\
                 (col("cd_centro_responsabilidade")=="304070110"),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido",
            when((col("cd_cpf_pessoa_atendida") > 0) &\
                 ~(col("cd_cpf_pessoa_atendida").isNull()) &\
                 ~(col("cd_porte_empresa").isNull()) &\
                 (col("cd_clientela_oltp")==50) &\
                 ~(col("cd_documento_empresa_atendida_calc").isNull()) &\
                 (col("cd_centro_responsabilidade").startswith("304")) &\
                 ~(col("cd_centro_responsabilidade").startswith("30412")) &\
                 ~(col("cd_centro_responsabilidade").isin("304070110")),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_industria",
            when((col("fl_industria")==1) &\
                 (col("qt_trabalhador_atendido")==1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_nao_industria",
            when((col("fl_industria")==0) &\
                 (col("qt_trabalhador_atendido")==1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_industria_sst_prom_saude",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,304030114,304030117,304030119,304030120,304030121,
                                                         304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)) &\
                 (col("qt_trabalhador_atendido_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_industria_sst",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)) &\
                 (col("qt_trabalhador_atendido_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_industria_promocao_saude",
            when((col("cd_centro_responsabilidade").isin(304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,
                                                        304070106,304070108)) &\
                 (col("qt_trabalhador_atendido_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_nao_industria_sst_prom_saude",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108,304030114,304030117,304030119,304030120,304030121,
                                                         304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,304070106,304070108)) &\
                 (col("qt_trabalhador_atendido_nao_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_nao_industria_sst",
            when((col("cd_centro_responsabilidade").isin(304010201,304010202,304010203,304010204,304010205,304010206,304010207,304010208,304010209,304070101,304070102,304070107,304070109,
                                                         304080101,304080102,304080103,304080104,304080105,304080106,304080107,304080108)) &\
                 (col("qt_trabalhador_atendido_nao_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_trabalhador_atendido_nao_industria_promocao_saude",
            when((col("cd_centro_responsabilidade").isin(304030114,304030117,304030119,304030120,304030121,304030122,304030123,304030124,304030125,304030126,304070103,304070104,304070105,
                                                        304070106,304070108)) &\
                 (col("qt_trabalhador_atendido_nao_industria") == 1),
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("cd_porte_empresa", lit(-99))\
.drop("cd_documento_empresa_atendida_calc", "fl_empresa_cadastro_especifico_inss", "fl_industria", "cd_clientela_oltp", "cd_centro_responsabilidade")\
.coalesce(4)\
.cache()

df_new_metrics_trab.count()

# COMMAND ----------

df_fta_ssi = df_fta_ssi.unpersist()

# COMMAND ----------

#New worker by "cd_entidade_regional" (distinct by "cd_cpf_pessoa_atendida")
df_new_metrics_trab__ers = df_new_metrics_trab\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento","cd_entidade_regional", "cd_porte_empresa", "cd_cpf_pessoa_atendida")\
.agg(max("qt_empresa_atendida").cast("int").alias("qt_empresa_atendida"),
     max("qt_industria_atendida").cast("int").alias("qt_industria_atendida"),
     max("qt_industria_atendida_sst_prom_saude").cast("int").alias("qt_industria_atendida_sst_prom_saude"),
     max("qt_industria_atendida_sst").cast("int").alias("qt_industria_atendida_sst"),
     max("qt_indústria_atendida_promocao_saude").cast("int").alias("qt_indústria_atendida_promocao_saude"),
     max("qt_pessoa_atendida").cast("int").alias("qt_pessoa_atendida"),    
     max("qt_trabalhador_atendido").cast("int").alias("qt_trabalhador_atendido"),    
     max("qt_trabalhador_atendido_industria").cast("int").alias("qt_trabalhador_atendido_industria"),
     max("qt_trabalhador_atendido_nao_industria").cast("int").alias("qt_trabalhador_atendido_nao_industria"),
     max("qt_trabalhador_atendido_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_industria_sst").cast("int").alias("qt_trabalhador_atendido_industria_sst"),
     max("qt_trabalhador_atendido_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_industria_promocao_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst"),
     max("qt_trabalhador_atendido_nao_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_promocao_saude"))\
.drop("cd_cpf_pessoa_atendida")\
.coalesce(4)\
.cache()

df_new_metrics_trab__ers.count()

# COMMAND ----------

#New worker metrics fixing -2 to "cd_entidade_regional" (distinct by "cd_cpf_pessoa_atendida")
df_new_metrics_trab__er_2 = df_new_metrics_trab\
.withColumn("cd_entidade_regional", lit(-2))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa", "cd_cpf_pessoa_atendida")\
.agg(max("qt_empresa_atendida").cast("int").alias("qt_empresa_atendida"),
     max("qt_industria_atendida").cast("int").alias("qt_industria_atendida"),
     max("qt_industria_atendida_sst_prom_saude").cast("int").alias("qt_industria_atendida_sst_prom_saude"),
     max("qt_industria_atendida_sst").cast("int").alias("qt_industria_atendida_sst"),
     max("qt_indústria_atendida_promocao_saude").cast("int").alias("qt_indústria_atendida_promocao_saude"),
     max("qt_pessoa_atendida").cast("int").alias("qt_pessoa_atendida"),    
     max("qt_trabalhador_atendido").cast("int").alias("qt_trabalhador_atendido"),    
     max("qt_trabalhador_atendido_industria").cast("int").alias("qt_trabalhador_atendido_industria"),
     max("qt_trabalhador_atendido_nao_industria").cast("int").alias("qt_trabalhador_atendido_nao_industria"),
     max("qt_trabalhador_atendido_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_industria_sst").cast("int").alias("qt_trabalhador_atendido_industria_sst"),
     max("qt_trabalhador_atendido_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_industria_promocao_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst_prom_saude"),
     max("qt_trabalhador_atendido_nao_industria_sst").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst"),
     max("qt_trabalhador_atendido_nao_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_promocao_saude"))\
.drop("cd_cpf_pessoa_atendida")\
.coalesce(4)\
.cache()

df_new_metrics_trab__er_2.count()

# COMMAND ----------

df_new_metrics_trab = df_new_metrics_trab.unpersist()

# COMMAND ----------

#Union of new company, industry and worker metrics (by cd_entidade_regional and by cd_entidade_regional = -2)
df_new_metrics = df_new_metrics_emp_ind__ers\
.union(df_new_metrics_emp_ind__er_2.select(df_new_metrics_emp_ind__ers.columns))\
.union(df_new_metrics_trab__ers.select(df_new_metrics_emp_ind__ers.columns))\
.union(df_new_metrics_trab__er_2.select(df_new_metrics_emp_ind__ers.columns))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa")\
.agg(sum("qt_empresa_atendida").cast("int").alias("qt_empresa_atendida"),
     sum("qt_industria_atendida").cast("int").alias("qt_industria_atendida"),
     sum("qt_industria_atendida_sst_prom_saude").cast("int").alias("qt_industria_atendida_sst_prom_saude"),
     sum("qt_industria_atendida_sst").cast("int").alias("qt_industria_atendida_sst"),
     sum("qt_indústria_atendida_promocao_saude").cast("int").alias("qt_indústria_atendida_promocao_saude"),
     sum("qt_pessoa_atendida").cast("int").alias("qt_pessoa_atendida"),     
     sum("qt_trabalhador_atendido").cast("int").alias("qt_trabalhador_atendido"),     
     sum("qt_trabalhador_atendido_industria").cast("int").alias("qt_trabalhador_atendido_industria"),
     sum("qt_trabalhador_atendido_nao_industria").cast("int").alias("qt_trabalhador_atendido_nao_industria"),
     sum("qt_trabalhador_atendido_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_industria_sst_prom_saude"),
     sum("qt_trabalhador_atendido_industria_sst").cast("int").alias("qt_trabalhador_atendido_industria_sst"),
     sum("qt_trabalhador_atendido_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_industria_promocao_saude"),
     sum("qt_trabalhador_atendido_nao_industria_sst_prom_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst_prom_saude"),
     sum("qt_trabalhador_atendido_nao_industria_sst").cast("int").alias("qt_trabalhador_atendido_nao_industria_sst"),
     sum("qt_trabalhador_atendido_nao_industria_promocao_saude").cast("int").alias("qt_trabalhador_atendido_nao_industria_promocao_saude"))

# COMMAND ----------

df_new_metrics_emp_ind__ers = df_new_metrics_emp_ind__ers.unpersist()
df_new_metrics_emp_ind__er_2 = df_new_metrics_emp_ind__er_2.unpersist()
df_new_metrics_trab__ers = df_new_metrics_trab__ers.unpersist()
df_new_metrics_trab__er_2 = df_new_metrics_trab__er_2.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Melt quantity columns

# COMMAND ----------

var_qt_columns = [column for column in df_new_metrics.columns if column.startswith('qt_')]

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

var_keep_columns = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_porte_empresa"]

# COMMAND ----------

df_new_metrics = melt(df_new_metrics, var_keep_columns, var_qt_columns, 'cd_metrica', 'vl_metrica')\
.coalesce(1)\
.cache()

df_new_metrics.count()

# COMMAND ----------

#Union of metrics alredy exists and new metrics
df = df_metrics_alredy_exists.union(df_new_metrics.select(df_metrics_alredy_exists.columns))\
.coalesce(1)\
.cache()

df.count()

# COMMAND ----------

df_metrics_alredy_exists = df_metrics_alredy_exists.unpersist()
df_new_metrics = df_new_metrics.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC Fill null values in value columns with 0

# COMMAND ----------

df_final = df.fillna(0, ["vl_metrica"])

# COMMAND ----------

# MAGIC %md
# MAGIC Change types

# COMMAND ----------

df_final = df_final.withColumn("vl_metrica", col("vl_metrica").cast("decimal(18,6)"))

# COMMAND ----------

# MAGIC %md
# MAGIC Add load timestamp

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df_final = tcf.add_control_fields(df_final, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write on ADLS

# COMMAND ----------

df_final.coalesce(1).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df = df.unpersist()

# COMMAND ----------

