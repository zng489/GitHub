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
# MAGIC Processo	trs_biz_gestao_financeira
# MAGIC Tabela/Arquivo Origem	/trs/evt/orcamento_nacional_realizado
# MAGIC Tabela/Arquivo Destino	/biz/orc/fta_gestao_financeira
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores financeiros básicos de receitas e despesas com educação SESI e SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atuaização	trunca as partições com dt_ultima_atualizacao_oltp >= #var_max_dt_ultima_atualizacao_oltp, dando a oportunidade de recuperar registros ignoraddos na carga anterior
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado
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
              "destination": "/orcamento/fta_gestao_financeira",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/trs_biz_fta_gestao_financeira"
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

var_user_parameters = {'closing': {'year': 2019, 'dt_closing': '2020-03-02', 'month': 12}}
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

# MAGIC %md
# MAGIC ### Looking documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* Obter os parâmtros informados pela UNIGEST para fechamento do Orçamento do mês: #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC Ler orcamento_nacional_realizado com os filtros recebidos como parâmetro para carga mensal na tabela fta_gestao_financeira, query abaixo: */ 
# MAGIC SELECT 
# MAGIC #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC #prm_mes_fechamento AS cd_mes_fechamento,
# MAGIC #prm_data_corte AS dt_fechamento, 
# MAGIC NVL(e.cd_entidade_regional, -98) AS cd_entidade_regional, /* Não informado */
# MAGIC cd_centro_responsabilidade,
# MAGIC 
# MAGIC -- Receita Total
# MAGIC SUM(CASE WHEN cd_conta_contabil like '4%'	    THEN vl_lancamento ELSE 0 END) AS vl_receita_total,
# MAGIC 
# MAGIC -- Receita Corrente
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41%'	    THEN vl_lancamento ELSE 0 END)     AS vl_receita_corrente,
# MAGIC 
# MAGIC -- Receita Servico e Convenio
# MAGIC SUM(CASE WHEN cd_conta_contabil like '410104%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_servico,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '410202%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_convenio,
# MAGIC 
# MAGIC -- Receita Contrib Compulsoria
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41010101001%' THEN vl_lancamento ELSE 0 END) AS vl_receita_contribuicao_direta,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41010101002%' THEN vl_lancamento ELSE 0 END) AS vl_receita_contribuicao_indireta,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41020101%' AND SUBSTRING(cd_entidade_regional_erp_oltp,2,1) = '2' THEN vl_lancamento ELSE 0 END) AS vl_receita_subvencao_ordinaria,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41020101%' AND SUBSTRING(cd_entidade_regional_erp_oltp,2,1) = '3' THEN vl_lancamento ELSE 0 END) AS vl_receita_auxilio_minimo,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41020102%' AND SUBSTRING(cd_entidade_regional_erp_oltp,2,1) = '2' THEN vl_lancamento ELSE 0 END) AS vl_receita_subvencao_especial,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41020102%' AND SUBSTRING(cd_entidade_regional_erp_oltp,2,1) = '3' THEN vl_lancamento ELSE 0 END) AS vl_receita_auxilio_especial,
# MAGIC 
# MAGIC -- Receita Financeira
# MAGIC SUM(CASE WHEN cd_conta_contabil like '410102%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_financeira,
# MAGIC 
# MAGIC -- Receita Apoio Financeiro (24-04-2020)
# MAGIC SUM(CASE WHEN cd_conta_contabil like '410203%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_apoio_financeiro,
# MAGIC 
# MAGIC -- Receita Outros (diferente de Servico e Convenio, Contrib Compulsoria e Auxílio,  Financeira e Apoio Financeiro), revisao em 24-04-2020
# MAGIC SUM
# MAGIC (
# MAGIC      CASE WHEN 
# MAGIC      cd_conta_contabil like '4%' AND
# MAGIC      NOT (cd_conta_contabil like '410104%'      OR 	--vl_receita_servico
# MAGIC           cd_conta_contabil like '410202%'      OR 	--vl_receita_convenio
# MAGIC 		  cd_conta_contabil like '41010101001%' OR 	--vl_receita_contribuicao_direta
# MAGIC           cd_conta_contabil like '41010101002%' OR 	--vl_receita_contribuicao_indireta
# MAGIC           cd_conta_contabil like '41020101%'    OR 	--vl_receita_auxilio_minimo e vl_receita_subvencao_ordinaria
# MAGIC           cd_conta_contabil like '41020102%'    OR 	--vl_receita_auxilio_especial e vl_receita_subvencao_especial
# MAGIC           cd_conta_contabil like '410102%'      OR  --vl_receita_financeira
# MAGIC 		  cd_conta_contabil like '410203%' 			--vl_receita_apoio_financeiro, incluída em 24-04-2020
# MAGIC      )
# MAGIC 	 THEN vl_lancamento ELSE 0 END
# MAGIC ) AS vl_receita_outros,
# MAGIC 
# MAGIC -- Receita Industrial 
# MAGIC SUM(CASE WHEN cd_conta_contabil like '410103%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_industrial,
# MAGIC 
# MAGIC -- Receita de subvencao e auxilio extraordinario (28/10/2020)
# MAGIC SUM(CASE WHEN cd_conta_contabil like '420201%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_subvencao_auxilio_extraordinario,
# MAGIC 
# MAGIC -- Receita Projeto Estratégico  (27-01-2021)
# MAGIC SUM(CASE WHEN cd_conta_contabil like '41020304%'	THEN vl_lancamento ELSE 0 END) AS vl_receita_projeto_estrategico,
# MAGIC 
# MAGIC --Despesas
# MAGIC SUM(CASE WHEN cd_conta_contabil like '3%'	    THEN vl_lancamento ELSE 0 END) AS vl_despesa_total,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '31%'	    THEN vl_lancamento ELSE 0 END) AS vl_despesa_corrente,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '32%'	    THEN vl_lancamento ELSE 0 END) AS vl_despesa_capital,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310101%'	THEN vl_lancamento ELSE 0 END) AS vl_despesa_pessoal,
# MAGIC 
# MAGIC -- Despesas incluídas em 24/04/2020
# MAGIC SUM(CASE WHEN cd_conta_contabil like '3%'      AND cd_centro_responsabilidade LIKE '3%'	    THEN vl_lancamento ELSE 0 END) AS vl_despesa_total_negocio,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310101%' AND cd_centro_responsabilidade LIKE '3%'	    THEN vl_lancamento ELSE 0 END) AS vl_despesa_pessoal_negocio,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310101%' AND cd_centro_responsabilidade NOT LIKE '3%'	THEN vl_lancamento ELSE 0 END) AS vl_despesa_pessoal_gestao,
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310102%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_ocupacao_utilidade
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310103%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_material
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310104%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_transporte_viagem
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310105%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_material_distribuicao_gratuita
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310106%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_servico_terceiro
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310107%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_arrendamento_mercantil
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310108%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_financeira
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310109%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_imposto_taxa_contrib
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310110%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_diversa
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310201%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_contrib_transf_reg
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310202%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_subvencao_aux_reg
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310203%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_convenio
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310204%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_apoio_financeiro
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310205%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_auxilio_a_terceiro
# MAGIC SUM(CASE WHEN cd_conta_contabil like '310206%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_contrib_associativa_filiacao
# MAGIC SUM(CASE WHEN cd_conta_contabil like '320101%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_investimento
# MAGIC SUM(CASE WHEN cd_conta_contabil like '320102%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_inversao_financeira
# MAGIC SUM(CASE WHEN cd_conta_contabil like '320201%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_subvencao_auxilio
# MAGIC SUM(CASE WHEN cd_conta_contabil like '320202%' THEN vl_lancamento ELSE 0 END) AS vl_despesa_amortizacao
# MAGIC -- FIM: Despesas incluídas em 24/04/2020
# MAGIC 
# MAGIC FROM orcamento_nacional_realizado o 
# MAGIC LEFT JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE YEAR(dt_lancamento) = #prm_ano_fechamento 
# MAGIC AND   MONTH(dt_lancamento) <= #prm_ano_fechamento 
# MAGIC AND   dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC 
# MAGIC GROUP BY NVL(e.cd_entidade_regional, -98), cd_centro_responsabilidade
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining both tables by cd_entidade_regional_erp_oltp

# COMMAND ----------

df_join = df_orcamento_nacional_realizado.join(df_entidade_regional, ["cd_entidade_regional_erp_oltp"], how='left')

# COMMAND ----------

del df_orcamento_nacional_realizado
del df_entidade_regional

# COMMAND ----------

df = df_join.filter(
  (year(col("dt_lancamento")) == var_parameters["prm_ano_fechamento"]) 
  & (month(col("dt_lancamento")) <= var_parameters["prm_mes_fechamento"]) 
  & (col("dt_ultima_atualizacao_oltp") <= var_parameters["prm_data_corte"])
). \
drop("dt_ultima_atualizacao_oltp", "dt_lancamento")

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
# MAGIC ### Creating revenues and expenses variables

# COMMAND ----------

var_new_columns_1 = [{"column_name": "vl_receita_total", "origin": 'cd_conta_contabil', "condition": '4%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_corrente", "origin": 'cd_conta_contabil', "condition": '41%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_servico", "origin": 'cd_conta_contabil', "condition": '410104%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_convenio", "origin": 'cd_conta_contabil', "condition": '410202%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_contribuicao_direta", "origin": 'cd_conta_contabil', "condition": '41010101001%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_contribuicao_indireta", "origin": 'cd_conta_contabil', "condition": '41010101002%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_financeira", "origin": 'cd_conta_contabil', "condition": '410102%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_apoio_financeiro", "origin": 'cd_conta_contabil', "condition": '410203%', "destination": "vl_lancamento", "otherwise": 0},     
              {"column_name": "vl_receita_industrial", "origin": 'cd_conta_contabil', "condition": '410103%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_receita_subvencao_auxilio_extraordinario", "origin": 'cd_conta_contabil', "condition": '420201%', "destination": "vl_lancamento", "otherwise": 0},     
              {"column_name": "vl_receita_projeto_estrategico", "origin": 'cd_conta_contabil', "condition": '41020304%', "destination": "vl_lancamento", "otherwise": 0},     
              {"column_name": "vl_despesa_total", "origin": 'cd_conta_contabil', "condition": '3%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_corrente", "origin": 'cd_conta_contabil', "condition": '31%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_capital", "origin": 'cd_conta_contabil', "condition": '32%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_pessoal", "origin": 'cd_conta_contabil', "condition": '310101%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_ocupacao_utilidade", "origin": 'cd_conta_contabil', "condition": '310102%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_material", "origin": 'cd_conta_contabil', "condition": '310103%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_transporte_viagem", "origin": 'cd_conta_contabil', "condition": '310104%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_material_distribuicao_gratuita", "origin": 'cd_conta_contabil', "condition": '310105%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_servico_terceiro", "origin": 'cd_conta_contabil', "condition": '310106%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_arrendamento_mercantil", "origin": 'cd_conta_contabil', "condition": '310107%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_financeira", "origin": 'cd_conta_contabil', "condition": '310108%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_imposto_taxa_contrib", "origin": 'cd_conta_contabil', "condition": '310109%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_diversa", "origin": 'cd_conta_contabil', "condition": '310110%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_arrecadacao_indireta", "origin": 'cd_conta_contabil', "condition": '31011001%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_contrib_transf_reg", "origin": 'cd_conta_contabil', "condition": '310201%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_subvencao_aux_reg", "origin": 'cd_conta_contabil', "condition": '310202%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_convenio", "origin": 'cd_conta_contabil', "condition": '310203%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_apoio_financeiro", "origin": 'cd_conta_contabil', "condition": '310204%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_auxilio_a_terceiro", "origin": 'cd_conta_contabil', "condition": '310205%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_contrib_associativa_filiacao", "origin": 'cd_conta_contabil', "condition": '310206%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_investimento", "origin": 'cd_conta_contabil', "condition": '320101%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_inversao_financeira", "origin": 'cd_conta_contabil', "condition": '320102%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_subvencao_auxilio", "origin": 'cd_conta_contabil', "condition": '320201%', "destination": "vl_lancamento", "otherwise": 0},
              {"column_name": "vl_despesa_amortizacao", "origin": 'cd_conta_contabil', "condition": '320202%', "destination": "vl_lancamento", "otherwise": 0}]

for item in var_new_columns_1:
  df= df.withColumn(item["column_name"], when(col(item["origin"]).like(item["condition"]), col(item["destination"])).otherwise(lit(item["otherwise"])))

# COMMAND ----------

var_new_columns_2 = [{"column_name": "vl_receita_subvencao_ordinaria", "origin_1": 'cd_conta_contabil', "condition_1": '41020101%', 
                      "origin_2": 'cd_entidade_regional_erp_oltp', "condition_2": '2', "destination": "vl_lancamento", "otherwise": 0},
                     {"column_name": "vl_receita_auxilio_minimo", "origin_1": 'cd_conta_contabil', "condition_1": '41020101%',
                      "origin_2": 'cd_entidade_regional_erp_oltp', "condition_2": '3', "destination": "vl_lancamento", "otherwise": 0},
                     {"column_name": "vl_receita_subvencao_especial", "origin_1": 'cd_conta_contabil', "condition_1": '41020102%', 
                      "origin_2": 'cd_entidade_regional_erp_oltp', "condition_2": '2', "destination": "vl_lancamento", "otherwise": 0},
                     {"column_name": "vl_receita_auxilio_especial", "origin_1": 'cd_conta_contabil', "condition_1": '41020102%',
                      "origin_2": 'cd_entidade_regional_erp_oltp', "condition_2": '3', "destination": "vl_lancamento", "otherwise": 0}]

for item in var_new_columns_2:
  df = df.withColumn(item["column_name"], when((col(item["origin_1"]).like(item["condition_1"])) & (substring(col(item["origin_2"]),2,1) == item["condition_2"]), col(item["destination"])).otherwise(lit(item["otherwise"])))

# COMMAND ----------

df = df.withColumn("vl_despesa_total_negocio", when((col("cd_conta_contabil").like("3%")) &\
                                                    (col("cd_centro_responsabilidade").like("3%")), 
                                                    col("vl_lancamento")).otherwise(lit(0)))\
.withColumn("vl_despesa_pessoal_negocio", when((col("cd_conta_contabil").like("310101%")) &\
                                               (col("cd_centro_responsabilidade").like("3%")), 
                                               col("vl_lancamento")).otherwise(lit(0)))\
.withColumn("vl_despesa_pessoal_gestao", when((col("cd_conta_contabil").like("310101%")) &\
                                               (~col("cd_centro_responsabilidade").like("3%")), 
                                               col("vl_lancamento")).otherwise(lit(0)))

# COMMAND ----------

var_conditions_vl_receita_outros = (col("cd_conta_contabil").like('4%') &~ (
                                  (col("cd_conta_contabil").like('410104%')) |\
                                  (col("cd_conta_contabil").like('410202%')) |\
                                  (col("cd_conta_contabil").like('41010101001%')) |\
                                  (col("cd_conta_contabil").like('41010101002%')) |\
                                  (col("cd_conta_contabil").like('41020101%')) |\
                                  (col("cd_conta_contabil").like('41020102%')) |\
                                  (col("cd_conta_contabil").like('410102%')) |\
                                  (col("cd_conta_contabil").like('410203%')) |\
                                  (col("cd_conta_contabil").like('410103%'))))

# COMMAND ----------

df = df.withColumn('vl_receita_outros', when(var_conditions_vl_receita_outros, col("vl_lancamento")).otherwise(lit(0)))\
.drop("cd_entidade_regional_erp_oltp", "cd_conta_contabil", "vl_lancamento")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggreganting values by cd_entidade_regional, cd_centro_responsabilidade

# COMMAND ----------

var_column_type_map = {"vl_receita_total" : "decimal(18,6)", 
                       "vl_receita_corrente" : "decimal(18,6)", 
                       "vl_receita_servico": "decimal(18,6)", 
                       "vl_receita_convenio": "decimal(18,6)",
                       "vl_receita_contribuicao_direta": "decimal(18,6)",
                       "vl_receita_contribuicao_indireta": "decimal(18,6)",
                       "vl_receita_subvencao_ordinaria": "decimal(18,6)",
                       "vl_receita_subvencao_especial": "decimal(18,6)", 
                       "vl_receita_auxilio_minimo": "decimal(18,6)", 
                       "vl_receita_auxilio_especial": "decimal(18,6)", 
                       "vl_receita_financeira": "decimal(18,6)",
                       "vl_receita_apoio_financeiro": "decimal(18,6)",
                       "vl_receita_outros": "decimal(18,6)",
                       "vl_receita_industrial" : "decimal(18,6)",                        
                       "vl_receita_subvencao_auxilio_extraordinario" : "decimal(18,6)",  
                       "vl_receita_projeto_estrategico": "decimal(18,6)",
                       "vl_despesa_total" : "decimal(18,6)", 
                       "vl_despesa_corrente": "decimal(18,6)", 
                       "vl_despesa_capital": "decimal(18,6)",                       
                       "vl_despesa_pessoal": "decimal(18,6)",
                       "vl_despesa_total_negocio": "decimal(18,6)",
                       "vl_despesa_pessoal_negocio": "decimal(18,6)",
                       "vl_despesa_pessoal_gestao": "decimal(18,6)",
                       "vl_despesa_ocupacao_utilidade": "decimal(18,6)",
                       "vl_despesa_material": "decimal(18,6)",
                       "vl_despesa_transporte_viagem": "decimal(18,6)",
                       "vl_despesa_material_distribuicao_gratuita": "decimal(18,6)",
                       "vl_despesa_servico_terceiro": "decimal(18,6)",
                       "vl_despesa_arrendamento_mercantil": "decimal(18,6)",
                       "vl_despesa_financeira": "decimal(18,6)",
                       "vl_despesa_imposto_taxa_contrib": "decimal(18,6)",
                       "vl_despesa_diversa": "decimal(18,6)",
                       "vl_despesa_arrecadacao_indireta": "decimal(18,6)",                       
                       "vl_despesa_contrib_transf_reg": "decimal(18,6)",
                       "vl_despesa_subvencao_aux_reg": "decimal(18,6)",
                       "vl_despesa_convenio": "decimal(18,6)",
                       "vl_despesa_apoio_financeiro": "decimal(18,6)",
                       "vl_despesa_auxilio_a_terceiro": "decimal(18,6)",
                       "vl_despesa_contrib_associativa_filiacao": "decimal(18,6)",
                       "vl_despesa_investimento": "decimal(18,6)",
                       "vl_despesa_inversao_financeira": "decimal(18,6)",
                       "vl_despesa_subvencao_auxilio": "decimal(18,6)",
                       "vl_despesa_amortizacao": "decimal(18,6)"}

for c in var_column_type_map:
  df = df.withColumn(c, df[c].cast(var_column_type_map[c]))

# COMMAND ----------

df = df.groupBy('cd_ano_fechamento', 'cd_mes_fechamento', 'dt_fechamento', 'cd_entidade_regional', 'cd_centro_responsabilidade')\
.agg(sum('vl_receita_total').alias('vl_receita_total'), \
     sum('vl_receita_corrente').alias('vl_receita_corrente'), \
     sum('vl_receita_servico').alias('vl_receita_servico'), \
     sum('vl_receita_convenio').alias('vl_receita_convenio'), \
     sum('vl_receita_contribuicao_direta').alias('vl_receita_contribuicao_direta'), \
     sum('vl_receita_contribuicao_indireta').alias('vl_receita_contribuicao_indireta'), \
     sum('vl_receita_subvencao_ordinaria').alias('vl_receita_subvencao_ordinaria'), \
     sum('vl_receita_auxilio_minimo').alias('vl_receita_auxilio_minimo'), \
     sum('vl_receita_subvencao_especial').alias('vl_receita_subvencao_especial'), \
     sum('vl_receita_auxilio_especial').alias('vl_receita_auxilio_especial'), \
     sum('vl_receita_financeira').alias('vl_receita_financeira'), \
     sum('vl_receita_apoio_financeiro').alias('vl_receita_apoio_financeiro'), \
     sum('vl_receita_outros').alias('vl_receita_outros'), \
     sum('vl_receita_industrial').alias('vl_receita_industrial'), \
     sum('vl_receita_subvencao_auxilio_extraordinario').alias('vl_receita_subvencao_auxilio_extraordinario'), \
     sum('vl_receita_projeto_estrategico').alias('vl_receita_projeto_estrategico'), \
     sum('vl_despesa_total').alias('vl_despesa_total'), \
     sum('vl_despesa_corrente').alias('vl_despesa_corrente'), \
     sum('vl_despesa_capital').alias('vl_despesa_capital'), \
     sum('vl_despesa_pessoal').alias('vl_despesa_pessoal'), \
     sum('vl_despesa_total_negocio').alias('vl_despesa_total_negocio'), \
     sum('vl_despesa_pessoal_negocio').alias('vl_despesa_pessoal_negocio'), \
     sum('vl_despesa_pessoal_gestao').alias('vl_despesa_pessoal_gestao'), \
     sum('vl_despesa_ocupacao_utilidade').alias('vl_despesa_ocupacao_utilidade'), \
     sum('vl_despesa_material').alias('vl_despesa_material'), \
     sum('vl_despesa_transporte_viagem').alias('vl_despesa_transporte_viagem'), \
     sum('vl_despesa_material_distribuicao_gratuita').alias('vl_despesa_material_distribuicao_gratuita'), \
     sum('vl_despesa_servico_terceiro').alias('vl_despesa_servico_terceiro'), \
     sum('vl_despesa_arrendamento_mercantil').alias('vl_despesa_arrendamento_mercantil'), \
     sum('vl_despesa_financeira').alias('vl_despesa_financeira'), \
     sum('vl_despesa_imposto_taxa_contrib').alias('vl_despesa_imposto_taxa_contrib'), \
     sum('vl_despesa_diversa').alias('vl_despesa_diversa'), \
     sum('vl_despesa_arrecadacao_indireta').alias('vl_despesa_arrecadacao_indireta'), \
     sum('vl_despesa_contrib_transf_reg').alias('vl_despesa_contrib_transf_reg'), \
     sum('vl_despesa_subvencao_aux_reg').alias('vl_despesa_subvencao_aux_reg'), \
     sum('vl_despesa_convenio').alias('vl_despesa_convenio'), \
     sum('vl_despesa_apoio_financeiro').alias('vl_despesa_apoio_financeiro'), \
     sum('vl_despesa_auxilio_a_terceiro').alias('vl_despesa_auxilio_a_terceiro'), \
     sum('vl_despesa_contrib_associativa_filiacao').alias('vl_despesa_contrib_associativa_filiacao'), \
     sum('vl_despesa_investimento').alias('vl_despesa_investimento'), \
     sum('vl_despesa_inversao_financeira').alias('vl_despesa_inversao_financeira'), \
     sum('vl_despesa_subvencao_auxilio').alias('vl_despesa_subvencao_auxilio'), \
     sum('vl_despesa_amortizacao').alias('vl_despesa_amortizacao'), \
    )

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
                       "vl_receita_total" : "decimal(18,6)", 
                       "vl_receita_corrente" : "decimal(18,6)", 
                       "vl_receita_servico": "decimal(18,6)", 
                       "vl_receita_convenio": "decimal(18,6)",
                       "vl_receita_contribuicao_direta": "decimal(18,6)",
                       "vl_receita_contribuicao_indireta": "decimal(18,6)",
                       "vl_receita_subvencao_ordinaria": "decimal(18,6)",
                       "vl_receita_subvencao_especial": "decimal(18,6)", 
                       "vl_receita_auxilio_minimo": "decimal(18,6)", 
                       "vl_receita_auxilio_especial": "decimal(18,6)", 
                       "vl_receita_financeira": "decimal(18,6)",
                       "vl_receita_apoio_financeiro": "decimal(18,6)",
                       "vl_receita_outros": "decimal(18,6)",
                       "vl_receita_industrial" : "decimal(18,6)", 
                       "vl_receita_subvencao_auxilio_extraordinario" : "decimal(18,6)", 
                       "vl_receita_projeto_estrategico": "decimal(18,6)",
                       "vl_despesa_total" : "decimal(18,6)", 
                       "vl_despesa_corrente": "decimal(18,6)", 
                       "vl_despesa_capital": "decimal(18,6)", 
                       "vl_despesa_pessoal": "decimal(18,6)",
                       "vl_despesa_total_negocio": "decimal(18,6)",
                       "vl_despesa_pessoal_negocio": "decimal(18,6)",
                       "vl_despesa_pessoal_gestao": "decimal(18,6)",
                       "vl_despesa_ocupacao_utilidade": "decimal(18,6)",
                       "vl_despesa_material": "decimal(18,6)",
                       "vl_despesa_transporte_viagem": "decimal(18,6)",
                       "vl_despesa_material_distribuicao_gratuita": "decimal(18,6)",
                       "vl_despesa_servico_terceiro": "decimal(18,6)",
                       "vl_despesa_arrendamento_mercantil": "decimal(18,6)",
                       "vl_despesa_financeira": "decimal(18,6)",
                       "vl_despesa_imposto_taxa_contrib": "decimal(18,6)",
                       "vl_despesa_diversa": "decimal(18,6)",
                       "vl_despesa_arrecadacao_indireta": "decimal(18,6)",
                       "vl_despesa_contrib_transf_reg": "decimal(18,6)",
                       "vl_despesa_subvencao_aux_reg": "decimal(18,6)",
                       "vl_despesa_convenio": "decimal(18,6)",
                       "vl_despesa_apoio_financeiro": "decimal(18,6)",
                       "vl_despesa_auxilio_a_terceiro": "decimal(18,6)",
                       "vl_despesa_contrib_associativa_filiacao": "decimal(18,6)",
                       "vl_despesa_investimento": "decimal(18,6)",
                       "vl_despesa_inversao_financeira": "decimal(18,6)",
                       "vl_despesa_subvencao_auxilio": "decimal(18,6)",
                       "vl_despesa_amortizacao": "decimal(18,6)"}

for c in var_column_type_map:
  df = df.withColumn(c, df[c].cast(var_column_type_map[c]))

# COMMAND ----------

#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding columns for partition and current timestamp 

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

