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
# MAGIC Processo	biz_biz_fta_gestao_financeira_tecnologia_inovacao_kpi_pivot
# MAGIC Tabela/Arquivo Origem	"/biz/orcamento/fta_receita_servico_convenio_rateada_negocio
# MAGIC /biz/orcamento/fta_gestao_financeira_tecnologia_inovacao"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_tecnologia_inovacao_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas de Negócios rateadas de STI do SENAI modelados para atender o consumo via Tableau
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à cd_ano_fechamento e as subpartições de cd_mes_fechamento
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs orcamento_nacional_realizado, que ocorre às 20:00, e da biz fta_gestao_financeira que acontece na sequência
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
                          "/orcamento/fta_gestao_financeira_tecnologia_inovacao",
                          "/orcamento/fta_despesa_rateada_negocio",
                          "/corporativo/dim_hierarquia_entidade_regional"],
              "destination": "/orcamento/fta_gestao_financeira_tecnologia_inovacao_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_tecnologia_inovacao_kpi_pivot"
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

src_fta_fin, src_fta_sti, src_fta_des, src_dim_reg = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"],t)  for t in var_tables["origins"]]
print(src_fta_fin, src_fta_sti, src_fta_des, src_dim_reg)

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
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_desptot_direta_sti' AS cd_metrica,
# MAGIC SUM(vl_desptot_sti_rateada + vl_desptot_etd_gestao_outros_sti_rateada + vl_desptot_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento 
# MAGIC AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional, 
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC --- NOVO INICIO --- 16/12
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_desptot_sti' AS cd_metrica,
# MAGIC SUM(vl_desptot_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC --- NOVO FIM --- 16/12
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_etd_gestao_outros_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_etd_gestao_outros_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_desptot_etd_gestao_outros_sti' AS cd_metrica,
# MAGIC SUM(vl_desptot_etd_gestao_outros_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_suporte_negocio_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_desptot_suporte_negocio_sti' AS cd_metrica,
# MAGIC SUM(vl_desptot_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND o.cd_centro_responsabilidade like '302%'
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_sti' as cd_metrica,   --- alterado 16/12/2020
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC --WHERE cd_centro_responsabilidade LIKE '302%'    ---- alterado 16/12/2020
# MAGIC WHERE (cd_centro_responsabilidade LIKE '3020101%'   
# MAGIC OR cd_centro_responsabilidade IN ('302010202', '302010204')
# MAGIC OR cd_centro_responsabilidade LIKE '3020105%'   
# MAGIC OR cd_centro_responsabilidade LIKE '30202%')
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_consultoria_tecnologia' as cd_metrica, 
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade in  ('302010202', '302010204')
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_metrologia' as cd_metrica, 
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021 
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade LIKE '3020105%'
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_tecnico_especializado' as cd_metrica, 
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade LIKE '3020101%'
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_solucao_inovacao' as cd_metrica, 
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021 
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade LIKE '30202%'
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_receita_servico_convenio_proj_outros' as cd_metrica, 
# MAGIC SUM(vl_receita_servico + vl_receita_convenio + vl_receita_projeto_estrategico) as vl_metrica  ---- alterado 27/01/2021
# MAGIC FROM fta_gestao_financeira o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC WHERE cd_centro_responsabilidade LIKE '302%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3020101%'
# MAGIC AND cd_centro_responsabilidade NOT IN  ('302010202', '302010204')
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3020105%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '30202%'
# MAGIC AND cd_entidade_nacional = 3
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_consultoria_tecnologia' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND cd_centro_responsabilidade  in ( '302010202', '302010204' )
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_metrologia' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND cd_centro_responsabilidade  LIKE ( '3020105%' )
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_tecnico_especializado' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND cd_centro_responsabilidade  LIKE ( '3020101%' )
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_solucao_inovacao' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND cd_centro_responsabilidade  LIKE ( '30202%' )
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_outros' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND cd_centro_responsabilidade  LIKE ( '302%' )
# MAGIC AND cd_centro_responsabilidade NOT LIKE ('3020101%')
# MAGIC AND cd_centro_responsabilidade NOT IN ('302010202', '302010204')
# MAGIC AND cd_centro_responsabilidade NOT LIKE ('3020105%')
# MAGIC AND cd_centro_responsabilidade NOT LIKE ('30202%')
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND (cd_centro_responsabilidade LIKE ('3020101%')
# MAGIC OR cd_centro_responsabilidade IN ('302010202', '302010204')
# MAGIC OR cd_centro_responsabilidade LIKE ('3020105%')
# MAGIC OR cd_centro_responsabilidade LIKE ('30202%'))
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC -- Incluído em 27/01/2021
# MAGIC UNION ALL
# MAGIC 
# MAGIC (select
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC 'vl_despcor_direta_proj_sti' AS cd_metrica,
# MAGIC SUM(vl_despcor_sti_rateada + vl_despcor_etd_gestao_outros_sti_rateada + vl_despcor_suporte_negocio_sti_rateada
# MAGIC vl_despcor_indireta_gestao_sti_rateada + vl_despcor_indireta_desenv_institucional_sti_rateada + vl_despcor_indireta_apoio_sti_ratead) AS  vl_metrica
# MAGIC FROM fta_despesa_rateada_sti o
# MAGIC WHERE cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento 
# MAGIC AND (cd_centro_responsabilidade LIKE ('3020101%')
# MAGIC OR cd_centro_responsabilidade IN ('302010202', '302010204')
# MAGIC OR cd_centro_responsabilidade LIKE ('3020105%')
# MAGIC OR cd_centro_responsabilidade LIKE ('30202%'))
# MAGIC GROUP BY 
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ORDER BY sg_entidade_regional, cd_centro_responsabilidade)
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira_tecnologia_inovacao

# COMMAND ----------

df_fta_sti = spark.read.parquet(src_fta_sti)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", 
        "vl_despcor_sti_rateada", "vl_despcor_etd_gestao_outros_sti_rateada", "vl_despcor_suporte_negocio_sti_rateada",
        "vl_despcor_indireta_gestao_sti_rateada", "vl_despcor_indireta_desenv_institucional_sti_rateada", "vl_despcor_indireta_apoio_sti_rateada",
        "vl_desptot_sti_rateada", "vl_desptot_etd_gestao_outros_sti_rateada", "vl_desptot_suporte_negocio_sti_rateada",
        "vl_desptot_indireta_gestao_sti_rateada", "vl_desptot_indireta_desenv_institucional_sti_rateada", "vl_desptot_indireta_apoio_sti_rateada",)\
.filter(((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) & (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"])))\
.withColumn("vl_despcor_direta_sti",
            (f.col("vl_despcor_sti_rateada") + 
             f.col("vl_despcor_etd_gestao_outros_sti_rateada") + 
             f.col("vl_despcor_suporte_negocio_sti_rateada")))\
.withColumn("vl_desptot_direta_sti",
            (f.col("vl_desptot_sti_rateada") + 
             f.col("vl_desptot_etd_gestao_outros_sti_rateada") + 
             f.col("vl_desptot_suporte_negocio_sti_rateada")))\
.withColumn("vl_despcor_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_despcor_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_desptot_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_desptot_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_etd_gestao_outros_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_despcor_etd_gestao_outros_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_desptot_etd_gestao_outros_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_desptot_etd_gestao_outros_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_suporte_negocio_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_despcor_suporte_negocio_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_desptot_suporte_negocio_sti",
            f.when(f.col("cd_centro_responsabilidade").startswith("302"), f.col("vl_desptot_suporte_negocio_sti_rateada"))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_sti",
            f.when((f.col("cd_centro_responsabilidade").startswith("3020101")) |\
                   (f.col("cd_centro_responsabilidade").isin("302010202", "302010204")) |\
                   (f.col("cd_centro_responsabilidade").startswith("3020105")) |\
                   (f.col("cd_centro_responsabilidade").startswith("30202")),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_consultoria_tecnologia",
            f.when(f.col("cd_centro_responsabilidade").isin("302010202", "302010204"),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_metrologia",
            f.when(f.col("cd_centro_responsabilidade").startswith("3020105"),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_tecnico_especializado",
            f.when(f.col("cd_centro_responsabilidade").startswith("3020101"),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_solucao_inovacao",
            f.when(f.col("cd_centro_responsabilidade").startswith("30202"),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_direta_proj_outros",
            f.when((f.col("cd_centro_responsabilidade").startswith("302")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("3020101")) &\
                   ~(f.col("cd_centro_responsabilidade").isin("302010202", "302010204")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("3020105")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("30202")),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_despcor_proj_sti_total",
            f.when((f.col("cd_centro_responsabilidade").startswith("3020101")) |\
                   (f.col("cd_centro_responsabilidade").isin("302010202", "302010204")) |\
                   (f.col("cd_centro_responsabilidade").startswith("3020105")) |\
                   (f.col("cd_centro_responsabilidade").startswith("30202")),
                   (f.col("vl_despcor_sti_rateada") + f.col("vl_despcor_etd_gestao_outros_sti_rateada") + f.col("vl_despcor_suporte_negocio_sti_rateada") +
                    f.col("vl_despcor_indireta_gestao_sti_rateada") + f.col("vl_despcor_indireta_desenv_institucional_sti_rateada") + f.col("vl_despcor_indireta_apoio_sti_rateada")))\
            .otherwise(f.lit(0)))\
.groupBy("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("vl_despcor_direta_sti").cast("decimal(18,6)").alias("vl_despcor_direta_sti"),
     f.sum("vl_desptot_direta_sti").cast("decimal(18,6)").alias("vl_desptot_direta_sti"),
     f.sum("vl_despcor_sti").cast("decimal(18,6)").alias("vl_despcor_sti"),
     f.sum("vl_desptot_sti").cast("decimal(18,6)").alias("vl_desptot_sti"),
     f.sum("vl_despcor_etd_gestao_outros_sti").cast("decimal(18,6)").alias("vl_despcor_etd_gestao_outros_sti"),
     f.sum("vl_desptot_etd_gestao_outros_sti").cast("decimal(18,6)").alias("vl_desptot_etd_gestao_outros_sti"),
     f.sum("vl_despcor_suporte_negocio_sti").cast("decimal(18,6)").alias("vl_despcor_suporte_negocio_sti"),
     f.sum("vl_desptot_suporte_negocio_sti").cast("decimal(18,6)").alias("vl_desptot_suporte_negocio_sti"),
     f.sum("vl_despcor_direta_proj_sti").cast("decimal(18,6)").alias("vl_despcor_direta_proj_sti"),
     f.sum("vl_despcor_direta_proj_consultoria_tecnologia").cast("decimal(18,6)").alias("vl_despcor_direta_proj_consultoria_tecnologia"),
     f.sum("vl_despcor_direta_proj_metrologia").cast("decimal(18,6)").alias("vl_despcor_direta_proj_metrologia"),
     f.sum("vl_despcor_direta_proj_tecnico_especializado").cast("decimal(18,6)").alias("vl_despcor_direta_proj_tecnico_especializado"),
     f.sum("vl_despcor_direta_proj_solucao_inovacao").cast("decimal(18,6)").alias("vl_despcor_direta_proj_solucao_inovacao"),
     f.sum("vl_despcor_direta_proj_outros").cast("decimal(18,6)").alias("vl_despcor_direta_proj_outros"),
     f.sum("vl_despcor_proj_sti_total").cast("decimal(18,6)").alias("vl_despcor_proj_sti_total"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_entidade_regional (aux)

# COMMAND ----------

df_dim_reg = spark.read.parquet(src_dim_reg)\
.select("cd_entidade_regional", "cd_entidade_nacional")\
.filter(f.col("cd_entidade_nacional") == 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_despesa_rateada_negocio

# COMMAND ----------

df_fta_des = spark.read.parquet(src_fta_des)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", 
        "vl_despesa_corrente", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_suporte_negocio_rateada", 
        "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada",
        "vl_despesa_total", "vl_desptot_etd_gestao_outros_rateada", "vl_desptot_suporte_negocio_rateada", 
        "vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_apoio_rateada")\
.filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]) &\
        (f.col("cd_centro_responsabilidade").startswith("302")))\
.join(df_dim_reg, ["cd_entidade_regional"], "inner")\
.withColumn("vl_despcor_sti_total", 
            (f.col("vl_despesa_corrente") + 
             f.col("vl_despcor_etd_gestao_outros_rateada") +              
             f.col("vl_despcor_suporte_negocio_rateada") +
             f.col("vl_despcor_indireta_gestao_rateada") +
             f.col("vl_despcor_indireta_desenv_institucional_rateada") +
             f.col("vl_despcor_indireta_apoio_rateada")))\
.withColumn("vl_desptot_sti_total", 
            (f.col("vl_despesa_total") + 
             f.col("vl_desptot_etd_gestao_outros_rateada") +              
             f.col("vl_desptot_suporte_negocio_rateada") +
             f.col("vl_desptot_indireta_gestao_rateada") +
             f.col("vl_desptot_indireta_desenv_institucional_rateada") +
             f.col("vl_desptot_indireta_apoio_rateada")))\
.drop("vl_despesa_corrente",      
      "vl_despcor_etd_gestao_outros_rateada",
      "vl_despcor_suporte_negocio_rateada",
      "vl_despcor_indireta_gestao_rateada",
      "vl_despcor_indireta_desenv_institucional_rateada",
      "vl_despcor_indireta_apoio_rateada",      
      "vl_despesa_total",      
      "vl_desptot_etd_gestao_outros_rateada",
      "vl_desptot_suporte_negocio_rateada",
      "vl_desptot_indireta_gestao_rateada",
      "vl_desptot_indireta_desenv_institucional_rateada",
      "vl_desptot_indireta_apoio_rateada",      
      "cd_entidade_nacional")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fta_gestao_financeira

# COMMAND ----------

df_fta_fin = spark.read.parquet(src_fta_fin)\
.select("cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_servico", "vl_receita_convenio", "vl_receita_projeto_estrategico")\
.filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
        (f.col("cd_mes_fechamento") == var_parameters["prm_mes_fechamento"]))\
.join(df_dim_reg, ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_servico_convenio_proj_sti",
            f.when((f.col("cd_centro_responsabilidade").startswith("3020101")) |\
                   (f.col("cd_centro_responsabilidade").isin("302010202", "302010204")) |\
                   (f.col("cd_centro_responsabilidade").startswith("3020105")) |\
                   (f.col("cd_centro_responsabilidade").startswith("30202")),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_receita_servico_convenio_proj_consultoria_tecnologia",
            f.when(f.col("cd_centro_responsabilidade").isin("302010202", "302010204"),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_receita_servico_convenio_proj_metrologia",
            f.when(f.col("cd_centro_responsabilidade").startswith("3020105"),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_receita_servico_convenio_proj_tecnico_especializado",
            f.when(f.col("cd_centro_responsabilidade").startswith("3020101"),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_receita_servico_convenio_proj_solucao_inovacao",
            f.when(f.col("cd_centro_responsabilidade").startswith("30202"),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.withColumn("vl_receita_servico_convenio_proj_outros",
            f.when((f.col("cd_centro_responsabilidade").startswith("302")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("3020101")) &\
                   ~(f.col("cd_centro_responsabilidade").isin("302010202", "302010204")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("3020105")) &\
                   ~(f.col("cd_centro_responsabilidade").startswith("30202")),
                   (f.col("vl_receita_servico") + f.col("vl_receita_convenio") + f.col("vl_receita_projeto_estrategico")))\
            .otherwise(f.lit(0)))\
.drop("vl_receita_servico", "vl_receita_convenio", "vl_receita_projeto_estrategico")

# COMMAND ----------

if (df_fta_sti.count() + df_fta_des.count() + df_fta_fin.count())==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

var_vl_columns_sti = [column for column in df_fta_sti.columns if column.startswith('vl_')]
var_vl_columns_des = [column for column in df_fta_des.columns if column.startswith('vl_')]
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

df_fta_sti = melt(df_fta_sti, var_keep_columns, var_vl_columns_sti, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df_fta_des = melt(df_fta_des, var_keep_columns, var_vl_columns_des, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df_fta_fin = melt(df_fta_fin, var_keep_columns, var_vl_columns_fin, 'cd_metrica', 'vl_metrica')

# COMMAND ----------

df = df_fta_sti\
.union(df_fta_des.select(df_fta_sti.columns))\
.union(df_fta_fin.select(df_fta_sti.columns))

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

