# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	biz_biz_fta_gestao_financeira_tecnologia_inovacao
# MAGIC Tabela/Arquivo Origem	/biz/orcamento/fta_despesa_rateada_negocio
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_tecnologia_inovacao
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas do STI-SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC </pre>

# COMMAND ----------

import json
import re
from datetime import datetime, date
from trs_control_field import trs_control_field as tcf
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
import pyspark.sql.functions as f

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------


#MOCK FOR USING THIS IN DEV
#Even though we're reading data from prod, sink must point to dev when in development

"""
var_tables = {"origins":["/orcamento/fta_despesa_rateada_negocio", "/corporativo/dim_hierarquia_entidade_regional"],
  "destination":"/orcamento/fta_gestao_financeira_tecnologia_inovacao", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_tecnologia_inovacao"}
  }

var_dls =  {"folders":
  {"landing":"/lnd", 
  "error":"/err", 
  "staging":"/stg", 
  "log":"/log", 
  "raw":"/raw", 
  "trusted": "/trs", 
  "business": "/biz"}
  }

var_adf =  {"adf_factory_name":"cnibigdatafactory",
"adf_pipeline_name":"trs_biz_fta_receita_servico_convenio_rateada_negocio",
"adf_pipeline_run_id":"development",
"adf_trigger_id":"development",
"adf_trigger_name":"marcela",
"adf_trigger_time":"2020-09-15T13:00:00.8532201Z",
"adf_trigger_type":"Manual"
}

var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}
"""

# COMMAND ----------

"""
USE THIS FOR DEV PURPOSES
var_sink = "{}{}{}".format(var_adls_uri, "/tmp/dev" + var_dls["folders"]["business"], var_tables["destination"]) 
"""

var_src_drn, var_src_reg =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], t) for t in var_tables["origins"]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
var_src_drn, var_src_reg, var_sink

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
  
var_parameters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 01
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP01: Obtem despesas de STI SENAI que serão rateadas por centro de responsabilidade  ---- Despesas do negócio: Diretas e suporte ao negócio
# MAGIC -- Despesa Indiretas: Gestão, Desenvolvimento Institucional e Apoio 
# MAGIC --========================================================================================================================================================
# MAGIC ---- Obtém todos os totais por entidade regional e centro de responsabilidade.
# MAGIC drop table rateio_step01_despesa_cr_sti;
# MAGIC create table rateio_step01_despesa_cr_sti
# MAGIC (
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC   sum(vl_despesa_corrente)                  AS vl_despesa_corrente
# MAGIC , sum(vl_despcor_suporte_negocio_rateada)   AS vl_despcor_suporte_negocio -- vl_despesa_corrente_suporte_negocio
# MAGIC , sum(vl_despcor_etd_gestao_outros_rateada) AS vl_despcor_etd_gestao_outros -- vl_despesa_corrente_etd_gestao_outros
# MAGIC , sum(vl_despcor_indireta_gestao_rateada)   AS vl_despcor_indireta_gestao -- vl_despesa_corrente_indireta_gestao
# MAGIC , sum(vl_despcor_indireta_desenv_institucional_rateada) AS vl_despcor_indireta_desenv_institucional -- vl_despesa_corrente_indireta_desenv_institucional
# MAGIC , sum(vl_despcor_indireta_apoio_rateada)    AS vl_despcor_indireta_apoio -- vl_despesa_corrente_indireta_apoio
# MAGIC , sum(vl_despesa_total)                     AS vl_despesa_total
# MAGIC , sum(vl_desptot_suporte_negocio_rateada)   AS vl_desptot_suporte_negocio -- vl_despesa_total_suporte_negocio
# MAGIC , sum(vl_desptot_etd_gestao_outros_rateada) AS vl_desptot_etd_gestao_outros -- vl_despesa_total_etd_gestao_outros
# MAGIC , sum(vl_desptot_indireta_gestao_rateada)   AS vl_desptot_indireta_gestao -- vl_despesa_total_indireta_gestao
# MAGIC , sum(vl_desptot_indireta_desenv_institucional_rateada) AS vl_desptot_indireta_desenv_institucional -- vl_despesa_total_indireta_desenv_institucional
# MAGIC , sum(vl_desptot_indireta_apoio_rateada)    AS vl_desptot_indireta_apoio -- vl_despesa_total_indireta_apoio
# MAGIC FROM fta_despesa_rateada_negocio o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e 
# MAGIC ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC 
# MAGIC WHERE e.cd_entidade_nacional = 3 -- SENAI
# MAGIC AND  (cd_centro_responsabilidade like '302%') 
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC cd_centro_responsabilidade
# MAGIC );
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
I'll try to keep the same notation as documentation when possible
It is really small: < 200 records. Keep in 1 partition
"""
var_o_columns = ["cd_centro_responsabilidade", "vl_despesa_corrente", "vl_despcor_suporte_negocio_rateada", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada", "vl_despesa_total", "vl_desptot_suporte_negocio_rateada", "vl_desptot_etd_gestao_outros_rateada", "vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_apoio_rateada", "cd_entidade_regional", "cd_ano_fechamento", "cd_mes_fechamento"]

o = spark.read.parquet(var_src_drn)\
.select(*var_o_columns)\
.fillna(0.0, subset=["vl_despesa_corrente",
                     "vl_despcor_suporte_negocio_rateada",
                     "vl_despcor_etd_gestao_outros_rateada",
                     "vl_despcor_indireta_gestao_rateada",
                     "vl_despcor_indireta_desenv_institucional_rateada",
                     "vl_despcor_indireta_apoio_rateada",
                     "vl_despesa_total",
                     "vl_desptot_suporte_negocio_rateada",
                     "vl_desptot_etd_gestao_outros_rateada",
                     "vl_desptot_indireta_gestao_rateada",
                     "vl_desptot_indireta_desenv_institucional_rateada",
                     "vl_desptot_indireta_apoio_rateada"])\
.filter((f.col("cd_centro_responsabilidade").startswith("302")) &\
        (f.col("cd_ano_fechamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.col("cd_mes_fechamento") == f.lit(var_parameters["prm_mes_fechamento"]).cast("int")))\
.drop(*["cd_ano_fechamento", "cd_mes_fechamento"])\
.coalesce(1)

# COMMAND ----------

"""
# e
It is really small: < 100 records. Keep in 1 partition
"""

var_e_columns = ["cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional"]

e = spark.read.parquet(var_src_reg)\
.select(*var_e_columns)\
.filter(f.col("cd_entidade_nacional") == 3)\
.coalesce(1)

# COMMAND ----------

"""
I'm testing for the first load and the object is really small:
  ~170 records, 23KB in mem cached. Can be kept in 1 partition
"""
rateio_step01_despesa_cr_sti = o.join(e, on=["cd_entidade_regional"], how="inner")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(
  f.sum(f.col("vl_despesa_corrente")).alias("vl_despesa_corrente"),
  f.sum(f.col("vl_despcor_suporte_negocio_rateada")).alias("vl_despcor_suporte_negocio"),
  f.sum(f.col("vl_despcor_etd_gestao_outros_rateada")).alias("vl_despcor_etd_gestao_outros"),
  f.sum(f.col("vl_despcor_indireta_gestao_rateada")).alias("vl_despcor_indireta_gestao"),
  f.sum(f.col("vl_despcor_indireta_desenv_institucional_rateada")).alias("vl_despcor_indireta_desenv_institucional"),
  f.sum(f.col("vl_despcor_indireta_apoio_rateada")).alias("vl_despcor_indireta_apoio"),
  f.sum(f.col("vl_despesa_total")).alias("vl_despesa_total"),
  f.sum(f.col("vl_desptot_suporte_negocio_rateada")).alias("vl_desptot_suporte_negocio"),
  f.sum(f.col("vl_desptot_etd_gestao_outros_rateada")).alias("vl_desptot_etd_gestao_outros"),
  f.sum(f.col("vl_desptot_indireta_gestao_rateada")).alias("vl_desptot_indireta_gestao"),
  f.sum(f.col("vl_desptot_indireta_desenv_institucional_rateada")).alias("vl_desptot_indireta_desenv_institucional"),
  f.sum(f.col("vl_desptot_indireta_apoio_rateada")).alias("vl_desptot_indireta_apoio")
)\
.fillna(0.0, subset=["vl_despesa_corrente",
                     "vl_despcor_suporte_negocio",
                     "vl_despcor_etd_gestao_outros",
                     "vl_despcor_indireta_gestao",
                     "vl_despcor_indireta_desenv_institucional",
                     "vl_despcor_indireta_apoio",
                     "vl_despesa_total",
                     "vl_desptot_suporte_negocio",
                     "vl_desptot_etd_gestao_outros",
                     "vl_desptot_indireta_gestao",
                     "vl_desptot_indireta_desenv_institucional",
                     "vl_desptot_indireta_apoio"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step01_despesa_cr_sti.count()

# COMMAND ----------

#assert  rateio_step01_despesa_cr_sti.count() == spark.sql("select count(*) from rateio_step01_despesa_cr_sti").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 02
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP02: Obtem despesas totais de custo e gastos  de STI SENAI que serão rateadas por  ---- Despesas do negócio: Diretas e suporte ao negócio
# MAGIC -- Despesa Indiretas: Gestão, Desenvolvimento Institucional e Apoio 
# MAGIC --========================================================================================================================================================
# MAGIC DROP TABLE rateio_step02_despesa_sti;
# MAGIC CREATE TABLE rateio_step02_despesa_sti
# MAGIC (
# MAGIC SELECT 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC   sum(vl_despesa_corrente)          AS vl_despesa_corrente
# MAGIC , sum(vl_despcor_etd_gestao_outros) AS vl_despcor_etd_gestao_outros
# MAGIC , sum(vl_despcor_suporte_negocio)   AS vl_despcor_suporte_negocio
# MAGIC , sum(vl_despcor_indireta_gestao)   AS vl_despcor_indireta_gestao
# MAGIC , sum(vl_despcor_indireta_desenv_institucional) AS vl_despcor_indireta_desenv_institucional
# MAGIC , sum(vl_despcor_indireta_apoio)    AS vl_despcor_indireta_apoio
# MAGIC , sum(vl_despesa_total)             AS vl_despesa_total
# MAGIC , sum(vl_desptot_etd_gestao_outros) AS vl_desptot_etd_gestao_outros
# MAGIC , sum(vl_desptot_suporte_negocio)   AS vl_desptot_suporte_negocio
# MAGIC , sum(vl_desptot_indireta_gestao)   AS vl_desptot_indireta_gestao
# MAGIC , sum(vl_desptot_indireta_desenv_institucional) AS vl_desptot_indireta_desenv_institucional
# MAGIC , sum(vl_desptot_indireta_apoio)    AS vl_desptot_indireta_apoio
# MAGIC FROM rateio_step01_despesa_cr_sti
# MAGIC GROUP BY 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional);
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has 28 records. Keep in 1 partition.
"""

rateio_step02_despesa_sti = rateio_step01_despesa_cr_sti\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(
  f.sum(f.col("vl_despesa_corrente")).alias("vl_despesa_corrente_step02"),
  f.sum(f.col("vl_despcor_suporte_negocio")).alias("vl_despcor_suporte_negocio_step02"),
  f.sum(f.col("vl_despcor_etd_gestao_outros")).alias("vl_despcor_etd_gestao_outros_step02"),
  f.sum(f.col("vl_despcor_indireta_gestao")).alias("vl_despcor_indireta_gestao_step02"),
  f.sum(f.col("vl_despcor_indireta_desenv_institucional")).alias("vl_despcor_indireta_desenv_institucional_step02"),
  f.sum(f.col("vl_despcor_indireta_apoio")).alias("vl_despcor_indireta_apoio_step02"),
  f.sum(f.col("vl_despesa_total")).alias("vl_despesa_total_step02"),
  f.sum(f.col("vl_desptot_suporte_negocio")).alias("vl_desptot_suporte_negocio_step02"),
  f.sum(f.col("vl_desptot_etd_gestao_outros")).alias("vl_desptot_etd_gestao_outros_step02"),
  f.sum(f.col("vl_desptot_indireta_gestao")).alias("vl_desptot_indireta_gestao_step02"),
  f.sum(f.col("vl_desptot_indireta_desenv_institucional")).alias("vl_desptot_indireta_desenv_institucional_step02"),
  f.sum(f.col("vl_desptot_indireta_apoio")).alias("vl_desptot_indireta_apoio_step02")
)\
.fillna(0.0, subset=["vl_despesa_corrente_step02",
                     "vl_despcor_suporte_negocio_step02",
                     "vl_despcor_etd_gestao_outros_step02",
                     "vl_despcor_indireta_gestao_step02",
                     "vl_despcor_indireta_desenv_institucional_step02",
                     "vl_despcor_indireta_apoio_step02",
                     "vl_despesa_total_step02",
                     "vl_desptot_suporte_negocio_step02",
                     "vl_desptot_etd_gestao_outros_step02",
                     "vl_desptot_indireta_gestao_step02",
                     "vl_desptot_indireta_desenv_institucional_step02",
                     "vl_desptot_indireta_apoio_step02"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_step02_despesa_sti.count() == spark.sql("select count(*) from rateio_step02_despesa_sti").collect()[0][0]

# COMMAND ----------

"""
Validation: ok

cd_entidade_nacional	                    3
sg_entidade_regional	                    SENAI-SP
cd_entidade_regional	                    1117285
vl_despesa_corrente	                        18704888.86
vl_despcor_etd_gestao_outros   	            4330469.29
vl_despcor_suporte_negocio	                7756570.7132219 
vl_despcor_indireta_gestao	                471411.201406064
vl_despcor_indireta_desenv_institucional	313440.750542681
vl_despcor_indireta_apoio	                2104324.33382293
vl_despesa_total	                        18711888.86
vl_desptot_etd_gestao_outros	            4337969.29
vl_desptot_suporte_negocio	                7716262.20953323
vl_desptot_indireta_gestao	                468901.360227049
vl_desptot_indireta_desenv_institucional	313258.015445232
vl_desptot_indireta_apoio                   2169835.48600704

display(rateio_step02_despesa_sti.filter(f.col("sg_entidade_regional")=='SENAI-SP'))

display(spark.sql("select * from rateio_step02_despesa_sti where sg_entidade_regional == 'SENAI-SP'"))

"""
       


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 03
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP03: Efetua os rateios das despesas totais e correntes do STI do SENAI ---- Despesas do negócio: Diretas e suporte ao negócio
# MAGIC -- Despesa Indiretas: Gestão, Desenvolvimento Institucional e Apoio 
# MAGIC --========================================================================================================================================================
# MAGIC DROP TABLE fta_gestao_financeira_sti;
# MAGIC CREATE TABLE  fta_gestao_financeira_sti;
# MAGIC (
# MAGIC SELECT 
# MAGIC  #prm_ano_fechamento AS cd_ano_fechamento
# MAGIC ,#prm_mes_fechamento AS cd_mes_fechamento    
# MAGIC ,#prm_data_corte AS dt_fechamento
# MAGIC ,o.cd_entidade_regional
# MAGIC ,o.cd_centro_responsabilidade
# MAGIC , o.vl_despesa_corrente AS vl_despcor_sti   ---- corrigido 20/10 antigo o.vl_despesa_corrente AS vl_despcor_sti_rateada   
# MAGIC --, e.vl_despesa_corrente AS vl_despcor_sti_rateada
# MAGIC --, ( o.vl_despesa_corrente / e.vl_despesa_corrente ) as pc_servico_operacional_corrente
# MAGIC , ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_etd_gestao_outros) AS vl_despcor_etd_gestao_outros_sti_rateada
# MAGIC , ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_suporte_negocio)   AS vl_despcor_suporte_negocio_sti_rateada
# MAGIC --, ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_indireta_gestao + e.vl_despcor_indireta_desenv_institucional + e.vl_despcor_indireta_apoio) as vl_custo_despesa_gestao_desev_institucional_apoio
# MAGIC , ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_indireta_gestao)   AS vl_despcor_indireta_gestao_sti_rateada
# MAGIC , ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_indireta_desenv_institucional) AS vl_despcor_indireta_desenv_institucional_sti_rateada
# MAGIC , ( o.vl_despesa_corrente / e.vl_despesa_corrente) * (e.vl_despcor_indireta_apoio)    AS vl_despcor_indireta_apoio_sti_rateada
# MAGIC 
# MAGIC , o.vl_despesa_total AS vl_desptot_sti   ---- corrigido 20/10 antigo o.vl_despesa_total AS vl_desptot_sti_rateada   
# MAGIC --, e.vl_despesa_total AS vl_desptot_sti_rateada
# MAGIC --, ( o.vl_despesa_total / e.vl_despesa_total ) as pc_servico_operacional_total
# MAGIC , ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_etd_gestao_outros) AS vl_desptot_etd_gestao_outros_sti_rateada
# MAGIC , ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_suporte_negocio)   AS vl_desptot_suporte_negocio_sti_rateada
# MAGIC --, ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_indireta_gestao + e.vl_desptot_indireta_desenv_institucional + e.vl_desptot_indireta_apoio) as vl_despesa_gestao_desev_institucional_apoio
# MAGIC , ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_indireta_gestao)   AS vl_desptot_indireta_gestao_sti_rateada
# MAGIC , ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_indireta_desenv_institucional) AS vl_desptot_indireta_desenv_institucional_sti_rateada
# MAGIC , ( o.vl_despesa_total / e.vl_despesa_total) * (e.vl_desptot_indireta_apoio)    AS vl_desptot_indireta_apoio_sti_rateada
# MAGIC FROM rateio_step01_despesa_cr_sti o
# MAGIC INNER JOIN rateio_step02_despesa_sti e 
# MAGIC ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC )
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is the final object
For the first load, this object is ~20k, keep it in 1 partition.
"""

fta_gestao_financeira_sti = rateio_step01_despesa_cr_sti\
.join(rateio_step02_despesa_sti\
      .select("cd_entidade_regional",
              "vl_despesa_corrente_step02",
              "vl_despcor_suporte_negocio_step02",
              "vl_despcor_etd_gestao_outros_step02",
              "vl_despcor_indireta_gestao_step02",
              "vl_despcor_indireta_desenv_institucional_step02",
              "vl_despcor_indireta_apoio_step02",
              "vl_despesa_total_step02",
              "vl_desptot_suporte_negocio_step02",
              "vl_desptot_etd_gestao_outros_step02",
              "vl_desptot_indireta_gestao_step02",
              "vl_desptot_indireta_desenv_institucional_step02",
              "vl_desptot_indireta_apoio_step02"),
      ["cd_entidade_regional"], "inner")\
.withColumn("vl_despcor_sti_rateada", f.col("vl_despesa_corrente"))\
.withColumn("vl_despcor_etd_gestao_outros_sti_rateada",
            ((f.col("vl_despesa_corrente") / f.col("vl_despesa_corrente_step02")) *\
             f.col("vl_despcor_etd_gestao_outros_step02")))\
.withColumn("vl_despcor_suporte_negocio_sti_rateada",
            ((f.col("vl_despesa_corrente") / f.col("vl_despesa_corrente_step02")) *\
             f.col("vl_despcor_suporte_negocio_step02")))\
.withColumn("vl_despcor_indireta_gestao_sti_rateada",
            ((f.col("vl_despesa_corrente") / f.col("vl_despesa_corrente_step02")) *\
             f.col("vl_despcor_indireta_gestao_step02")))\
.withColumn("vl_despcor_indireta_desenv_institucional_sti_rateada",
            ((f.col("vl_despesa_corrente") / f.col("vl_despesa_corrente_step02")) *\
             f.col("vl_despcor_indireta_desenv_institucional_step02")))\
.withColumn("vl_despcor_indireta_apoio_sti_rateada",
            ((f.col("vl_despesa_corrente") / f.col("vl_despesa_corrente_step02")) *\
             f.col("vl_despcor_indireta_apoio_step02")))\
.withColumn("vl_desptot_sti_rateada", f.col("vl_despesa_total"))\
.withColumn("vl_desptot_etd_gestao_outros_sti_rateada",
            ((f.col("vl_despesa_total") / f.col("vl_despesa_total_step02")) *\
             f.col("vl_desptot_etd_gestao_outros_step02")))\
.withColumn("vl_desptot_suporte_negocio_sti_rateada",
            ((f.col("vl_despesa_total") / f.col("vl_despesa_total_step02")) *\
             f.col("vl_desptot_suporte_negocio_step02")))\
.withColumn("vl_desptot_indireta_gestao_sti_rateada",
            ((f.col("vl_despesa_total") / f.col("vl_despesa_total_step02")) *\
             f.col("vl_desptot_indireta_gestao_step02")))\
.withColumn("vl_desptot_indireta_desenv_institucional_sti_rateada",
            ((f.col("vl_despesa_total") / f.col("vl_despesa_total_step02")) *\
             f.col("vl_desptot_indireta_desenv_institucional_step02")))\
.withColumn("vl_desptot_indireta_apoio_sti_rateada",
            ((f.col("vl_despesa_total") / f.col("vl_despesa_total_step02")) *\
             f.col("vl_desptot_indireta_apoio_step02")))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.drop("cd_entidade_nacional",
      "sg_entidade_regional",
      "vl_despesa_corrente",
      "vl_despcor_suporte_negocio",
      "vl_despcor_etd_gestao_outros",
      "vl_despcor_indireta_gestao",
      "vl_despcor_indireta_desenv_institucional",
      "vl_despcor_indireta_apoio",
      "vl_despesa_total",
      "vl_desptot_suporte_negocio",
      "vl_desptot_etd_gestao_outros",
      "vl_desptot_indireta_gestao",
      "vl_desptot_indireta_desenv_institucional",
      "vl_desptot_indireta_apoio",
      "vl_despesa_corrente_step02",
      "vl_despcor_suporte_negocio_step02",
      "vl_despcor_etd_gestao_outros_step02",
      "vl_despcor_indireta_gestao_step02",
      "vl_despcor_indireta_desenv_institucional_step02", 
      "vl_despcor_indireta_apoio_step02",
      "vl_despesa_total_step02",
      "vl_desptot_suporte_negocio_step02",
      "vl_desptot_etd_gestao_outros_step02",
      "vl_desptot_indireta_gestao_step02",
      "vl_desptot_indireta_desenv_institucional_step02",
      "vl_desptot_indireta_apoio_step02")\
.fillna(0, subset=["vl_despcor_sti_rateada",
                   "vl_despcor_etd_gestao_outros_sti_rateada",
                   "vl_despcor_suporte_negocio_sti_rateada",
                   "vl_despcor_indireta_gestao_sti_rateada",
                   "vl_despcor_indireta_desenv_institucional_sti_rateada",
                   "vl_despcor_indireta_apoio_sti_rateada",
                   "vl_desptot_sti_rateada",
                   "vl_desptot_etd_gestao_outros_sti_rateada",
                   "vl_desptot_suporte_negocio_sti_rateada",
                   "vl_desptot_indireta_gestao_sti_rateada",
                   "vl_desptot_indireta_desenv_institucional_sti_rateada",
                   "vl_desptot_indireta_apoio_sti_rateada"])\
.coalesce(1)\
.cache()

# Activate cache
fta_gestao_financeira_sti.count()

# COMMAND ----------

#assert  fta_gestao_financeira_sti.count() == spark.sql("select count(*) from fta_despesa_rateio_sti").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step01_despesa_cr_sti is not needed anymore.
This object can be removed from cache.
"""
rateio_step01_despesa_cr_sti = rateio_step01_despesa_cr_sti.unpersist()


# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz

# COMMAND ----------

fta_gestao_financeira_sti = tcf.add_control_fields(fta_gestao_financeira_sti, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjusting schema

# COMMAND ----------

var_column_type_map = {"cd_entidade_regional" : "int", 
                       "cd_centro_responsabilidade" : "string",                        
                       "vl_despcor_sti_rateada": "decimal(18,6)", 
                       "vl_despcor_etd_gestao_outros_sti_rateada": "decimal(18,6)", 
                       "vl_despcor_suporte_negocio_sti_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_sti_rateada": "decimal(18,6)", 
                       "vl_despcor_indireta_desenv_institucional_sti_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_apoio_sti_rateada": "decimal(18,6)",
                       "vl_desptot_sti_rateada": "decimal(18,6)",
                       "vl_desptot_etd_gestao_outros_sti_rateada": "decimal(18,6)", 
                       "vl_desptot_suporte_negocio_sti_rateada": "decimal(18,6)", 
                       "vl_desptot_indireta_gestao_sti_rateada": "decimal(18,6)", 
                       "vl_desptot_indireta_desenv_institucional_sti_rateada": "decimal(18,6)", 
                       "vl_desptot_indireta_apoio_sti_rateada": "decimal(18,6)",                        
                       "cd_mes_fechamento": "int",
                       "cd_ano_fechamento": "int", 
                       "dt_fechamento": "date"}

for c in var_column_type_map:
  fta_gestao_financeira_sti = fta_gestao_financeira_sti.withColumn(c, fta_gestao_financeira_sti[c].cast(var_column_type_map[c]))

# COMMAND ----------

"""
VALIDAÇÃO - OK

display(fta_despesa_rateada_sti.filter(f.col("sg_entidade_regional")=='SENAI-SP').orderBy("cd_centro_responsabilidade"))

display(spark.sql("select * from fta_despesa_rateio_sti where sg_entidade_regional == 'SENAI-SP' order by cd_centro_responsabilidade"))

"""

# COMMAND ----------

"""
VALIDAÇÃO - OK

Validadas as métricas a seguir para SENAI-SP:

vl_despcor_sti_cr_rateada
vl_despcor_etd_gestao_outros_sti_rateada
vl_despcor_suporte_negocio_sti_rateada
vl_despcor_indireta_gestao_sti_ratead
vl_despcor_indireta_desenv_institucional_sti_rateada 
vl_despcor_indireta_apoio_sti_rateada
vl_desptot_sti_cr_rateada
vl_desptot_etd_gestao_outros_sti_rateada
vl_desptot_suporte_negocio_sti_rateada
vl_desptot_indireta_gestao_sti_rateada
vl_desptot_indireta_desenv_institucional_sti_rateada
vl_desptot_indireta_apoio_sti_rateada
--vl_despcor_sti_rateada 
--pc_servico_operacional_corrente 
--vl_custo_despesa_gestao_desev_institucional_apoio 
--vl_desptot_sti_rateada 
--pc_servico_operacional_total 
--vl_despesa_gestao_desev_institucional_apoio 
"""
"""
df_teste = fta_despesa_rateada_sti.filter(f.col("cd_entidade_regional")==1117285)\
.drop("cd_mes_fechamento", "cd_ano_fechamento", "dt_fechamento", "dh_insercao_biz", "kv_process_control")\
.orderBy("cd_centro_responsabilidade")

df_val = spark.sql("select cd_entidade_regional, \
                   cd_centro_responsabilidade, \
                   cast(vl_despcor_sti_cr_rateada as decimal(18,6)), \
                   cast(vl_despcor_etd_gestao_outros_sti_rateada as decimal(18,6)), \
                   cast(vl_despcor_suporte_negocio_sti_rateada as decimal(18,6)), \
                   cast(vl_despcor_indireta_gestao_sti_rateada as decimal(18,6)), \
                   cast(vl_despcor_indireta_desenv_institucional_sti_rateada as decimal(18,6)), \
                   cast(vl_despcor_indireta_apoio_sti_rateada as decimal(18,6)), \
                   cast(vl_desptot_sti_cr_rateada as decimal(18,6)), \
                   cast(vl_desptot_etd_gestao_outros_sti_rateada as decimal(18,6)), \
                   cast(vl_desptot_suporte_negocio_sti_rateada as decimal(18,6)), \
                   cast(vl_desptot_indireta_gestao_sti_rateada as decimal(18,6)), \
                   cast(vl_desptot_indireta_desenv_institucional_sti_rateada as decimal(18,6)), \
                   cast(vl_desptot_indireta_apoio_sti_rateada as decimal(18,6)) \
                   from fta_despesa_rateio_sti where sg_entidade_regional == 'SENAI-SP' order by cd_centro_responsabilidade")

display(df_teste.subtract(df_val.select(df_teste.columns)))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

fta_gestao_financeira_sti.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

"""
Source fta_gestao_financeira_sti is not needed anymore.
This object can be removed from cache.
"""
fta_gestao_financeira_sti = fta_gestao_financeira_sti.unpersist()

# COMMAND ----------

