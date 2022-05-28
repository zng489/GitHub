# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	biz_biz_fta_gestao_financeira_educacao_basica_contnd
# MAGIC Tabela/Arquivo Origem	"/trs/evt/orcamento_nacional_realizado
# MAGIC /trs/mtd/corp/entidade_regional
# MAGIC /biz/orcamento/fta_despesa_rateada_negocio
# MAGIC /biz/orcamento/fte_producao_educacao_sesi"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_educacao_basica_contnd
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas de Negócios rateadas por hora escolar para Educação Básica e Continuada SESI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento e  #prm_mes_fechamento informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento e  #prm_mes_fechamento pela data atual do processamento
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC Manutenção:
# MAGIC   2020/10/26 11:00 - Thomaz Moreira - Atualização conforme manutenção da Patrícia
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
var_tables = {"origins":["/evt/orcamento_nacional_realizado", "/mtd/corp/entidade_regional", "/orcamento/fta_despesa_rateada_negocio", "/producao/fte_producao_educacao_sesi"],
  "destination":"/orcamento/fta_gestao_financeira_educacao_basica_contnd", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_educacao_basica_contnd"}
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
"adf_trigger_name":"shin",
"adf_trigger_time":"2020-08-18T13:00:00.8532201Z",
"adf_trigger_type":"Manual"
}

var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}
"""

# COMMAND ----------

"""
USE THIS FOR DEV PURPOSES
#var_sink = "{}{}{}".format(var_adls_uri, "/tmp/dev" + var_dls["folders"]["business"], var_tables["destination"]) 
"""
var_src_onr, var_src_reg =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], t) for t in var_tables["origins"][:2]]
var_src_drn, var_src_pes =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], t) for t in var_tables["origins"][-2:]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
var_src_onr, var_src_reg, var_src_drn, var_src_pes, var_sink

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
# MAGIC --====================================================================================================================================
# MAGIC -- STEP01: Obtem despesa indireta orçamento SESI a ser rateada o ano/mês e data de fechamento informados por parâmetro
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_es_step01_despesa_indireta;
# MAGIC CREATE TABLE rateio_es_step01_despesa_indireta AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_conta_contabil,
# MAGIC 
# MAGIC -- despesa total
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '303070111%' THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_vira_vida,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '30310%' OR o.cd_centro_responsabilidade LIKE '30311%'  
# MAGIC                                                              THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_etd_gestao_educacao,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '307%'       THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_suporte_negocio,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '1%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_gestao,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '2%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_desenv_inst,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '4%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_apoio,
# MAGIC 
# MAGIC --despesa corrente
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '303070111%' THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_vira_vida,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '30310%' OR o.cd_centro_responsabilidade LIKE '30311%'  
# MAGIC                                                                                                 THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_etd_gestao_educacao,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '307%'       THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_suporte_negocio,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '1%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_indireta_gestao,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '2%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_indireta_desenv_inst,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '4%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_indireta_apoio
# MAGIC 
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE o.cd_conta_contabil LIKE '3%' -- para despesas
# MAGIC 
# MAGIC AND   e.cd_entidade_nacional = 2 -- SESI
# MAGIC AND NOT -- sem transferências regimentais/regulamentares
# MAGIC     (
# MAGIC         cd_conta_contabil LIKE '31020101%' -- Federação/CNI
# MAGIC      OR cd_conta_contabil LIKE '31020102%' -- Conselho Nacional do SESI
# MAGIC      OR cd_conta_contabil LIKE '31020105%' -- IEL
# MAGIC      OR cd_conta_contabil LIKE '31011001%' -- Receita Federal (3,5% Arrecadação Indireta)
# MAGIC     )
# MAGIC     
# MAGIC AND YEAR(o.dt_lancamento) = #prm_ano_fechamento AND MONTH(o.dt_lancamento) <= #prm_mes_fechamento AND o.dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC 
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_conta_contabil;
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
I'll try to keep the same notation as documentation when possible

This is a small table, in test fase it has only ~290k records and ~4M in mem cache. Keep in 1 partition.
"""
var_o_columns = ["cd_centro_responsabilidade", "cd_conta_contabil", "vl_lancamento", "cd_entidade_regional_erp_oltp", "dt_lancamento", "dt_ultima_atualizacao_oltp"]

o = spark.read.parquet(var_src_onr)\
.select(*var_o_columns)\
.fillna(0.0, subset=["vl_lancamento"])\
.filter((f.col("cd_conta_contabil").startswith("3")) &\
        (f.year("dt_lancamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.month("dt_lancamento") <= f.lit(var_parameters["prm_mes_fechamento"]).cast("int")) &\
        (f.col("dt_ultima_atualizacao_oltp") <= f.lit(var_parameters["prm_data_corte"]).cast("timestamp")))\
.drop(*["dt_lancamento", "dt_ultima_atualizacao_oltp"])\
.coalesce(1)\
.cache()

# Activate cache
o.count()

# COMMAND ----------

"""
# e
This object will be reused a lot. It is really small:  30 records. 
Cache it and keep in 1 partition
"""

var_e_columns = ["cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_entidade_regional_erp_oltp"]

e = spark.read.parquet(var_src_reg)\
.select(*var_e_columns)\
.filter(f.col("cd_entidade_nacional").isin(2))\
.coalesce(1)\
.cache()

# Activate cache
e.count()

# COMMAND ----------

"""
I'm testing for the first load and the object is really small: ~2,7k records. Can be kept in 1 partition
"""
rateio_es_step01_despesa_indireta = o\
.filter(~((f.col("cd_conta_contabil").startswith("31020101")) |\
          (f.col("cd_conta_contabil").startswith("31020102")) |\
          (f.col("cd_conta_contabil").startswith("31020105")) |\
          (f.col("cd_conta_contabil").startswith("31011001"))))\
.join(e, ["cd_entidade_regional_erp_oltp"], "inner")\
.drop(*["cd_entidade_regional_erp_oltp"])\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_conta_contabil")\
.agg(f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("303070111"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_vira_vida"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30310")) | \
                  (f.col("cd_centro_responsabilidade").startswith("30311")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_etd_gestao_educacao"),
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("307"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_suporte_negocio"),
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("1"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_indireta_gestao"),
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("2"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_indireta_desenv_inst"),
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("4"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_indireta_apoio"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("303070111")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_vira_vida"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("30310")) | \
                  (f.col("cd_centro_responsabilidade").startswith("30311")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_etd_gestao_educacao"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("307")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_suporte_negocio"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("1")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_indireta_gestao"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("2")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_indireta_desenv_inst"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")) & \
                  (f.col("cd_centro_responsabilidade").startswith("4")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_indireta_apoio"))\
.fillna(0, subset=["vl_desptot_vira_vida",
                   "vl_desptot_etd_gestao_educacao",
                   "vl_desptot_suporte_negocio",
                   "vl_desptot_indireta_gestao",
                   "vl_desptot_indireta_desenv_inst",
                   "vl_desptot_indireta_apoio",
                   "vl_despcor_vira_vida",
                   "vl_despcor_etd_gestao_educacao",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_indireta_gestao",
                   "vl_despcor_indireta_desenv_inst",
                   "vl_despcor_indireta_apoio"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_es_step01_despesa_indireta.count() == spark.sql("select count(*) from rateio_es_step01_despesa_indireta").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 02
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP02: Obtem dos drivers de rateio a serem aplicados sobre negócio SESI o ano/mês fechamento informados por parâmetro
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_es_step02_despesa_negocio;
# MAGIC CREATE TABLE rateio_es_step02_despesa_negocio AS
# MAGIC SELECT
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC 
# MAGIC --despesa total
# MAGIC SUM(vl_despesa_total)                                      AS vl_despesa_total,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '30301%' 
# MAGIC          OR   cd_centro_responsabilidade LIKE     '3030201%'
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_educ_basica_contnd,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '303%' 
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30301%' 
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '3030201%' 
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_educacao_outros,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '304%' 
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_ssi,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '305%' 
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_cultura,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '306%' 
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_cooperacao_social,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '3%' 
# MAGIC          AND substring(cd_centro_responsabilidade, 1, 3) NOT IN ('303','304','305','306')
# MAGIC          THEN vl_despesa_total END)                     AS vl_desptot_outro_servico,
# MAGIC          
# MAGIC -- despesa corrente
# MAGIC SUM(vl_despesa_corrente)                                   AS vl_despesa_corrente,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '30301%' 
# MAGIC          OR   cd_centro_responsabilidade LIKE     '3030201%'
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_educ_basica_contnd,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '303%' 
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30301%' 
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '3030201%' 
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_educacao_outros,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '304%' 
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_ssi,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '305%' 
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_cultura,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '306%' 
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_cooperacao_social,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE     '3%' 
# MAGIC          AND substring(cd_centro_responsabilidade, 1, 3) NOT IN ('303','304','305','306')
# MAGIC          THEN vl_despesa_corrente END)                     AS vl_despcor_outro_servico
# MAGIC 
# MAGIC FROM
# MAGIC (  -- leitura só para somar as colunas em um único campo vl_despesa_corrente, e trabalhar com ele
# MAGIC    SELECT 
# MAGIC    e.cd_entidade_nacional,
# MAGIC    e.sg_entidade_regional,
# MAGIC    o.cd_entidade_regional,
# MAGIC    o.cd_centro_responsabilidade,
# MAGIC 
# MAGIC     -- despesa total
# MAGIC    (vl_despesa_total + vl_desptot_vira_vida_rateada + vl_desptot_olimpiada_rateada + vl_desptot_etd_gestao_educacao_rateada + 
# MAGIC     vl_desptot_etd_gestao_outros_rateada + vl_desptot_suporte_negocio_rateada + vl_desptot_indireta_gestao_rateada +
# MAGIC     vl_desptot_indireta_desenv_institucional_rateada + vl_desptot_indireta_apoio_rateada
# MAGIC    ) AS vl_despesa_total,
# MAGIC 
# MAGIC     -- despesa corrente
# MAGIC    (vl_despesa_corrente + vl_despcor_vira_vida_rateada + vl_despcor_olimpiada_rateada + vl_despcor_etd_gestao_educacao_rateada + 
# MAGIC     vl_despcor_etd_gestao_outros_rateada + vl_despcor_suporte_negocio_rateada + vl_despcor_indireta_gestao_rateada +
# MAGIC     vl_despcor_indireta_desenv_institucional_rateada + vl_despcor_indireta_apoio_rateada
# MAGIC    ) AS vl_despesa_corrente
# MAGIC    
# MAGIC    FROM fta_despesa_rateada_negocio o
# MAGIC    INNER JOIN entidade_regional e 
# MAGIC    ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC    WHERE  e.cd_entidade_nacional = 2 -- SESI 
# MAGIC    AND o.cd_ano_fechamento = #prm_ano_fechamento AND o.cd_mes_fechamento = #prm_mes_fechamento
# MAGIC 
# MAGIC ) o
# MAGIC GROUP BY
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional;
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only 28 record. Keep in 1 partition
"""

var_drn_columns = ["cd_entidade_regional", "cd_centro_responsabilidade", "vl_despesa_total", "vl_desptot_vira_vida_rateada", "vl_desptot_olimpiada_rateada", "vl_desptot_etd_gestao_educacao_rateada", "vl_desptot_etd_gestao_outros_rateada", "vl_desptot_suporte_negocio_rateada", "vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_apoio_rateada", "vl_despesa_corrente", "vl_despcor_vira_vida_rateada", "vl_despcor_olimpiada_rateada", "vl_despcor_etd_gestao_educacao_rateada", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_suporte_negocio_rateada", "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada", "cd_ano_fechamento", "cd_mes_fechamento"]

rateio_es_step02_despesa_negocio = spark.read.parquet(var_src_drn)\
.select(*var_drn_columns)\
.filter((f.col("cd_ano_fechamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.col("cd_mes_fechamento") == f.lit(var_parameters["prm_mes_fechamento"]).cast("int")))\
.withColumn("vl_despesa_total", 
            f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + \
            f.col("vl_desptot_etd_gestao_outros_rateada") + f.col("vl_desptot_suporte_negocio_rateada") + f.col("vl_desptot_indireta_gestao_rateada") + \
            f.col("vl_desptot_indireta_desenv_institucional_rateada") + f.col("vl_desptot_indireta_apoio_rateada"))\
.withColumn("vl_despesa_corrente", 
            f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + \
            f.col("vl_despcor_etd_gestao_outros_rateada") + f.col("vl_despcor_suporte_negocio_rateada") + f.col("vl_despcor_indireta_gestao_rateada") + \
            f.col("vl_despcor_indireta_desenv_institucional_rateada") + f.col("vl_despcor_indireta_apoio_rateada"))\
.join(e, ["cd_entidade_regional"], "inner")\
.drop("vl_desptot_vira_vida_rateada", "vl_desptot_olimpiada_rateada", "vl_desptot_etd_gestao_educacao_rateada", "vl_desptot_etd_gestao_outros_rateada", "vl_desptot_suporte_negocio_rateada", "vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_apoio_rateada", "vl_despcor_vira_vida_rateada", "vl_despcor_olimpiada_rateada", "vl_despcor_etd_gestao_educacao_rateada", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_suporte_negocio_rateada", "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada","cd_ano_fechamento", "cd_mes_fechamento", "cd_entidade_regional_erp_oltp")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum("vl_despesa_total").alias("vl_despesa_total"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30301")) | \
                  (f.col("cd_centro_responsabilidade").startswith("3030201")), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_educ_basica_contnd"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30301")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("3030201")), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_educacao_outros"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("304")), \
                  f.col("vl_despesa_total")) \
           .otherwise(f.lit(0))).alias("vl_desptot_ssi"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("305")), \
                  f.col("vl_despesa_total")) \
           .otherwise(f.lit(0))).alias("vl_desptot_cultura"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("306")), \
                  f.col("vl_despesa_total")) \
           .otherwise(f.lit(0))).alias("vl_desptot_cooperacao_social"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("3")) & \
                  ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','304','305','306')), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_outro_servico"),
     f.sum("vl_despesa_corrente").alias("vl_despesa_corrente"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30301")) | \
                  (f.col("cd_centro_responsabilidade").startswith("3030201")), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_educ_basica_contnd"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30301")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("3030201")), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_educacao_outros"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("304")), \
                  f.col("vl_despesa_corrente")) \
           .otherwise(f.lit(0))).alias("vl_despcor_ssi"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("305")), \
                  f.col("vl_despesa_corrente")) \
           .otherwise(f.lit(0))).alias("vl_despcor_cultura"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("306")), \
                  f.col("vl_despesa_corrente")) \
           .otherwise(f.lit(0))).alias("vl_despcor_cooperacao_social"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("3")) & \
                  ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','304','305','306')), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_outro_servico"))\
.fillna(0, subset=["vl_despesa_total",
                   "vl_desptot_educ_basica_contnd",
                   "vl_desptot_educacao_outros",
                   "vl_desptot_ssi",
                   "vl_desptot_cultura",
                   "vl_desptot_cooperacao_social",
                   "vl_desptot_outro_servico",
                   "vl_despesa_corrente",
                   "vl_despcor_educ_basica_contnd",
                   "vl_despcor_educacao_outros",
                   "vl_despcor_ssi",
                   "vl_despcor_cultura",
                   "vl_despcor_cooperacao_social",
                   "vl_despcor_outro_servico"])\
.coalesce(1)


# COMMAND ----------

#assert  rateio_es_step02_despesa_negocio.count() == spark.sql("select count(*) from rateio_es_step02_despesa_negocio").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 03
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP03: Obtem os drivers de rateio a de Horas de Educação Básica e Continuada para o ano/mês fechamento informados por parâmetro
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_es_step03_hora_aula;
# MAGIC CREATE TABLE rateio_es_step03_hora_aula AS
# MAGIC SELECT
# MAGIC cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC qt_matricula_educ_basica_contnd,
# MAGIC qt_matricula_educ_basica_contnd_gratuidade_reg,
# MAGIC qt_matricula_educ_basica_contnd_gratuidade_nreg,
# MAGIC qt_matricula_educ_basica_contnd_paga,      
# MAGIC qt_hr_aula,
# MAGIC SUM(qt_hr_aula) OVER(PARTITION BY cd_entidade_regional) AS qt_hr_aula_total_dr,
# MAGIC qt_hr_aula_gratuidade_reg,
# MAGIC qt_hr_aula_gratuidade_nreg,
# MAGIC qt_hr_aula_paga
# MAGIC FROM 
# MAGIC (     -- agrega por cd_entidade_regional e cd_centro_responsabilidade no fechamento
# MAGIC       SELECT 
# MAGIC       cd_entidade_regional,
# MAGIC       cd_centro_responsabilidade,
# MAGIC       
# MAGIC       SUM(qt_matricula_educacao_sesi)                 AS qt_matricula_educ_basica_contnd,
# MAGIC       SUM(qt_matricula_educacao_sesi_gratuidade_reg)  AS qt_matricula_educ_basica_contnd_gratuidade_reg,
# MAGIC       SUM(qt_matricula_educacao_sesi_gratuidade_nreg) AS qt_matricula_educ_basica_contnd_gratuidade_nreg,
# MAGIC       SUM(qt_matricula_educacao_sesi_paga)            AS qt_matricula_educ_basica_contnd_paga,      
# MAGIC       SUM(qt_hr_aula + qt_hr_reconhec_saberes)        AS qt_hr_aula,      ----- Alterado em 28/04/2021
# MAGIC       SUM(qt_hr_aula_gratuidade_reg + qt_hr_recon_sab_gratuidade_reg) AS qt_hr_aula_gratuidade_reg,    ----- Alterado em 28/04/2021
# MAGIC       SUM(qt_hr_aula_gratuidade_nreg)                 AS qt_hr_aula_gratuidade_nreg,
# MAGIC       SUM(qt_hr_aula_paga)                            AS qt_hr_aula_paga
# MAGIC 
# MAGIC       FROM fte_producao_educacao_sesi
# MAGIC       WHERE 
# MAGIC       -- Educação Básica e Continuada
# MAGIC       (cd_centro_responsabilidade LIKE '30301%' OR cd_centro_responsabilidade LIKE '3030201%')
# MAGIC       AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC       GROUP BY 
# MAGIC       cd_entidade_regional,
# MAGIC       cd_centro_responsabilidade
# MAGIC )
# MAGIC ;
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This window will be re-used during the process.
"""

window = Window.partitionBy("cd_entidade_regional")

# COMMAND ----------

"""
This is a really small table, in test fase it has 238 records. Keep in 1 partition.
""" 

var_pes_columns = ["cd_entidade_regional", "cd_centro_responsabilidade", "qt_matricula_educacao_sesi", "qt_matricula_educacao_sesi_gratuidade_reg", "qt_matricula_educacao_sesi_gratuidade_nreg", "qt_matricula_educacao_sesi_paga", "qt_hr_aula", "qt_hr_reconhec_saberes", "qt_hr_aula_gratuidade_reg", "qt_hr_recon_sab_gratuidade_reg", "qt_hr_aula_gratuidade_nreg", "qt_hr_aula_paga", "cd_ano_fechamento", "cd_mes_fechamento"]

rateio_es_step03_hora_aula = spark.read.parquet(var_src_pes)\
.select(*var_pes_columns)\
.filter(((f.col("cd_centro_responsabilidade").startswith("30301")) |\
         (f.col("cd_centro_responsabilidade").startswith("3030201"))) &\
        (f.col("cd_ano_fechamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.col("cd_mes_fechamento") == f.lit(var_parameters["prm_mes_fechamento"]).cast("int")))\
.withColumn("qt_hr_aula", f.col("qt_hr_aula") + f.col("qt_hr_reconhec_saberes"))\
.withColumn("qt_hr_aula_gratuidade_reg", f.col("qt_hr_aula_gratuidade_reg") + f.col("qt_hr_recon_sab_gratuidade_reg"))\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("qt_matricula_educacao_sesi").alias("qt_matricula_educacao_sesi"),
     f.sum("qt_matricula_educacao_sesi_gratuidade_reg").alias("qt_matricula_educacao_sesi_gratuidade_reg"),
     f.sum("qt_matricula_educacao_sesi_gratuidade_nreg").alias("qt_matricula_educacao_sesi_gratuidade_nreg"),
     f.sum("qt_matricula_educacao_sesi_paga").alias("qt_matricula_educacao_sesi_paga"),
     f.sum("qt_hr_aula").alias("qt_hr_aula"),
     f.sum("qt_hr_aula_gratuidade_reg").alias("qt_hr_aula_gratuidade_reg"),
     f.sum("qt_hr_aula_gratuidade_nreg").alias("qt_hr_aula_gratuidade_nreg"),
     f.sum("qt_hr_aula_paga").alias("qt_hr_aula_paga"))\
.withColumn("qt_hr_aula_total_dr", f.sum("qt_hr_aula").over(window))\
.fillna(0, subset=["qt_matricula_educacao_sesi",
                   "qt_matricula_educacao_sesi_gratuidade_reg",
                   "qt_matricula_educacao_sesi_gratuidade_nreg",
                   "qt_matricula_educacao_sesi_paga",
                   "qt_hr_aula",
                   "qt_hr_aula_gratuidade_reg",
                   "qt_hr_aula_gratuidade_nreg",
                   "qt_hr_aula_paga",
                   "qt_hr_aula_total_dr"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_es_step03_hora_aula.count()

# COMMAND ----------

#assert  rateio_es_step03_hora_aula.count() == spark.sql("select count(*) from rateio_es_step03_hora_aula").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 04
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP04: Calcula as despesas indiretas educação basica e continuada rateadas
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_es_step04_01_despesa_indireta_x_driver_rateio;
# MAGIC CREATE TABLE rateio_es_step04_01_despesa_indireta_x_driver_rateio AS
# MAGIC SELECT DISTINCT
# MAGIC H.cd_entidade_nacional,
# MAGIC H.sg_entidade_regional,
# MAGIC H.cd_entidade_regional,
# MAGIC H.cd_centro_responsabilidade,
# MAGIC I.cd_conta_contabil,
# MAGIC 
# MAGIC -- hora escolar
# MAGIC H.qt_hr_aula,
# MAGIC H.qt_hr_aula_total_dr,
# MAGIC 
# MAGIC -- despesa negócio
# MAGIC --    desposa total
# MAGIC P.vl_desptot_educ_basica_contnd,
# MAGIC P.vl_desptot_educ_basica_contnd + P.vl_desptot_educacao_outros AS vl_desptot_educacao,
# MAGIC P.vl_despesa_total,
# MAGIC --    despesa corrente
# MAGIC P.vl_despcor_educ_basica_contnd,
# MAGIC P.vl_despcor_educ_basica_contnd + P.vl_despcor_educacao_outros AS vl_despcor_educacao,
# MAGIC P.vl_despesa_corrente,
# MAGIC 
# MAGIC -- Despesas indiretas a ratear
# MAGIC --    desposa total
# MAGIC I.vl_desptot_vira_vida,
# MAGIC I.vl_desptot_etd_gestao_educacao,
# MAGIC I.vl_desptot_suporte_negocio,
# MAGIC I.vl_desptot_indireta_gestao,
# MAGIC I.vl_desptot_indireta_apoio,
# MAGIC I.vl_desptot_indireta_desenv_inst,
# MAGIC 
# MAGIC --    despesa corrente
# MAGIC I.vl_despcor_vira_vida,
# MAGIC I.vl_despcor_etd_gestao_educacao,
# MAGIC I.vl_despcor_suporte_negocio,
# MAGIC I.vl_despcor_indireta_gestao,
# MAGIC I.vl_despcor_indireta_apoio,
# MAGIC I.vl_despcor_indireta_desenv_inst
# MAGIC 
# MAGIC FROM       rateio_es_step01_despesa_indireta I
# MAGIC INNER JOIN rateio_es_step02_despesa_negocio  P ON I.cd_entidade_regional = P.cd_entidade_regional
# MAGIC INNER JOIN rateio_es_step03_hora_aula        H ON I.cd_entidade_regional = H.cd_entidade_regional;
# MAGIC 
# MAGIC ----------------------------------------------------------------
# MAGIC 
# MAGIC DROP TABLE   rateio_es_step04_02_despesa_indireta_rateada;
# MAGIC CREATE TABLE rateio_es_step04_02_despesa_indireta_rateada AS
# MAGIC SELECT
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC cd_conta_contabil,
# MAGIC 
# MAGIC -- despesa total
# MAGIC vl_desptot_vira_vida * (qt_hr_aula/qt_hr_aula_total_dr)                              AS vl_desptot_vira_vida_educ_basica_contnd_rateada,
# MAGIC vl_desptot_etd_gestao_educacao * 
# MAGIC (vl_desptot_educ_basica_contnd/vl_desptot_educacao)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_desptot_etd_gestao_educ_basica_contnd_rateada,
# MAGIC vl_desptot_suporte_negocio * 
# MAGIC (vl_desptot_educ_basica_contnd/vl_despesa_total)*(qt_hr_aula/qt_hr_aula_total_dr)    AS vl_desptot_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC vl_desptot_indireta_gestao * 
# MAGIC (vl_desptot_educ_basica_contnd/vl_despesa_total)*(qt_hr_aula/qt_hr_aula_total_dr)    AS vl_desptot_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC vl_desptot_indireta_apoio * 
# MAGIC (vl_desptot_educ_basica_contnd/vl_despesa_total)*(qt_hr_aula/qt_hr_aula_total_dr)    AS vl_desptot_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC vl_desptot_indireta_desenv_inst * 
# MAGIC (vl_desptot_educ_basica_contnd/vl_despesa_total)*(qt_hr_aula/qt_hr_aula_total_dr)    AS vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada,
# MAGIC 
# MAGIC -- despesa corrente
# MAGIC vl_despcor_vira_vida * (qt_hr_aula/qt_hr_aula_total_dr)                              AS vl_despcor_vira_vida_educ_basica_contnd_rateada,
# MAGIC vl_despcor_etd_gestao_educacao * 
# MAGIC (vl_despcor_educ_basica_contnd/vl_despcor_educacao)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_despcor_etd_gestao_educ_basica_contnd_rateada,
# MAGIC vl_despcor_suporte_negocio * 
# MAGIC (vl_despcor_educ_basica_contnd/vl_despesa_corrente)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_despcor_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC vl_despcor_indireta_gestao * 
# MAGIC (vl_despcor_educ_basica_contnd/vl_despesa_corrente)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_despcor_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC vl_despcor_indireta_apoio * 
# MAGIC (vl_despcor_educ_basica_contnd/vl_despesa_corrente)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_despcor_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC vl_despcor_indireta_desenv_inst * 
# MAGIC (vl_despcor_educ_basica_contnd/vl_despesa_corrente)*(qt_hr_aula/qt_hr_aula_total_dr) AS vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada
# MAGIC 
# MAGIC FROM rateio_es_step04_01_despesa_indireta_x_driver_rateio;
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only ~22k records and ~38kb in mem cache. Keep in 1 partition.

"""

rateio_es_step04_01_despesa_indireta_x_driver_rateio = rateio_es_step01_despesa_indireta\
.join(rateio_es_step02_despesa_negocio.drop("cd_entidade_nacional", "sg_entidade_regional", "vl_desptot_ssi", "vl_desptot_cultura", "vl_desptot_cooperacao_social", "vl_desptot_outro_servico",
                                            "vl_despcor_ssi", "vl_despcor_cultura", "vl_despcor_cooperacao_social", "vl_despcor_outro_servico"), 
      ["cd_entidade_regional"], "inner")\
.join(rateio_es_step03_hora_aula.drop("qt_matricula_educacao_sesi", "qt_matricula_educacao_sesi_gratuidade_reg", "qt_matricula_educacao_sesi_gratuidade_nreg", "qt_matricula_educacao_sesi_paga",
                                      "qt_hr_aula_gratuidade_reg", "qt_hr_aula_gratuidade_nreg", "qt_hr_aula_paga"), 
      ["cd_entidade_regional"], "inner")\
.withColumn("vl_desptot_educacao", f.col("vl_desptot_educ_basica_contnd") + f.col("vl_desptot_educacao_outros"))\
.withColumn("vl_despcor_educacao", f.col("vl_despcor_educ_basica_contnd") + f.col("vl_despcor_educacao_outros"))\
.drop("vl_desptot_educacao_outros", "vl_despcor_educacao_outros")\
.fillna(0, subset=["qt_hr_aula",
                   "qt_hr_aula_total_dr",
                   "vl_desptot_educ_basica_contnd",
                   "vl_desptot_educacao",
                   "vl_despesa_total",
                   "vl_despcor_educ_basica_contnd",
                   "vl_despcor_educacao",
                   "vl_despesa_corrente",
                   "vl_desptot_vira_vida",
                   "vl_desptot_etd_gestao_educacao",
                   "vl_desptot_suporte_negocio",
                   "vl_desptot_indireta_gestao",
                   "vl_desptot_indireta_apoio",
                   "vl_desptot_indireta_desenv_inst",
                   "vl_despcor_vira_vida",
                   "vl_despcor_etd_gestao_educacao",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_indireta_gestao",
                   "vl_despcor_indireta_apoio",
                   "vl_despcor_indireta_desenv_inst"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_es_step04_01_despesa_indireta_x_driver_rateio.count() == spark.sql("select count(*) from rateio_es_step04_01_despesa_indireta_x_driver_rateio").collect()[0][0]

# COMMAND ----------

"""
This is a small table, in test fase it has only 22k records. Keep in 1 partition.
"""

rateio_es_step04_02_despesa_indireta_rateada = rateio_es_step04_01_despesa_indireta_x_driver_rateio\
.withColumn("vl_desptot_vira_vida_educ_basica_contnd_rateada",
            ((f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_vira_vida")))\
.withColumn("vl_desptot_etd_gestao_educ_basica_contnd_rateada",
            ((f.col("vl_desptot_educ_basica_contnd") / f.col("vl_desptot_educacao")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_etd_gestao_educacao")))\
.withColumn("vl_desptot_suporte_negocio_educ_basica_contnd_rateada",
            ((f.col("vl_desptot_educ_basica_contnd") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_suporte_negocio")))\
.withColumn("vl_desptot_indireta_gestao_educ_basica_contnd_rateada",
            ((f.col("vl_desptot_educ_basica_contnd") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_indireta_gestao")))\
.withColumn("vl_desptot_indireta_apoio_educ_basica_contnd_rateada",
            ((f.col("vl_desptot_educ_basica_contnd") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_indireta_apoio")))\
.withColumn("vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada",
            ((f.col("vl_desptot_educ_basica_contnd") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_desptot_indireta_desenv_inst")))\
.withColumn("vl_despcor_vira_vida_educ_basica_contnd_rateada",
            ((f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_vira_vida")))\
.withColumn("vl_despcor_etd_gestao_educ_basica_contnd_rateada",
            ((f.col("vl_despcor_educ_basica_contnd") / f.col("vl_despcor_educacao")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_etd_gestao_educacao")))\
.withColumn("vl_despcor_suporte_negocio_educ_basica_contnd_rateada",
            ((f.col("vl_despcor_educ_basica_contnd") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_suporte_negocio")))\
.withColumn("vl_despcor_indireta_gestao_educ_basica_contnd_rateada",
            ((f.col("vl_despcor_educ_basica_contnd") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_indireta_gestao")))\
.withColumn("vl_despcor_indireta_apoio_educ_basica_contnd_rateada",
            ((f.col("vl_despcor_educ_basica_contnd") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_indireta_apoio")))\
.withColumn("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada",
            ((f.col("vl_despcor_educ_basica_contnd") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_aula") / f.col("qt_hr_aula_total_dr")) *\
             f.col("vl_despcor_indireta_desenv_inst")))\
.fillna(0, subset=["vl_desptot_vira_vida_educ_basica_contnd_rateada",
                   "vl_desptot_etd_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada",
                   "vl_despcor_vira_vida_educ_basica_contnd_rateada",
                   "vl_despcor_etd_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada"])\
.drop("qt_hr_aula", "qt_hr_aula_total_dr", "vl_desptot_educ_basica_contnd", "vl_desptot_educacao", "vl_despesa_total", "vl_despcor_educ_basica_contnd", "vl_despcor_educacao", "vl_despesa_corrente", "vl_desptot_vira_vida", "vl_desptot_etd_gestao_educacao", "vl_desptot_suporte_negocio", "vl_desptot_indireta_gestao", "vl_desptot_indireta_apoio", "vl_desptot_indireta_desenv_inst", "vl_despcor_vira_vida", "vl_despcor_etd_gestao_educacao", "vl_despcor_suporte_negocio", "vl_despcor_indireta_gestao", "vl_despcor_indireta_apoio", "vl_despcor_indireta_desenv_inst")\
.coalesce(1)


# COMMAND ----------

#assert  rateio_es_step04_02_despesa_indireta_rateada.count() == spark.sql("select count(*) from rateio_es_step04_02_despesa_indireta_rateada").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 05
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP05: Obtem as despesas diretas de Educação Básica e Continuada o ano/mês e data fechamento informados por parâmetro 
# MAGIC -- e regisra o resultado do rateio
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE rateio_es_step05_01_despesa_direta_educ_basica_contnd;
# MAGIC CREATE TABLE rateio_es_step05_01_despesa_direta_educ_basica_contnd AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional, 
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.cd_conta_contabil,
# MAGIC SUM(o.vl_lancamento)  AS vl_desptot_educ_basica_contnd,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_educ_basica_contnd
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE o.cd_conta_contabil LIKE '3%' -- para despesas 
# MAGIC AND   o.cd_entidade_nacional = 2 -- SESI
# MAGIC AND   (cd_centro_responsabilidade LIKE '30301%'     -- EDUCACAO_BASICA
# MAGIC OR     cd_centro_responsabilidade LIKE '3030201%') -- EDUCACAO_CONTINUADA sem eventos
# MAGIC AND YEAR(o.dt_lancamento) = #prm_ano_fechamento AND MONTH(o.dt_lancamento) <= #prm_mes_fechamento AND o.dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC 
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.cd_conta_contabil;
# MAGIC 
# MAGIC ---------------------------------------------------------------
# MAGIC -- Em 13/11/2020 último passo abaixo que gravava em gravar em biz/orcamento/fta_gestao_financeira_ensino_profissional_contnd
# MAGIC -- foi transformado em intermedirário e criado um STEP06 a frente
# MAGIC ---------------------------------------------------------------
# MAGIC --DROP TABLE   rateio_es_step05_02_despesa_rateada;
# MAGIC CREATE TABLE rateio_es_step05_02_despesa_rateada AS
# MAGIC SELECT
# MAGIC NVL(D.cd_entidade_nacional, R.cd_entidade_nacional) AS cd_entidade_nacional, 
# MAGIC NVL(D.sg_entidade_regional, R.sg_entidade_regional) AS sg_entidade_regional,
# MAGIC NVL(D.cd_entidade_regional ,R.cd_entidade_regional) AS cd_entidade_regional,
# MAGIC NVL(D.cd_centro_responsabilidade ,R.cd_centro_responsabilidade) AS cd_centro_responsabilidade,
# MAGIC NVL(D.cd_conta_contabil ,R.cd_conta_contabil)       AS cd_conta_contabil,
# MAGIC 
# MAGIC NVL(D.vl_desptot_educ_basica_contnd, 0)                              AS vl_desptot_educ_basica_contnd,
# MAGIC NVL(R.vl_desptot_vira_vida_educ_basica_contnd_rateada, 0)            AS vl_desptot_vira_vida_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_desptot_etd_gestao_educ_basica_contnd_rateada, 0)           AS vl_desptot_etd_gestao_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_desptot_suporte_negocio_educ_basica_contnd_rateada, 0)      AS vl_desptot_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_desptot_indireta_gestao_educ_basica_contnd_rateada, 0)      AS vl_desptot_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_desptot_indireta_apoio_educ_basica_contnd_rateada, 0)       AS vl_desptot_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada, 0) AS vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada,
# MAGIC 
# MAGIC NVL(D.vl_despcor_educ_basica_contnd, 0)                              AS vl_despcor_educ_basica_contnd,
# MAGIC NVL(R.vl_despcor_vira_vida_educ_basica_contnd_rateada, 0)            AS vl_despcor_vira_vida_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_despcor_etd_gestao_educ_basica_contnd_rateada, 0)           AS vl_despcor_etd_gestao_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_despcor_suporte_negocio_educ_basica_contnd_rateada, 0)      AS vl_despcor_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_despcor_indireta_gestao_educ_basica_contnd_rateada, 0)      AS vl_despcor_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_despcor_indireta_apoio_educ_basica_contnd_rateada, 0)       AS vl_despcor_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC NVL(R.vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada, 0) AS vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada
# MAGIC 
# MAGIC FROM rateio_es_step04_02_despesa_indireta_rateada R 
# MAGIC FULL OUTER JOIN rateio_es_step05_01_despesa_direta_educ_basica_contnd D -- em 22/10/2020
# MAGIC ON  D.cd_entidade_regional       = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC AND D.cd_conta_contabil          = R.cd_conta_contabil;
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only ~6k records and ~150K in mem cache. Keep in 1 partition.
"""

rateio_es_step05_01_despesa_direta_educ_basica_contnd = o\
.filter((f.col("cd_centro_responsabilidade").startswith("30301")) |\
        (f.col("cd_centro_responsabilidade").startswith("3030201")))\
.join(e, ["cd_entidade_regional_erp_oltp"], "inner")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_conta_contabil")\
.agg(f.sum("vl_lancamento").alias("vl_desptot_educ_basica_contnd"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")), \
                  f.col("vl_lancamento")) \
           .otherwise(f.lit(0))).alias("vl_despcor_educ_basica_contnd"))\
.withColumnRenamed("cd_entidade_nacional", "cd_entidade_nacional_05")\
.withColumnRenamed("sg_entidade_regional", "sg_entidade_regional_05")\
.withColumnRenamed("cd_entidade_regional", "cd_entidade_regional_05")\
.withColumnRenamed("cd_centro_responsabilidade", "cd_centro_responsabilidade_05")\
.withColumnRenamed("cd_conta_contabil", "cd_conta_contabil_05")\
.drop("vl_lancamento", "cd_entidade_regional_erp_oltp")\
.coalesce(1)\
.cache()

# Activate cache
rateio_es_step05_01_despesa_direta_educ_basica_contnd.count()

# COMMAND ----------

#assert  rateio_es_step05_01_despesa_direta_educ_basica_contnd.count() == spark.sql("select count(*) from rateio_es_step05_01_despesa_direta_educ_basica_contnd").collect()[0][0]

# COMMAND ----------

"""
Source o and e are not needed anymore.
These objects can be removed from cache.
"""
o = o.unpersist()
e = e.unpersist()

# COMMAND ----------

"""
This is the final object
This is a small table, in test fase it has only ~22k records and ~2,6M in mem cache. Keep in 1 partition.
"""

rateio_es_step05_02_despesa_rateada = rateio_es_step04_02_despesa_indireta_rateada\
.join(rateio_es_step05_01_despesa_direta_educ_basica_contnd,
      (rateio_es_step04_02_despesa_indireta_rateada.cd_entidade_regional == rateio_es_step05_01_despesa_direta_educ_basica_contnd.cd_entidade_regional_05) &\
      (rateio_es_step04_02_despesa_indireta_rateada.cd_centro_responsabilidade == rateio_es_step05_01_despesa_direta_educ_basica_contnd.cd_centro_responsabilidade_05) &\
      (rateio_es_step04_02_despesa_indireta_rateada.cd_conta_contabil == rateio_es_step05_01_despesa_direta_educ_basica_contnd.cd_conta_contabil_05),
      "outer")\
.withColumn("cd_entidade_nacional",
           f.when(f.col("cd_entidade_nacional_05").isNull(), f.col("cd_entidade_nacional"))\
           .otherwise(f.col("cd_entidade_nacional_05")))\
.withColumn("sg_entidade_regional",
           f.when(f.col("sg_entidade_regional_05").isNull(), f.col("sg_entidade_regional"))\
           .otherwise(f.col("sg_entidade_regional_05")))\
.withColumn("cd_entidade_regional",
           f.when(f.col("cd_entidade_regional_05").isNull(), f.col("cd_entidade_regional"))\
           .otherwise(f.col("cd_entidade_regional_05")))\
.withColumn("cd_centro_responsabilidade",
           f.when(f.col("cd_centro_responsabilidade_05").isNull(), f.col("cd_centro_responsabilidade"))\
           .otherwise(f.col("cd_centro_responsabilidade_05")))\
.withColumn("cd_conta_contabil",
           f.when(f.col("cd_conta_contabil_05").isNull(), f.col("cd_conta_contabil"))\
           .otherwise(f.col("cd_conta_contabil_05")))\
.drop("cd_entidade_nacional_05", "sg_entidade_regional_05", "cd_entidade_regional_05", "cd_centro_responsabilidade_05", "cd_conta_contabil_05")\
.fillna(0, subset=["vl_desptot_educ_basica_contnd",
                   "vl_desptot_vira_vida_educ_basica_contnd_rateada",
                   "vl_desptot_etd_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada",
                   "vl_despcor_educ_basica_contnd",
                   "vl_despcor_vira_vida_educ_basica_contnd_rateada",
                   "vl_despcor_etd_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_es_step05_02_despesa_rateada.count()

# COMMAND ----------

#assert  fta_despesa_rateada_educacao_basica_contnd.count() == spark.sql("select count(*) from fta_despesa_rateada_educacao_basica_contnd").collect()[0][0]

# COMMAND ----------

"""
Source rateio_ep_step05_01_despesa_direta_ensino_prof is not needed anymore.
This object can be removed from cache.
"""
rateio_es_step05_01_despesa_direta_educ_basica_contnd = rateio_es_step05_01_despesa_direta_educ_basica_contnd.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 06
# MAGIC ```
# MAGIC ====================================================================================================================================
# MAGIC -- STEP06: Calcula despesa com gratuidade a partir das despesas rateadas unificadas no STEP05 com a produção obtida no STEP03
# MAGIC -- e registra o resultado final do rateio incluindo as despesas com gratuidade
# MAGIC -- Incluído em 13/11/2020 
# MAGIC --====================================================================================================================================
# MAGIC -- gravar em biz/orcamento/fta_gestao_financeira_ensino_profissional_contnd
# MAGIC DROP TABLE   fta_gestao_financeira_educacao_basica_contnd;
# MAGIC CREATE TABLE fta_gestao_financeira_educacao_basica_contnd AS
# MAGIC SELECT
# MAGIC #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC #prm_mes_fechamento AS cd_mes_fechamento,
# MAGIC #prm_data_corte AS  AS dt_fechamento,
# MAGIC R.cd_entidade_regional,
# MAGIC R.cd_centro_responsabilidade,
# MAGIC R.cd_conta_contabil,
# MAGIC 
# MAGIC R.vl_desptot_educ_basica_contnd,
# MAGIC R.vl_desptot_vira_vida_educ_basica_contnd_rateada,
# MAGIC R.vl_desptot_etd_gestao_educ_basica_contnd_rateada,
# MAGIC R.vl_desptot_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC R.vl_desptot_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC R.vl_desptot_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC R.vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_educ_basica_contnd,
# MAGIC R.vl_despcor_vira_vida_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_etd_gestao_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_suporte_negocio_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_indireta_gestao_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_indireta_apoio_educ_basica_contnd_rateada,
# MAGIC R.vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada,
# MAGIC 
# MAGIC -- despesa total com gratuidade
# MAGIC CASE WHEN NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_educ_basica_contnd / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_vira_vida_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_vira_vida_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_etd_gestao_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_etd_gestao_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_suporte_negocio_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_suporte_negocio_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_gestao_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_gestao_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_apoio_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_apoio_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_desenv_inst_educ_basica_contnd_gratuidade,
# MAGIC 
# MAGIC -- despesa corrente com gratuidade
# MAGIC CASE WHEN NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_educ_basica_contnd / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_vira_vida_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_vira_vida_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_etd_gestao_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_etd_gestao_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_suporte_negocio_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_gestao_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_gestao_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_apoio_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_apoio_educ_basica_contnd_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_aula, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada / H.qt_hr_aula) * H.qt_hr_aula_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_desenv_inst_educ_basica_contnd_gratuidade
# MAGIC 
# MAGIC FROM rateio_es_step05_02_despesa_rateada R 
# MAGIC LEFT JOIN rateio_es_step03_hora_aula H 
# MAGIC ON  H.cd_entidade_regional       = R.cd_entidade_regional 
# MAGIC AND H.cd_centro_responsabilidade = R.cd_centro_responsabilidade;
# MAGIC ```

# COMMAND ----------

"""
This is the final object
This is a small table, in test fase it has only ~29k records and ~3M in mem cache. Keep in 1 partition.
"""

fta_gestao_financeira_educacao_basica_contnd = rateio_es_step05_02_despesa_rateada\
.join(rateio_es_step03_hora_aula\
      .select("qt_hr_aula", "qt_hr_aula_gratuidade_reg", "cd_entidade_regional", "cd_centro_responsabilidade"),
      ["cd_entidade_regional", "cd_centro_responsabilidade"],
      "left")\
.fillna(0, subset=["vl_desptot_educ_basica_contnd",
                   "vl_desptot_vira_vida_educ_basica_contnd_rateada",
                   "vl_desptot_etd_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada",
                   "vl_despcor_educ_basica_contnd",
                   "vl_despcor_vira_vida_educ_basica_contnd_rateada",
                   "vl_despcor_etd_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_suporte_negocio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_gestao_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_apoio_educ_basica_contnd_rateada",
                   "vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada",
                   "vl_despcor_educ_basica_contnd",
                   "qt_hr_aula",
                   "qt_hr_aula_gratuidade_reg"])\
.withColumn("vl_desptot_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_educ_basica_contnd") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_vira_vida_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_vira_vida_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_etd_gestao_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_etd_gestao_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_suporte_negocio_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_suporte_negocio_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_gestao_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_indireta_gestao_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_apoio_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_indireta_apoio_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_desenv_inst_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_educ_basica_contnd") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_vira_vida_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_etd_gestao_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_suporte_negocio_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_gestao_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_apoio_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_desenv_inst_educ_basica_contnd_gratuidade",
           f.when(f.col("qt_hr_aula") > 0, 
                  (f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada") / f.col("qt_hr_aula")) * f.col("qt_hr_aula_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.drop("cd_entidade_nacional", "sg_entidade_regional", "qt_hr_aula", "qt_hr_aula_gratuidade_reg")\
.coalesce(1)\
.cache()

# Activate cache
fta_gestao_financeira_educacao_basica_contnd.count()

# COMMAND ----------

"""
Source rateio_es_step05_02_despesa_rateada and rateio_es_step03_hora_aula are not needed anymore.
These object can be removed from cache.
"""
rateio_es_step05_02_despesa_rateada = rateio_es_step05_02_despesa_rateada.unpersist()
rateio_es_step03_hora_aula = rateio_es_step03_hora_aula.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO 1 - OK

sg_entidade_regional = 'SESI-SP'
cd_entidade_regional = 1117258

vl_despcor_educ_basica_contnd - 5014367.54
vl_despcor_etd_gestao_educ_basica_contnd_rateada - 2600107.82677531
vl_despcor_vira_vida_educ_basica_contnd_rateada - 43.3242530935
vl_despcor_suporte_negocio_educ_basica_contnd_rateada - 1244730.53233257
vl_despcor_indireta_gestao_apoio_educ_basica_contnd_rateada - 372548.57597973
vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada - 90454.6666277782

display(fta_gestao_financeira_educacao_basica_contnd \
.filter((f.col("cd_entidade_regional") == 1117258) &\
        (f.col("cd_conta_contabil").startswith("31010103")) &\
        (f.col("cd_centro_responsabilidade")==303010301))\
.withColumn("cd_conta_contabil", f.substring(f.col("cd_conta_contabil"), 1, 8))\
.groupBy("cd_conta_contabil")\
.agg(f.sum("vl_despcor_educ_basica_contnd").alias("vl_despcor_educ_basica_contnd"), 
     f.sum("vl_despcor_etd_gestao_educ_basica_contnd_rateada").alias("vl_despcor_etd_gestao_educ_basica_contnd_rateada"),
     f.sum("vl_despcor_vira_vida_educ_basica_contnd_rateada").alias("vl_despcor_vira_vida_educ_basica_contnd_rateada"),
     f.sum("vl_despcor_suporte_negocio_educ_basica_contnd_rateada").alias("vl_despcor_suporte_negocio_educ_basica_contnd_rateada"),
     f.sum(f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") +\
           f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada")).alias("vl_despcor_indireta_gestao_apoio_educ_basica_contnd_rateada"),
     f.sum("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada").alias("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada"))\
.withColumn("ds_cr", f.lit('Ensino Médio')))

"""


# COMMAND ----------

"""
VALIDAÇÃO 2 - OK

var_hcr_src = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], "/corporativo/dim_hierarquia_centro_responsabilidade") 
var_hcr_columns = ["cd_centro_responsabilidade", "ds_centro_responsabilidade"]
df_dim_her = spark.read.parquet(var_hcr_src)\
.select(*var_hcr_columns)

display(fta_gestao_financeira_educacao_basica_contnd \
.filter((f.col("cd_entidade_regional") == 1117258) &\
        (f.col("cd_centro_responsabilidade").isin("303010202", "303010301", "303010408", "303020101")))\
.join(df_dim_her, ["cd_centro_responsabilidade"], "inner")\
.groupBy("ds_centro_responsabilidade", "cd_centro_responsabilidade")\
.agg(f.sum(f.col("vl_despcor_educ_basica_contnd") + \
           f.col("vl_despcor_etd_gestao_educ_basica_contnd_rateada") \
           + f.col("vl_despcor_vira_vida_educ_basica_contnd_rateada")),
     f.sum("vl_despcor_suporte_negocio_educ_basica_contnd_rateada"),
     f.sum(f.col("vl_despcor_indireta_gestao_educ_basica_contnd_rateada") + \
           f.col("vl_despcor_indireta_apoio_educ_basica_contnd_rateada") + \
           f.col("vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada"))) \
.orderBy("ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 2 - OK

df_dim_her.createOrReplaceTempView("dim_hierarquia_centro_responsabilidade")

display(spark.sql("select \
                  b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade, \
                  SUM(vl_despcor_educ_basica_contnd) +\
                  SUM(vl_despcor_etd_gestao_educ_basica_contnd_rateada) +\
                  SUM(vl_despcor_vira_vida_educ_basica_contnd_rateada), \
                  SUM(vl_despcor_suporte_negocio_educ_basica_contnd_rateada), \
                  SUM(vl_despcor_indireta_gestao_educ_basica_contnd_rateada) + \
                  SUM(vl_despcor_indireta_apoio_educ_basica_contnd_rateada) + \
                  SUM(vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada) \
                  from fta_gestao_financeira_educacao_basica_contnd a \
                  inner join dim_hierarquia_centro_responsabilidade b \
                  on a.cd_centro_responsabilidade = b.cd_centro_responsabilidade \
                  where a.sg_entidade_regional = 'SESI-SP' \
                  and a.cd_centro_responsabilidade in (303010202, 303010301, 303010408, 303020101) \
                  group by b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade \
                  order by b.ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 3 - OK

display(fta_gestao_financeira_educacao_basica_contnd \
.filter((f.col("cd_entidade_regional") == 1117258) &\
        (f.col("cd_centro_responsabilidade").isin("303010202", "303010301", "303010408", "303020101")))\
.join(df_dim_her, ["cd_centro_responsabilidade"], "inner")\
.groupBy("ds_centro_responsabilidade", "cd_centro_responsabilidade")\
.agg(f.sum(f.col("vl_desptot_educ_basica_contnd") + \
           f.col("vl_desptot_etd_gestao_educ_basica_contnd_rateada") \
           + f.col("vl_desptot_vira_vida_educ_basica_contnd_rateada")),
     f.sum("vl_desptot_suporte_negocio_educ_basica_contnd_rateada"),
     f.sum(f.col("vl_desptot_indireta_gestao_educ_basica_contnd_rateada") + \
           f.col("vl_desptot_indireta_apoio_educ_basica_contnd_rateada") + \
           f.col("vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada"))) \
.orderBy("ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 3 - OK

display(spark.sql("select \
                  b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade, \
                  SUM(vl_desptot_educ_basica_contnd) +\
                  SUM(vl_desptot_etd_gestao_educ_basica_contnd_rateada) +\
                  SUM(vl_desptot_vira_vida_educ_basica_contnd_rateada), \
                  SUM(vl_desptot_suporte_negocio_educ_basica_contnd_rateada), \
                  SUM(vl_desptot_indireta_gestao_educ_basica_contnd_rateada) + \
                  SUM(vl_desptot_indireta_apoio_educ_basica_contnd_rateada) + \
                  SUM(vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada) \
                  from fta_gestao_financeira_educacao_basica_contnd a \
                  inner join dim_hierarquia_centro_responsabilidade b \
                  on a.cd_centro_responsabilidade = b.cd_centro_responsabilidade \
                  where a.sg_entidade_regional = 'SESI-SP' \
                  and a.cd_centro_responsabilidade in (303010202, 303010301, 303010408, 303020101) \
                  group by b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade \
                  order by b.ds_centro_responsabilidade"))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz

# COMMAND ----------

fta_gestao_financeira_educacao_basica_contnd = tcf.add_control_fields(fta_gestao_financeira_educacao_basica_contnd, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjusting schema

# COMMAND ----------

var_column_type_map = {"cd_entidade_regional": "int",
                       "cd_centro_responsabilidade": "string",
                       "cd_conta_contabil": "string",
                       "vl_desptot_vira_vida_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_etd_gestao_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_suporte_negocio_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_gestao_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_apoio_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_desenv_inst_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_vira_vida_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_etd_gestao_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_suporte_negocio_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_apoio_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_desenv_inst_educ_basica_contnd_rateada": "decimal(18,6)",
                       "vl_desptot_educ_basica_contnd": "decimal(18,6)",
                       "vl_despcor_educ_basica_contnd": "decimal(18,6)",
                       "vl_desptot_vira_vida_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_etd_gestao_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_suporte_negocio_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_gestao_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_apoio_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_desenv_inst_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_vira_vida_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_etd_gestao_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_suporte_negocio_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_apoio_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_desenv_inst_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_desptot_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "vl_despcor_educ_basica_contnd_gratuidade": "decimal(18,6)",
                       "cd_ano_fechamento": "int",
                       "cd_mes_fechamento": "int",
                       "dt_fechamento": "date"}

for c in var_column_type_map:
  fta_gestao_financeira_educacao_basica_contnd = fta_gestao_financeira_educacao_basica_contnd.withColumn(c, fta_gestao_financeira_educacao_basica_contnd[c].cast(var_column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

fta_gestao_financeira_educacao_basica_contnd.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

"""
Source fta_gestao_financeira_ensino_profissional is not needed anymore.
This object can be removed from cache.
"""
fta_gestao_financeira_educacao_basica_contnd = fta_gestao_financeira_educacao_basica_contnd.unpersist()

# COMMAND ----------

