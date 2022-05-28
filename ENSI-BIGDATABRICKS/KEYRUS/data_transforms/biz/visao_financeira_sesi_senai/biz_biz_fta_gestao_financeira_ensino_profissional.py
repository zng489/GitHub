# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	biz_biz_fta_gestao_financeira_ensino_profissional
# MAGIC Tabela/Arquivo Origem	"/trs/evt/orcamento_nacional_realizado
# MAGIC /biz/orcamento/fta_despesa_rateada_negocio
# MAGIC /biz/orcamento/fte_producao_ensino_profissional"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas de Negócios rateadas por hora escolar para Ensino Profissional - SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento e  #prm_mes_fechamento informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento e  #prm_mes_fechamento pela data atual do processamento
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
var_tables = {"origins":["/evt/orcamento_nacional_realizado", "/mtd/corp/entidade_regional", "/orcamento/fta_despesa_rateada_negocio", "/producao/fte_producao_ensino_profissional"],
  "destination":"/orcamento/fta_gestao_financeira_ensino_profissional", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_ensino_profissional"}
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
var_src_drn, var_src_pep =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], t) for t in var_tables["origins"][-2:]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
var_src_onr, var_src_reg, var_src_drn, var_src_pep, var_sink

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
# MAGIC -- STEP01: Obtem despesa indireta orçamento SENAI a ser rateada o ano/mês e data de fechamento informados por parâmetro
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_ep_step01_despesa_indireta;
# MAGIC CREATE TABLE rateio_ep_step01_despesa_indireta AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_conta_contabil,
# MAGIC 
# MAGIC -- despesa total
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '303070108%' THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_olimpiada,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '30310%' OR o.cd_centro_responsabilidade LIKE '30311%'  
# MAGIC                                                              THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_etd_gestao_educacao,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '307%'       THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_suporte_negocio,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '1%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_gestao,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '2%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_desenv_inst,
# MAGIC SUM(CASE WHEN o.cd_centro_responsabilidade LIKE '4%'         THEN o.vl_lancamento ELSE 0 END)  AS vl_desptot_indireta_apoio,
# MAGIC 
# MAGIC --despesa corrente
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' AND o.cd_centro_responsabilidade LIKE '303070108%' THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_olimpiada,
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
# MAGIC --AND   e.cd_entidade_nacional = 3 AND o.cd_conta_contabil NOT LIKE '31010623%' -- SENAI, sem reversão   
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
This object will be reused a lot. It is really small: < 30 records. 
Cache it and keep in 1 partition
"""

var_e_columns = ["cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_entidade_regional_erp_oltp"]

e = spark.read.parquet(var_src_reg)\
.select(*var_e_columns)\
.filter(f.col("cd_entidade_nacional").isin(3))\
.coalesce(1)\
.cache()

# Activate cache
e.count()

# COMMAND ----------

"""
I'm testing for the first load and the object is really small: ~2,7k records. Can be kept in 1 partition
"""
rateio_ep_step01_despesa_indireta = o\
.filter(~((f.col("cd_conta_contabil").startswith("31020101")) |\
          (f.col("cd_conta_contabil").startswith("31020102")) |\
          (f.col("cd_conta_contabil").startswith("31020105")) |\
          (f.col("cd_conta_contabil").startswith("31011001"))))\
.join(e, ["cd_entidade_regional_erp_oltp"], "inner")\
.drop(*["cd_entidade_regional_erp_oltp"])\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_conta_contabil")\
.agg(f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("303070108"), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_desptot_olimpiada"),
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
                  (f.col("cd_centro_responsabilidade").startswith("303070108")), \
                  f.col("vl_lancamento")) \
            .otherwise(f.lit(0))).alias("vl_despcor_olimpiada"),
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
.fillna(0, subset=["vl_desptot_olimpiada",
                   "vl_desptot_etd_gestao_educacao",
                   "vl_desptot_suporte_negocio",
                   "vl_desptot_indireta_gestao",
                   "vl_desptot_indireta_desenv_inst",
                   "vl_desptot_indireta_apoio",
                   "vl_despcor_olimpiada",
                   "vl_despcor_etd_gestao_educacao",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_indireta_gestao",
                   "vl_despcor_indireta_desenv_inst",
                   "vl_despcor_indireta_apoio"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_ep_step01_despesa_indireta.count() == spark.sql("select count(*) from rateio_ep_step01_despesa_indireta").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 02
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP02: Obtem dos drivers de rateio a serem aplicados sobre negócio SENAI o ano/mês fechamento informados por parâmetro
# MAGIC -- Manutenção em 13/11/2020
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_ep_step02_despesa_negocio;
# MAGIC CREATE TABLE rateio_ep_step02_despesa_negocio AS
# MAGIC SELECT
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC -- em 13/11/2020 incluído ELSE para ficar com 0 no lugar de nulo
# MAGIC --despesa total
# MAGIC SUM(vl_despesa_total)                                       AS vl_despesa_total,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '30303%'
# MAGIC          OR   cd_centro_responsabilidade     LIKE '30304%'
# MAGIC          THEN vl_despesa_total ELSE 0 END)                  AS vl_desptot_educacao_profissional,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '303%'
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30303%'
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30304%'
# MAGIC          THEN vl_despesa_total ELSE 0 END)                  AS vl_desptot_educacao_outros,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '302%'
# MAGIC         THEN vl_despesa_total ELSE 0 END)                   AS vl_desptot_tecnol_inov,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '3%'
# MAGIC          AND substring(cd_centro_responsabilidade, 1, 3)
# MAGIC              NOT IN ('303','302')
# MAGIC          THEN vl_despesa_total ELSE 0 END)                  AS vl_desotot_outro_servico,
# MAGIC -- despesa corrente
# MAGIC SUM(vl_despesa_corrente)                                    AS vl_despesa_corrente,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '30303%'
# MAGIC          OR   cd_centro_responsabilidade     LIKE '30304%'
# MAGIC          THEN vl_despesa_corrente ELSE 0 END)               AS vl_despcor_educacao_profissional,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '303%'
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30303%'
# MAGIC          AND  cd_centro_responsabilidade NOT LIKE '30304%'
# MAGIC          THEN vl_despesa_corrente ELSE 0 END)               AS vl_despcor_educacao_outros,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '302%'
# MAGIC          THEN vl_despesa_corrente ELSE 0 END)            AS vl_despcor_tecnol_inov,
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade     LIKE '3%'
# MAGIC          AND substring(cd_centro_responsabilidade, 1, 3)
# MAGIC              NOT IN ('303','302')
# MAGIC          THEN vl_despesa_corrente ELSE 0 END)               AS vl_desocor_outro_servico
# MAGIC FROM
# MAGIC (  -- leitura só para somar as colunas em um único campo vl_despesa_corrente, e trabalhar com ele
# MAGIC    SELECT
# MAGIC    e.cd_entidade_nacional,
# MAGIC    e.sg_entidade_regional,
# MAGIC    o.cd_entidade_regional,
# MAGIC    o.cd_centro_responsabilidade,
# MAGIC     -- despesa total
# MAGIC    (vl_despesa_total + vl_desptot_vira_vida_rateada + vl_desptot_olimpiada_rateada + vl_desptot_etd_gestao_educacao_rateada +
# MAGIC     vl_desptot_etd_gestao_outros_rateada + vl_desptot_suporte_negocio_rateada + vl_desptot_indireta_gestao_rateada +
# MAGIC     vl_desptot_indireta_desenv_institucional_rateada + vl_desptot_indireta_apoio_rateada
# MAGIC    ) AS vl_despesa_total,
# MAGIC     -- despesa corrente
# MAGIC    (vl_despesa_corrente + vl_despcor_vira_vida_rateada + vl_despcor_olimpiada_rateada + vl_despcor_etd_gestao_educacao_rateada +
# MAGIC     vl_despcor_etd_gestao_outros_rateada + vl_despcor_suporte_negocio_rateada + vl_despcor_indireta_gestao_rateada +
# MAGIC     vl_despcor_indireta_desenv_institucional_rateada + vl_despcor_indireta_apoio_rateada
# MAGIC    ) AS vl_despesa_corrente
# MAGIC    FROM fta_despesa_rateada_negocio o
# MAGIC    INNER JOIN entidade_regional e
# MAGIC    ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC    WHERE  e.cd_entidade_nacional = 3 -- SENAI
# MAGIC    -- AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC    -- filtro para teste fechamento jun 2020
# MAGIC    AND   cd_ano_fechamento = 2020 AND cd_mes_fechamento = 6
# MAGIC ) o
# MAGIC GROUP BY
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional;
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only 29 record. Keep in 1 partition
"""

var_drn_columns = ["cd_entidade_regional", "cd_centro_responsabilidade", "vl_despesa_total", "vl_desptot_vira_vida_rateada", "vl_desptot_olimpiada_rateada", "vl_desptot_etd_gestao_educacao_rateada", "vl_desptot_etd_gestao_outros_rateada", "vl_desptot_suporte_negocio_rateada", "vl_desptot_indireta_gestao_rateada", "vl_desptot_indireta_desenv_institucional_rateada", "vl_desptot_indireta_apoio_rateada", "vl_despesa_corrente", "vl_despcor_vira_vida_rateada", "vl_despcor_olimpiada_rateada", "vl_despcor_etd_gestao_educacao_rateada", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_suporte_negocio_rateada", "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada", "cd_ano_fechamento", "cd_mes_fechamento"]

rateio_ep_step02_despesa_negocio = spark.read.parquet(var_src_drn)\
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
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30303")) | \
                  (f.col("cd_centro_responsabilidade").startswith("30304")), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_educacao_profissional"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30304")), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_educacao_outros"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("302")), \
                  f.col("vl_despesa_total")) \
           .otherwise(f.lit(0))).alias("vl_desptot_tecnol_inov"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("3")) & \
                  ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','302')), \
                  f.col("vl_despesa_total")) \
            .otherwise(f.lit(0))).alias("vl_desptot_outro_servico"),
     f.sum("vl_despesa_corrente").alias("vl_despesa_corrente"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30303")) | \
                  (f.col("cd_centro_responsabilidade").startswith("30304")), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_educacao_profissional"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("30304")), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_educacao_outros"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("302")), \
                  f.col("vl_despesa_corrente")) \
           .otherwise(f.lit(0))).alias("vl_despcor_tecnol_inov"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("3")) & \
                  ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','302')), \
                  f.col("vl_despesa_corrente")) \
            .otherwise(f.lit(0))).alias("vl_despcor_outro_servico"))\
.fillna(0, subset=["vl_despesa_total",
                   "vl_desptot_educacao_profissional",
                   "vl_desptot_educacao_outros",
                   "vl_desptot_tecnol_inov",
                   "vl_desptot_outro_servico",
                   "vl_despesa_corrente",
                   "vl_despcor_educacao_profissional",
                   "vl_despcor_educacao_outros",
                   "vl_despcor_tecnol_inov",
                   "vl_despcor_outro_servico"])\
.coalesce(1)


# COMMAND ----------

#assert  rateio_ep_step02_despesa_negocio.count() == spark.sql("select count(*) from rateio_ep_step02_despesa_negocio").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 03
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP03: Obtem os drivers de rateio a de Horas de Ensino Profissional para o ano/mês fechamento informados por parâmetro
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_ep_step03_hora_escolar;
# MAGIC CREATE TABLE rateio_ep_step03_hora_escolar AS
# MAGIC SELECT
# MAGIC o.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.qt_matricula_ensino_prof,
# MAGIC o.qt_matricula_ensino_prof_gratuidade_reg,
# MAGIC o.qt_hr_escolar,
# MAGIC o.qt_hr_escolar_gratuidade_reg,
# MAGIC SUM(o.qt_hr_escolar)         OVER(PARTITION BY o.cd_entidade_regional) AS qt_hr_escolar_total_dr,
# MAGIC o.qt_hr_escolar_tecnica,
# MAGIC SUM(o.qt_hr_escolar_tecnica) OVER(PARTITION BY o.cd_entidade_regional) AS qt_hr_escolar_tecnica_total_dr
# MAGIC FROM 
# MAGIC (     -- agrega por cd_entidade_regional e cd_centro_responsabilidade no fechamento
# MAGIC       SELECT 
# MAGIC       cd_entidade_regional,
# MAGIC       cd_centro_responsabilidade,
# MAGIC       SUM(qt_matricula_ensino_prof)                AS qt_matricula_ensino_prof,
# MAGIC       SUM(qt_matricula_ensino_prof_gratuidade_reg) AS qt_matricula_ensino_prof_gratuidade_reg,
# MAGIC       SUM(qt_hr_escolar)                           AS qt_hr_escolar,
# MAGIC       SUM(qt_hr_escolar_gratuidade_reg)            AS qt_hr_escolar_gratuidade_reg, 
# MAGIC       
# MAGIC       -- Somente EDUCACAO PROFISSIONAL E TECNOLOGICA, exceto Eucação para o trabalho
# MAGIC       SUM(CASE WHEN cd_centro_responsabilidade  LIKE '30303%'
# MAGIC           AND cd_centro_responsabilidade    NOT LIKE '3030301%' 
# MAGIC           THEN qt_hr_escolar ELSE 0 END
# MAGIC           ) AS qt_hr_escolar_tecnica           
# MAGIC            
# MAGIC       FROM fte_producao_ensino_profissional o
# MAGIC       
# MAGIC       WHERE
# MAGIC       cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento = #prm_mes_fechamento
# MAGIC       AND NVL(cd_centro_responsabilidade, '-98') <> '-98' -- em 12/11/2020, nulo não trazer CR nulo
# MAGIC       GROUP BY 
# MAGIC       cd_entidade_regional,
# MAGIC       cd_centro_responsabilidade
# MAGIC ) o
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
This is a really small table, in test fase it has 309 records. Keep in 1 partition.
"""

var_pep_columns = ["cd_entidade_regional", "cd_centro_responsabilidade", "qt_matricula_ensino_prof", "qt_matricula_ensino_prof_gratuidade_reg", "qt_hr_escolar", "qt_hr_escolar_gratuidade_reg", "cd_ano_fechamento", "cd_mes_fechamento"]

rateio_ep_step03_hora_escolar = spark.read.parquet(var_src_pep)\
.select(*var_pep_columns)\
.filter((f.col("cd_ano_fechamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.col("cd_mes_fechamento") == f.lit(var_parameters["prm_mes_fechamento"]).cast("int")) &\
        (f.coalesce(f.col("cd_centro_responsabilidade"), f.lit("-98")) != '-98'))\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("qt_matricula_ensino_prof").alias("qt_matricula_ensino_prof"),
     f.sum("qt_matricula_ensino_prof_gratuidade_reg").alias("qt_matricula_ensino_prof_gratuidade_reg"),
     f.sum("qt_hr_escolar").alias("qt_hr_escolar"),
     f.sum("qt_hr_escolar_gratuidade_reg").alias("qt_hr_escolar_gratuidade_reg"),
     f.sum(f.when((f.col("cd_centro_responsabilidade").startswith("30303")) & \
                  ~(f.col("cd_centro_responsabilidade").startswith("3030301")), \
                  f.col("qt_hr_escolar")) \
            .otherwise(f.lit(0))).alias("qt_hr_escolar_tecnica"))\
.withColumn("qt_hr_escolar_total_dr", f.sum("qt_hr_escolar").over(window))\
.withColumn("qt_hr_escolar_tecnica_total_dr", f.sum("qt_hr_escolar_tecnica").over(window))\
.fillna(0, subset=["qt_matricula_ensino_prof",
                   "qt_matricula_ensino_prof_gratuidade_reg",
                   "qt_hr_escolar",
                   "qt_hr_escolar_gratuidade_reg",
                   "qt_hr_escolar_tecnica",
                   "qt_hr_escolar_total_dr",
                   "qt_hr_escolar_tecnica_total_dr"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_ep_step03_hora_escolar.count()

# COMMAND ----------

#assert  rateio_ep_step03_hora_escolar.count() == spark.sql("select count(*) from rateio_ep_step03_hora_escolar").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 04
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP04: Calcula as despesas indiretas educação profissional rateadas
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE   rateio_ep_step04_01_despesa_indireta_x_driver_rateio;
# MAGIC CREATE TABLE rateio_ep_step04_01_despesa_indireta_x_driver_rateio AS
# MAGIC SELECT DISTINCT
# MAGIC I.cd_entidade_nacional,
# MAGIC I.sg_entidade_regional,
# MAGIC H.cd_entidade_regional,
# MAGIC H.cd_centro_responsabilidade,
# MAGIC I.cd_conta_contabil,
# MAGIC 
# MAGIC -- hora escolar
# MAGIC H.qt_hr_escolar,
# MAGIC H.qt_hr_escolar_total_dr,
# MAGIC H.qt_hr_escolar_tecnica,
# MAGIC H.qt_hr_escolar_tecnica_total_dr,
# MAGIC 
# MAGIC -- despesa negócio
# MAGIC --    desposa total
# MAGIC P.vl_desptot_educacao_profissional,
# MAGIC P.vl_desptot_educacao_profissional + P.vl_desptot_educacao_outros AS vl_desptot_educacao,
# MAGIC P.vl_despesa_total,
# MAGIC --    despesa corrente
# MAGIC P.vl_despcor_educacao_profissional,
# MAGIC P.vl_despcor_educacao_profissional + P.vl_despcor_educacao_outros AS vl_despcor_educacao,
# MAGIC P.vl_despesa_corrente,
# MAGIC 
# MAGIC -- Despesas indiretas a ratear
# MAGIC --    desposa total
# MAGIC I.vl_desptot_olimpiada,
# MAGIC I.vl_desptot_etd_gestao_educacao,
# MAGIC I.vl_desptot_suporte_negocio,
# MAGIC I.vl_desptot_indireta_gestao,
# MAGIC I.vl_desptot_indireta_apoio,
# MAGIC I.vl_desptot_indireta_desenv_inst,
# MAGIC 
# MAGIC --    despesa corrente
# MAGIC I.vl_despcor_olimpiada,
# MAGIC I.vl_despcor_etd_gestao_educacao,
# MAGIC I.vl_despcor_suporte_negocio,
# MAGIC I.vl_despcor_indireta_gestao,
# MAGIC I.vl_despcor_indireta_apoio,
# MAGIC I.vl_despcor_indireta_desenv_inst
# MAGIC 
# MAGIC FROM       rateio_ep_step01_despesa_indireta I
# MAGIC INNER JOIN rateio_ep_step02_despesa_negocio  P ON I.cd_entidade_regional = P.cd_entidade_regional
# MAGIC INNER JOIN rateio_ep_step03_hora_escolar     H ON I.cd_entidade_regional = H.cd_entidade_regional;
# MAGIC 
# MAGIC ----------------------------------------------------------------
# MAGIC DROP TABLE   rateio_ep_step04_02_despesa_indireta_rateada;
# MAGIC CREATE TABLE rateio_ep_step04_02_despesa_indireta_rateada AS
# MAGIC SELECT
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC cd_conta_contabil,
# MAGIC 
# MAGIC -- despesa total
# MAGIC vl_desptot_olimpiada *
# MAGIC (qt_hr_escolar_tecnica/qt_hr_escolar_tecnica_total_dr)                                        AS vl_desptot_olimpiada_ensino_prof_rateada,
# MAGIC vl_desptot_etd_gestao_educacao * 
# MAGIC (vl_desptot_educacao_profissional/vl_desptot_educacao)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_desptot_etd_gestao_ensino_prof_rateada,
# MAGIC vl_desptot_suporte_negocio * 
# MAGIC (vl_desptot_educacao_profissional/vl_despesa_total)*(qt_hr_escolar/qt_hr_escolar_total_dr)    AS vl_desptot_suporte_negocio_ensino_prof_rateada,
# MAGIC vl_desptot_indireta_gestao * 
# MAGIC (vl_desptot_educacao_profissional/vl_despesa_total)*(qt_hr_escolar/qt_hr_escolar_total_dr)    AS vl_desptot_indireta_gestao_ensino_prof_rateada,
# MAGIC vl_desptot_indireta_apoio * 
# MAGIC (vl_desptot_educacao_profissional/vl_despesa_total)*(qt_hr_escolar/qt_hr_escolar_total_dr)    AS vl_desptot_indireta_apoio_ensino_prof_rateada,
# MAGIC vl_desptot_indireta_desenv_inst * 
# MAGIC (vl_desptot_educacao_profissional/vl_despesa_total)*(qt_hr_escolar/qt_hr_escolar_total_dr)    AS vl_desptot_indireta_desenv_inst_ensino_prof_rateada,
# MAGIC 
# MAGIC -- despesa corrente
# MAGIC vl_despcor_olimpiada *
# MAGIC (qt_hr_escolar_tecnica/qt_hr_escolar_tecnica_total_dr)                                        AS vl_despcor_olimpiada_ensino_prof_rateada,
# MAGIC vl_despcor_etd_gestao_educacao * 
# MAGIC (vl_despcor_educacao_profissional/vl_despcor_educacao)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_despcor_etd_gestao_ensino_prof_rateada,
# MAGIC vl_despcor_suporte_negocio * 
# MAGIC (vl_despcor_educacao_profissional/vl_despesa_corrente)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_despcor_suporte_negocio_ensino_prof_rateada,
# MAGIC vl_despcor_indireta_gestao * 
# MAGIC (vl_despcor_educacao_profissional/vl_despesa_corrente)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_despcor_indireta_gestao_ensino_prof_rateada,
# MAGIC vl_despcor_indireta_apoio * 
# MAGIC (vl_despcor_educacao_profissional/vl_despesa_corrente)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_despcor_indireta_apoio_ensino_prof_rateada,
# MAGIC vl_despcor_indireta_desenv_inst * 
# MAGIC (vl_despcor_educacao_profissional/vl_despesa_corrente)*(qt_hr_escolar/qt_hr_escolar_total_dr) AS vl_despcor_indireta_desenv_inst_ensino_prof_rateada
# MAGIC FROM rateio_ep_step04_01_despesa_indireta_x_driver_rateio;
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only ~1,3k records and ~38kb in mem cache. Keep in 1 partition.

"""

rateio_ep_step04_01_despesa_indireta_x_driver_rateio = rateio_ep_step01_despesa_indireta\
.join(rateio_ep_step02_despesa_negocio.drop("cd_entidade_nacional", "sg_entidade_regional", "vl_desptot_tecnol_inov", "vl_desptot_outro_servico", "vl_despcor_tecnol_inov", "vl_despcor_outro_servico"), 
      ["cd_entidade_regional"], "inner")\
.join(rateio_ep_step03_hora_escolar.drop("qt_matricula_ensino_prof", "qt_matricula_ensino_prof_gratuidade_reg", "qt_hr_escolar_gratuidade_reg"), 
      ["cd_entidade_regional"], "inner")\
.withColumn("vl_desptot_educacao", f.col("vl_desptot_educacao_profissional") + f.col("vl_desptot_educacao_outros"))\
.withColumn("vl_despcor_educacao", f.col("vl_despcor_educacao_profissional") + f.col("vl_despcor_educacao_outros"))\
.drop("vl_desptot_educacao_outros", "vl_despcor_educacao_outros")\
.fillna(0, subset=["qt_hr_escolar",
                   "qt_hr_escolar_total_dr",
                   "qt_hr_escolar_tecnica",
                   "qt_hr_escolar_tecnica_total_dr",
                   "vl_desptot_educacao_profissional",
                   "vl_desptot_educacao",
                   "vl_despesa_total",
                   "vl_despcor_educacao_profissional",
                   "vl_despcor_educacao",
                   "vl_despesa_corrente",
                   "vl_desptot_olimpiada",
                   "vl_desptot_etd_gestao_educacao",
                   "vl_desptot_suporte_negocio",
                   "vl_desptot_indireta_gestao",
                   "vl_desptot_indireta_apoio",
                   "vl_desptot_indireta_desenv_inst",
                   "vl_despcor_olimpiada",
                   "vl_despcor_etd_gestao_educacao",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_indireta_gestao",
                   "vl_despcor_indireta_apoio",
                   "vl_despcor_indireta_desenv_inst"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_ep_step04_01_despesa_indireta_x_driver_rateio.count() == spark.sql("select count(*) from rateio_ep_step04_01_despesa_indireta_x_driver_rateio").collect()[0][0]

# COMMAND ----------

"""
This is a small table, in test fase it has only 872 records. Keep in 1 partition.
"""

rateio_ep_step04_02_despesa_indireta_rateada = rateio_ep_step04_01_despesa_indireta_x_driver_rateio\
.withColumn("vl_desptot_olimpiada_ensino_prof_rateada",
            ((f.col("qt_hr_escolar_tecnica") / f.col("qt_hr_escolar_tecnica_total_dr")) *\
             f.col("vl_desptot_olimpiada")))\
.withColumn("vl_desptot_etd_gestao_ensino_prof_rateada",
            ((f.col("vl_desptot_educacao_profissional") / f.col("vl_desptot_educacao")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_desptot_etd_gestao_educacao")))\
.withColumn("vl_desptot_suporte_negocio_ensino_prof_rateada",
            ((f.col("vl_desptot_educacao_profissional") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_desptot_suporte_negocio")))\
.withColumn("vl_desptot_indireta_gestao_ensino_prof_rateada",
            ((f.col("vl_desptot_educacao_profissional") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_desptot_indireta_gestao")))\
.withColumn("vl_desptot_indireta_apoio_ensino_prof_rateada",
            ((f.col("vl_desptot_educacao_profissional") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_desptot_indireta_apoio")))\
.withColumn("vl_desptot_indireta_desenv_inst_ensino_prof_rateada",
            ((f.col("vl_desptot_educacao_profissional") / f.col("vl_despesa_total")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_desptot_indireta_desenv_inst")))\
.withColumn("vl_despcor_olimpiada_ensino_prof_rateada",
            ((f.col("qt_hr_escolar_tecnica") / f.col("qt_hr_escolar_tecnica_total_dr")) *\
             f.col("vl_despcor_olimpiada")))\
.withColumn("vl_despcor_etd_gestao_ensino_prof_rateada",
            ((f.col("vl_despcor_educacao_profissional") / f.col("vl_despcor_educacao")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_despcor_etd_gestao_educacao")))\
.withColumn("vl_despcor_suporte_negocio_ensino_prof_rateada",
            ((f.col("vl_despcor_educacao_profissional") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_despcor_suporte_negocio")))\
.withColumn("vl_despcor_indireta_gestao_ensino_prof_rateada",
            ((f.col("vl_despcor_educacao_profissional") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_despcor_indireta_gestao")))\
.withColumn("vl_despcor_indireta_apoio_ensino_prof_rateada",
            ((f.col("vl_despcor_educacao_profissional") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_despcor_indireta_apoio")))\
.withColumn("vl_despcor_indireta_desenv_inst_ensino_prof_rateada",
            ((f.col("vl_despcor_educacao_profissional") / f.col("vl_despesa_corrente")) *\
             (f.col("qt_hr_escolar") / f.col("qt_hr_escolar_total_dr")) *\
             f.col("vl_despcor_indireta_desenv_inst")))\
.fillna(0, subset=["vl_desptot_olimpiada_ensino_prof_rateada",
                   "vl_desptot_etd_gestao_ensino_prof_rateada",
                   "vl_desptot_suporte_negocio_ensino_prof_rateada",
                   "vl_desptot_indireta_gestao_ensino_prof_rateada",
                   "vl_desptot_indireta_apoio_ensino_prof_rateada",
                   "vl_desptot_indireta_desenv_inst_ensino_prof_rateada",
                   "vl_despcor_olimpiada_ensino_prof_rateada",
                   "vl_despcor_etd_gestao_ensino_prof_rateada",
                   "vl_despcor_suporte_negocio_ensino_prof_rateada",
                   "vl_despcor_indireta_gestao_ensino_prof_rateada",
                   "vl_despcor_indireta_apoio_ensino_prof_rateada",
                   "vl_despcor_indireta_desenv_inst_ensino_prof_rateada"])\
.drop("qt_hr_escolar", "qt_hr_escolar_total_dr", "qt_hr_escolar_tecnica", "qt_hr_escolar_tecnica_total_dr", "vl_desptot_educacao_profissional", "vl_desptot_educacao", "vl_despesa_total", "vl_despcor_educacao_profissional", "vl_despcor_educacao", "vl_despesa_corrente", "vl_desptot_olimpiada", "vl_desptot_etd_gestao_educacao", "vl_desptot_suporte_negocio", "vl_desptot_indireta_gestao", "vl_desptot_indireta_apoio", "vl_desptot_indireta_desenv_inst", "vl_despcor_olimpiada", "vl_despcor_etd_gestao_educacao", "vl_despcor_suporte_negocio", "vl_despcor_indireta_gestao", "vl_despcor_indireta_apoio", "vl_despcor_indireta_desenv_inst")\
.coalesce(1)


# COMMAND ----------

#assert  rateio_ep_step04_02_despesa_indireta_rateada.count() == spark.sql("select count(*) from rateio_ep_step04_02_despesa_indireta_rateada").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 05
# MAGIC ```
# MAGIC %sql
# MAGIC --====================================================================================================================================
# MAGIC -- STEP05:
# MAGIC -- 01. Obtem as despesas diretas de Ensino Profissional o ano/mês e data fechamento informados por parâmetro, deixando
# MAGIC -- negativos os lançamentos de reversão
# MAGIC -- 02. Aprorpia reversões fora dos CRs 303003 no CR 303030205 também com lançamento * -1
# MAGIC -- 03. Regisra o resultado do rateio
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE rateio_ep_step05_01_despesa_direta_ensino_prof;
# MAGIC CREATE TABLE rateio_ep_step05_01_despesa_direta_ensino_prof AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.cd_conta_contabil,
# MAGIC -- em 13/11/2020 voltamos atrás a sumarizaão, retirando o CASE de  cd_conta_contabil LIKE '31010623%' com valor negativo
# MAGIC SUM(o.vl_lancamento) AS vl_desptot_ensino_prof,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' THEN o.vl_lancamento ELSE 0 END)  AS vl_despcor_ensino_prof
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE o.cd_conta_contabil LIKE '3%' -- para despesas
# MAGIC AND   e.cd_entidade_nacional = 3 -- SENAI
# MAGIC -- em 13/11/2020 volta o fitro de reversão, em 16/11/2020 somente para CR 30303%
# MAGIC AND ( (o.cd_centro_responsabilidade LIKE '30303%' AND cd_conta_contabil NOT LIKE '31010623%')
# MAGIC     OR o.cd_centro_responsabilidade LIKE '30304%') -- Ensino Profissional
# MAGIC -- AND YEAR(dt_lancamento) = #prm_ano_fechamento AND MONTH(dt_lancamento) <= #prm_mes_fechamento AND o.dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC -- filtro para teste fechamento jun 2020
# MAGIC AND   YEAR(o.dt_lancamento) = 2020 AND MONTH(o.dt_lancamento) <= 6 AND o.dt_ultima_atualizacao_oltp <= '2020-07-22'
# MAGIC 
# MAGIC 
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.cd_conta_contabil;
# MAGIC 
# MAGIC 
# MAGIC -- incluído em 06/11/2020
# MAGIC DROP TABLE   zrateio_ep_step05_02_reversao_senai_nao_30303;
# MAGIC CREATE TABLE zrateio_ep_step05_02_reversao_senai_nao_30303 AS
# MAGIC SELECT
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC '303030205' AS cd_centro_responsabilidade, -- apropria neste CR específico para corrigir
# MAGIC o.cd_conta_contabil,
# MAGIC SUM(o.vl_lancamento) * -1  AS vl_desptot_ensino_prof,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' THEN o.vl_lancamento ELSE 0 END) * -1  AS vl_despcor_ensino_prof
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC WHERE o.cd_conta_contabil LIKE '31010623%' -- reversão de contribuições industriais do SENAI
# MAGIC AND   e.cd_entidade_nacional = 3 --SENAI
# MAGIC AND   o.cd_centro_responsabilidade NOT LIKE '30303%' -- CRs <> 30303
# MAGIC AND YEAR(dt_lancamento) = #prm_ano_fechamento AND MONTH(dt_lancamento) <= #prm_mes_fechamento AND dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC GROUP BY
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_conta_contabil;
# MAGIC ------------------------------------------------------------------------------------------------
# MAGIC -- Final: Append - Substituição Parcial Ano/Mês em fta_gestao_financeira_ensino_profissional
# MAGIC -- Em 12/11/2020 último passo abaixo que gravava em biz/orcamento/fta_gestao_financeira_ensino_profissional
# MAGIC -- foi transformado em intermedirário e criado um STEP06 a frente
# MAGIC ------------------------------------------------------------------------------------------------
# MAGIC DROP TABLE   zrateio_ep_step05_03_despesa_rateada;
# MAGIC CREATE TABLE zrateio_ep_step05_03_despesa_rateada AS
# MAGIC SELECT
# MAGIC NVL(D.cd_entidade_nacional ,R.cd_entidade_nacional) AS cd_entidade_nacional,
# MAGIC NVL(D.sg_entidade_regional ,R.sg_entidade_regional) AS sg_entidade_regional,
# MAGIC NVL(D.cd_entidade_regional ,R.cd_entidade_regional) AS cd_entidade_regional,
# MAGIC NVL(D.cd_entidade_regional ,R.cd_entidade_regional) AS cd_entidade_regional,
# MAGIC NVL(D.cd_centro_responsabilidade ,R.cd_centro_responsabilidade) AS cd_centro_responsabilidade,
# MAGIC NVL(D.cd_conta_contabil ,R.cd_conta_contabil)       AS cd_conta_contabil,
# MAGIC 
# MAGIC NVL(D.vl_desptot_ensino_prof, 0)                         AS vl_desptot_ensino_prof,
# MAGIC NVL(R.vl_desptot_olimpiada_ensino_prof_rateada, 0)       AS vl_desptot_olimpiada_ensino_prof_rateada,
# MAGIC NVL(R.vl_desptot_etd_gestao_ensino_prof_rateada, 0)      AS vl_desptot_etd_gestao_ensino_prof_rateada,
# MAGIC NVL(R.vl_desptot_suporte_negocio_ensino_prof_rateada, 0) AS vl_desptot_suporte_negocio_ensino_prof_rateada,
# MAGIC NVL(R.vl_desptot_indireta_gestao_ensino_prof_rateada, 0) AS vl_desptot_indireta_gestao_ensino_prof_rateada,
# MAGIC NVL(R.vl_desptot_indireta_apoio_ensino_prof_rateada, 0)  AS vl_desptot_indireta_apoio_ensino_prof_rateada,
# MAGIC NVL(R.vl_desptot_indireta_desenv_inst_ensino_prof_rateada, 0)  AS vl_desptot_indireta_desenv_inst_ensino_prof_rateada,
# MAGIC 
# MAGIC NVL(D.vl_despcor_ensino_prof, 0)                         AS vl_despcor_ensino_prof,
# MAGIC NVL(R.vl_despcor_olimpiada_ensino_prof_rateada, 0)       AS vl_despcor_olimpiada_ensino_prof_rateada,
# MAGIC NVL(R.vl_despcor_etd_gestao_ensino_prof_rateada, 0)      AS vl_despcor_etd_gestao_ensino_prof_rateada,
# MAGIC NVL(R.vl_despcor_suporte_negocio_ensino_prof_rateada, 0) AS vl_despcor_suporte_negocio_ensino_prof_rateada,
# MAGIC NVL(R.vl_despcor_indireta_gestao_ensino_prof_rateada, 0) AS vl_despcor_indireta_gestao_ensino_prof_rateada,
# MAGIC NVL(R.vl_despcor_indireta_apoio_ensino_prof_rateada, 0)  AS vl_despcor_indireta_apoio_ensino_prof_rateada,
# MAGIC NVL(R.vl_despcor_indireta_desenv_inst_ensino_prof_rateada, 0)  AS vl_despcor_indireta_desenv_inst_ensino_prof_rateada
# MAGIC 
# MAGIC FROM rateio_ep_step04_02_despesa_indireta_rateada R 
# MAGIC FULL OUTER JOIN (SELECT * FROM zrateio_ep_step05_01_despesa_direta_ensino_prof
# MAGIC  -- merge com os casos de reversão apropriados no CR 303030205 em 10/11/2020
# MAGIC  UNION ALL
# MAGIC  SELECT * FROM zrateio_ep_step05_02_reversao_senai_nao_30303
# MAGIC )D 
# MAGIC ON  D.cd_entidade_regional       = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC AND D.cd_conta_contabil          = R.cd_conta_contabil
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only ~10k records and ~240K in mem cache. Keep in 1 partition.
"""

rateio_ep_step05_01_despesa_direta_ensino_prof = o\
.filter(((f.col("cd_centro_responsabilidade").startswith("30303")) &\
         ~(f.col("cd_conta_contabil").startswith("31010623"))) |\
        (f.col("cd_centro_responsabilidade").startswith("30304")))\
.join(e, ["cd_entidade_regional_erp_oltp"], "inner")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_conta_contabil")\
.agg(f.sum("vl_lancamento").alias("vl_desptot_ensino_prof"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")), \
                  f.col("vl_lancamento")) \
           .otherwise(f.lit(0))).alias("vl_despcor_ensino_prof"))\
.withColumnRenamed("cd_entidade_nacional", "cd_entidade_nacional_05")\
.withColumnRenamed("sg_entidade_regional", "sg_entidade_regional_05")\
.withColumnRenamed("cd_entidade_regional", "cd_entidade_regional_05")\
.withColumnRenamed("cd_centro_responsabilidade", "cd_centro_responsabilidade_05")\
.withColumnRenamed("cd_conta_contabil", "cd_conta_contabil_05")\
.drop("vl_lancamento", "cd_entidade_regional_erp_oltp")\
.coalesce(1)\
.cache()

# Activate cache
rateio_ep_step05_01_despesa_direta_ensino_prof.count()

# COMMAND ----------

#assert  rateio_ep_step05_01_despesa_direta_ensino_prof.count() == spark.sql("select count(*) from rateio_ep_step05_01_despesa_direta_ensino_prof").collect()[0][0]

# COMMAND ----------

"""
This is a small table, in test fase it has only ~10k records and ~240K in mem cache. Keep in 1 partition.
"""

rateio_ep_step05_02_reversao_senai_nao_30303 = o\
.filter(f.col("cd_conta_contabil").startswith("31010623") &\
        ~(f.col("cd_centro_responsabilidade").startswith("30303")))\
.withColumn("vl_lancamento", (f.col("vl_lancamento"))*(-1))\
.withColumn("cd_centro_responsabilidade", f.lit("303030205"))\
.join(e, ["cd_entidade_regional_erp_oltp"], "inner")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_conta_contabil")\
.agg(f.sum("vl_lancamento").alias("vl_desptot_ensino_prof"),
     f.sum(f.when((f.col("cd_conta_contabil").startswith("31")), \
                  f.col("vl_lancamento")) \
           .otherwise(f.lit(0))).alias("vl_despcor_ensino_prof"))\
.withColumnRenamed("cd_entidade_nacional", "cd_entidade_nacional_05")\
.withColumnRenamed("sg_entidade_regional", "sg_entidade_regional_05")\
.withColumnRenamed("cd_entidade_regional", "cd_entidade_regional_05")\
.withColumnRenamed("cd_centro_responsabilidade", "cd_centro_responsabilidade_05")\
.withColumnRenamed("cd_conta_contabil", "cd_conta_contabil_05")\
.drop("vl_lancamento", "cd_entidade_regional_erp_oltp")\
.coalesce(1)\
.cache()

# Activate cache
rateio_ep_step05_02_reversao_senai_nao_30303.count()

# COMMAND ----------

"""
Source o and e are not needed anymore.
These objects can be removed from cache.
"""
o = o.unpersist()
e = e.unpersist()

# COMMAND ----------

d = rateio_ep_step05_01_despesa_direta_ensino_prof\
.union(rateio_ep_step05_02_reversao_senai_nao_30303.select(rateio_ep_step05_01_despesa_direta_ensino_prof.columns))\
.coalesce(1)\
.cache()

# COMMAND ----------

"""
Source rateio_ep_step05_01_despesa_direta_ensino_prof and rateio_ep_step05_02_reversao_senai_nao_30303 are not needed anymore.
These objects can be removed from cache.
"""
rateio_ep_step05_01_despesa_direta_ensino_prof = rateio_ep_step05_01_despesa_direta_ensino_prof.unpersist()
rateio_ep_step05_02_reversao_senai_nao_30303 = rateio_ep_step05_02_reversao_senai_nao_30303.unpersist()

# COMMAND ----------

"""
This is a small table, in test fase it has only ~29k records and ~3M in mem cache. Keep in 1 partition.
"""

rateio_ep_step05_03_despesa_rateada = rateio_ep_step04_02_despesa_indireta_rateada\
.join(d,
      (rateio_ep_step04_02_despesa_indireta_rateada.cd_entidade_regional == rateio_ep_step05_01_despesa_direta_ensino_prof.cd_entidade_regional_05) &\
      (rateio_ep_step04_02_despesa_indireta_rateada.cd_centro_responsabilidade == rateio_ep_step05_01_despesa_direta_ensino_prof.cd_centro_responsabilidade_05) &\
      (rateio_ep_step04_02_despesa_indireta_rateada.cd_conta_contabil == rateio_ep_step05_01_despesa_direta_ensino_prof.cd_conta_contabil_05),
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
.fillna(0, subset=["vl_desptot_ensino_prof",
                   "vl_desptot_olimpiada_ensino_prof_rateada",
                   "vl_desptot_etd_gestao_ensino_prof_rateada",
                   "vl_desptot_suporte_negocio_ensino_prof_rateada",
                   "vl_desptot_indireta_gestao_ensino_prof_rateada",
                   "vl_desptot_indireta_apoio_ensino_prof_rateada",
                   "vl_desptot_indireta_desenv_inst_ensino_prof_rateada",
                   "vl_despcor_ensino_prof",
                   "vl_despcor_olimpiada_ensino_prof_rateada",
                   "vl_despcor_etd_gestao_ensino_prof_rateada",
                   "vl_despcor_suporte_negocio_ensino_prof_rateada",
                   "vl_despcor_indireta_gestao_ensino_prof_rateada",
                   "vl_despcor_indireta_apoio_ensino_prof_rateada",
                   "vl_despcor_indireta_desenv_inst_ensino_prof_rateada"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_ep_step05_03_despesa_rateada.count()

# COMMAND ----------

#assert  fta_despesa_rateada_ensino_profissional.count() == spark.sql("select count(*) from fta_despesa_rateada_ensino_profissional").collect()[0][0]

# COMMAND ----------

"""
Source rateio_ep_step05_01_despesa_direta_ensino_prof is not needed anymore.
This object can be removed from cache.
"""
rateio_ep_step05_01_despesa_direta_ensino_prof = rateio_ep_step05_01_despesa_direta_ensino_prof.unpersist()
d = d.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 06
# MAGIC ```
# MAGIC %sql
# MAGIC --====================================================================================================================================
# MAGIC -- STEP06: Calcula despesa com gratuidade a partir das despesas rateadas unificadas no STEP05 com a produção obtida no STEP03
# MAGIC -- e registra o resultado final do rateio incluindo as despesas com gratuidade
# MAGIC -- Incluído em 12/11/2020 
# MAGIC --====================================================================================================================================
# MAGIC 
# MAGIC -- gravar em biz/orcamento/fta_gestao_financeira_ensino_profissional
# MAGIC DROP TABLE   fta_gestao_financeira_ensino_profissional;
# MAGIC CREATE TABLE fta_gestao_financeira_ensino_profissional AS
# MAGIC SELECT
# MAGIC --#prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC --#prm_mes_fechamento AS cd_mes_fechamento,
# MAGIC --#prm_data_corte AS  AS dt_fechamento,
# MAGIC R.cd_entidade_regional,
# MAGIC R.cd_centro_responsabilidade,
# MAGIC R.cd_conta_contabil,
# MAGIC 
# MAGIC R.vl_desptot_ensino_prof,
# MAGIC R.vl_desptot_olimpiada_ensino_prof_rateada,
# MAGIC R.vl_desptot_etd_gestao_ensino_prof_rateada,
# MAGIC R.vl_desptot_suporte_negocio_ensino_prof_rateada, 
# MAGIC R.vl_desptot_indireta_gestao_ensino_prof_rateada,
# MAGIC R.vl_desptot_indireta_apoio_ensino_prof_rateada,
# MAGIC R.vl_desptot_indireta_desenv_inst_ensino_prof_rateada,
# MAGIC R.vl_despcor_ensino_prof,
# MAGIC R.vl_despcor_olimpiada_ensino_prof_rateada,
# MAGIC R.vl_despcor_etd_gestao_ensino_prof_rateada,
# MAGIC R.vl_despcor_suporte_negocio_ensino_prof_rateada, 
# MAGIC R.vl_despcor_indireta_gestao_ensino_prof_rateada,
# MAGIC R.vl_despcor_indireta_apoio_ensino_prof_rateada,
# MAGIC R.vl_despcor_indireta_desenv_inst_ensino_prof_rateada,
# MAGIC 
# MAGIC -- despesa total com gratuidade
# MAGIC CASE WHEN NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_ensino_prof / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_olimpiada_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_olimpiada_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_etd_gestao_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_etd_gestao_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_suporte_negocio_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_suporte_negocio_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_gestao_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_gestao_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_apoio_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_apoio_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_desptot_indireta_desenv_inst_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_desptot_indireta_desenv_inst_ensino_prof_gratuidade,
# MAGIC 
# MAGIC -- despesa corrente com gratuidade
# MAGIC CASE WHEN NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_ensino_prof / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_olimpiada_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_olimpiada_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_etd_gestao_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_etd_gestao_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_suporte_negocio_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_suporte_negocio_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_gestao_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_gestao_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_apoio_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_apoio_ensino_prof_gratuidade,
# MAGIC CASE WHEN  NVL(H.qt_hr_escolar, 0) > 0
# MAGIC      THEN (R.vl_despcor_indireta_desenv_inst_ensino_prof_rateada / H.qt_hr_escolar) * H.qt_hr_escolar_gratuidade_reg 
# MAGIC      ELSE 0 END AS vl_despcor_indireta_desenv_inst_ensino_prof_gratuidade
# MAGIC      
# MAGIC FROM rateio_ep_step05_03_despesa_rateada R 
# MAGIC LEFT JOIN rateio_ep_step03_hora_escolar H 
# MAGIC ON  H.cd_entidade_regional       = R.cd_entidade_regional 
# MAGIC AND H.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC ```

# COMMAND ----------

"""
This is the final object
This is a small table, in test fase it has only ~29k records and ~3M in mem cache. Keep in 1 partition.
"""

fta_gestao_financeira_ensino_profissional = rateio_ep_step05_03_despesa_rateada\
.join(rateio_ep_step03_hora_escolar\
      .select("qt_hr_escolar", "qt_hr_escolar_gratuidade_reg", "cd_entidade_regional", "cd_centro_responsabilidade"),
      ["cd_entidade_regional", "cd_centro_responsabilidade"],
      "left")\
.fillna(0, subset=["vl_desptot_ensino_prof",
                   "vl_desptot_olimpiada_ensino_prof_rateada",
                   "vl_desptot_etd_gestao_ensino_prof_rateada",
                   "vl_desptot_suporte_negocio_ensino_prof_rateada",
                   "vl_desptot_indireta_gestao_ensino_prof_rateada",
                   "vl_desptot_indireta_apoio_ensino_prof_rateada",
                   "vl_desptot_indireta_desenv_inst_ensino_prof_rateada",
                   "vl_despcor_ensino_prof",
                   "vl_despcor_olimpiada_ensino_prof_rateada",
                   "vl_despcor_etd_gestao_ensino_prof_rateada",
                   "vl_despcor_suporte_negocio_ensino_prof_rateada",
                   "vl_despcor_indireta_gestao_ensino_prof_rateada",
                   "vl_despcor_indireta_apoio_ensino_prof_rateada",
                   "vl_despcor_indireta_desenv_inst_ensino_prof_rateada",
                   "qt_hr_escolar"])\
.withColumn("vl_desptot_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_ensino_prof") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_olimpiada_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_olimpiada_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_etd_gestao_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_etd_gestao_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_suporte_negocio_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_suporte_negocio_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_gestao_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_indireta_gestao_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_apoio_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_indireta_apoio_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_desptot_indireta_desenv_inst_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_desptot_indireta_desenv_inst_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_ensino_prof") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_olimpiada_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_olimpiada_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_etd_gestao_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_etd_gestao_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_suporte_negocio_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_suporte_negocio_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_gestao_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_indireta_gestao_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_apoio_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_indireta_apoio_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("vl_despcor_indireta_desenv_inst_ensino_prof_gratuidade",
           f.when(f.col("qt_hr_escolar") > 0, 
                  (f.col("vl_despcor_indireta_desenv_inst_ensino_prof_rateada") / f.col("qt_hr_escolar")) * f.col("qt_hr_escolar_gratuidade_reg"))\
           .otherwise(f.lit(0)))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.drop("cd_entidade_nacional", "sg_entidade_regional", "qt_hr_escolar", "qt_hr_escolar_gratuidade_reg")\
.coalesce(1)\
.cache()

# Activate cache
fta_gestao_financeira_ensino_profissional.count()

# COMMAND ----------

"""
Source rateio_ep_step05_03_despesa_rateada and rateio_ep_step05_03_despesa_rateada are not needed anymore.
These object can be removed from cache.
"""
rateio_ep_step05_03_despesa_rateada = rateio_ep_step05_03_despesa_rateada.unpersist()
rateio_ep_step03_hora_escolar = rateio_ep_step03_hora_escolar.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO 1 - OK

sg_entidade_regional = 'SENAI-SP'
cd_entidade_regional = 1117285

vl_despcor_ensino_prof - 18101901.93
vl_despcor_etd_gestao_ensino_prof_rateada - 3379431.4759817636
vl_despcor_olimpiada_ensino_prof_rateada - 286386.5338253225
vl_despcor_suporte_negocio_ensino_prof_rateada - 4128688.558133996
vl_despcor_indireta_gestao_ensino_prof_rateada - 289160.77707759547
vl_despcor_indireta_apoio_ensino_prof_rateada - 899846.0785998286
calc_vl_despcor_indireta_gestao_apoio - 1189006.855677424
vl_despcor_indireta_desenv_inst_ensino_prof_rateada - 441247.9341017866

display(fta_gestao_financeira_ensino_profissional \
.filter((f.col("cd_entidade_regional") == 1117285) &\
        (f.col("cd_conta_contabil").startswith("31010103")) &\
        (f.col("cd_centro_responsabilidade").isin('303030201', '303030202', '303030205','303030209')))\
.withColumn("cd_conta_contabil", f.substring(f.col("cd_conta_contabil"), 1, 8))\
.groupBy("cd_conta_contabil")\
.agg(f.sum("vl_despcor_ensino_prof").alias("vl_despcor_ensino_prof"), 
     f.sum("vl_despcor_etd_gestao_ensino_prof_rateada").alias("vl_despcor_etd_gestao_ensino_prof_rateada"),
     f.sum("vl_despcor_olimpiada_ensino_prof_rateada").alias("vl_despcor_olimpiada_ensino_prof_rateada"),
     f.sum("vl_despcor_suporte_negocio_ensino_prof_rateada").alias("vl_despcor_suporte_negocio_ensino_prof_rateada"),
     f.sum("vl_despcor_indireta_gestao_ensino_prof_rateada").alias("vl_despcor_indireta_gestao_ensino_prof_rateada"),
     f.sum("vl_despcor_indireta_apoio_ensino_prof_rateada").alias("vl_despcor_indireta_apoio_ensino_prof_rateada"),     
     f.sum(f.col("vl_despcor_indireta_gestao_ensino_prof_rateada") + f.col("vl_despcor_indireta_apoio_ensino_prof_rateada")).alias("calc_vl_despcor_indireta_gestao_apoio"),
     f.sum("vl_despcor_indireta_desenv_inst_ensino_prof_rateada").alias("vl_despcor_indireta_desenv_inst_ensino_prof_rateada"))\
.withColumn("ds_grupo_cr_fic", f.lit('FIC - Presencial')))
"""

# COMMAND ----------

"""
VALIDAÇÃO 2 - OK

var_hcr_src = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], "/corporativo/dim_hierarquia_centro_responsabilidade") 
var_hcr_columns = ["cd_centro_responsabilidade", "ds_centro_responsabilidade"]
df_dim_her = spark.read.parquet(var_hcr_src)\
.select(*var_hcr_columns)


display(fta_gestao_financeira_ensino_profissional \
.filter((f.col("cd_entidade_regional") == 1117285) &\
        (f.col("cd_centro_responsabilidade").isin('303030101', '303030402', '303030205','303040101')))\
.join(df_dim_her, ["cd_centro_responsabilidade"], "inner")\
.groupBy("ds_centro_responsabilidade", "cd_centro_responsabilidade")\
.agg(f.sum(f.col("vl_despcor_ensino_prof") + \
           f.col("vl_despcor_olimpiada_ensino_prof_rateada") \
           + f.col("vl_despcor_etd_gestao_ensino_prof_rateada")) \
     .alias("vl_despcor_direta_ensino_prof"),
     f.sum("vl_despcor_suporte_negocio_ensino_prof_rateada").alias("vl_despcor_suporte_negocio_ensino_prof_rateada"),
     f.sum(f.col("vl_despcor_indireta_gestao_ensino_prof_rateada") + \
           f.col("vl_despcor_indireta_apoio_ensino_prof_rateada") + \
           f.col("vl_despcor_indireta_desenv_inst_ensino_prof_rateada")) \
     .alias("vl_despcor_indireta_ensino_prof_rateada")) \
.orderBy("ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 2 - OK


df_dim_her.createOrReplaceTempView("dim_hierarquia_centro_responsabilidade")

display(spark.sql("select b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade, \
                  SUM(vl_despcor_ensino_prof+vl_despcor_olimpiada_ensino_prof_rateada+vl_despcor_etd_gestao_ensino_prof_rateada) as vl_despcor_direta_ensino_prof, \
                  SUM(vl_despcor_suporte_negocio_ensino_prof_rateada) AS vl_despcor_suporte_negocio_ensino_prof_rateada, \
                  SUM(vl_despcor_indireta_gestao_ensino_prof_rateada+vl_despcor_indireta_apoio_ensino_prof_rateada+vl_despcor_indireta_desenv_inst_ensino_prof_rateada) \
                  AS vl_despcor_indireta_ensino_prof_rateada \
                  from fta_gestao_financeira_ensino_profissional a \
                  inner join dim_hierarquia_centro_responsabilidade b \
                  on a.cd_centro_responsabilidade = b.cd_centro_responsabilidade \
                  where a.sg_entidade_regional = 'SENAI-SP' \
                  and a.cd_centro_responsabilidade in (303030101, 303030402, 303030205, 303040101) \
                  group by b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade \
                  order by b.ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 3 - OK

display(fta_gestao_financeira_ensino_profissional \
.filter((f.col("cd_entidade_regional") == 1117285) &\
        (f.col("cd_centro_responsabilidade").isin('303030101', '303030402', '303030205','303040101')))\
.join(df_dim_her, ["cd_centro_responsabilidade"], "inner")\
.groupBy("ds_centro_responsabilidade", "cd_centro_responsabilidade")\
.agg(f.sum(f.col("vl_desptot_ensino_prof") + \
           f.col("vl_desptot_olimpiada_ensino_prof_rateada") \
           + f.col("vl_desptot_etd_gestao_ensino_prof_rateada")) \
     .alias("vl_desptot_direta_ensino_prof"),
     f.sum("vl_desptot_suporte_negocio_ensino_prof_rateada").alias("vl_despcor_suporte_negocio_ensino_prof_rateada"),
     f.sum(f.col("vl_desptot_indireta_gestao_ensino_prof_rateada") + \
           f.col("vl_desptot_indireta_apoio_ensino_prof_rateada") + \
           f.col("vl_desptot_indireta_desenv_inst_ensino_prof_rateada")) \
     .alias("vl_desptot_indireta_ensino_prof_rateada")) \
.orderBy("ds_centro_responsabilidade"))
"""

# COMMAND ----------

"""
VALIDAÇÃO 3 - OK

display(spark.sql("select b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade, \
                  SUM(vl_desptot_ensino_prof+vl_desptot_olimpiada_ensino_prof_rateada+vl_desptot_etd_gestao_ensino_prof_rateada) as vl_desptot_direta_ensino_prof, \
                  SUM(vl_desptot_suporte_negocio_ensino_prof_rateada) AS vl_desptot_suporte_negocio_ensino_prof_rateada, \
                  SUM(vl_desptot_indireta_gestao_ensino_prof_rateada+vl_desptot_indireta_apoio_ensino_prof_rateada+vl_desptot_indireta_desenv_inst_ensino_prof_rateada) \
                  AS vl_desptot_indireta_ensino_prof_rateada \
                  from fta_gestao_financeira_ensino_profissional a \
                  inner join dim_hierarquia_centro_responsabilidade b \
                  on a.cd_centro_responsabilidade = b.cd_centro_responsabilidade \
                  where a.sg_entidade_regional = 'SENAI-SP' \
                  and a.cd_centro_responsabilidade in (303030101, 303030402, 303030205, 303040101) \
                  group by b.ds_centro_responsabilidade, \
                  a.cd_centro_responsabilidade \
                  order by b.ds_centro_responsabilidade"))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz

# COMMAND ----------

fta_gestao_financeira_ensino_profissional = tcf.add_control_fields(fta_gestao_financeira_ensino_profissional, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjusting schema

# COMMAND ----------

var_column_type_map = {"cd_entidade_regional": "int",
                       "cd_centro_responsabilidade": "string",
                       "cd_conta_contabil": "string",
                       "vl_desptot_olimpiada_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_etd_gestao_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_suporte_negocio_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_gestao_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_apoio_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_indireta_desenv_inst_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_olimpiada_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_etd_gestao_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_suporte_negocio_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_apoio_ensino_prof_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_desenv_inst_ensino_prof_rateada": "decimal(18,6)",
                       "vl_desptot_ensino_prof": "decimal(18,6)",
                       "vl_despcor_ensino_prof": "decimal(18,6)",
                       "vl_desptot_olimpiada_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_etd_gestao_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_suporte_negocio_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_gestao_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_apoio_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_indireta_desenv_inst_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_olimpiada_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_etd_gestao_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_suporte_negocio_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_apoio_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_indireta_desenv_inst_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_desptot_ensino_prof_gratuidade": "decimal(18,6)",
                       "vl_despcor_ensino_prof_gratuidade": "decimal(18,6)",
                       "cd_ano_fechamento": "int",
                       "cd_mes_fechamento": "int",
                       "dt_fechamento": "date"}

for c in var_column_type_map:
  fta_gestao_financeira_ensino_profissional = fta_gestao_financeira_ensino_profissional.withColumn(c, fta_gestao_financeira_ensino_profissional[c].cast(var_column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

fta_gestao_financeira_ensino_profissional.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

"""
Source fta_gestao_financeira_ensino_profissional is not needed anymore.
This object can be removed from cache.
"""
fta_gestao_financeira_ensino_profissional = fta_gestao_financeira_ensino_profissional.unpersist()

# COMMAND ----------

