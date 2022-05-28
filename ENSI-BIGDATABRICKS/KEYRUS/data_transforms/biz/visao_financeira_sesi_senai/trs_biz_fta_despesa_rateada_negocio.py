# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_despesa_rateada_negocio
# MAGIC Tabela/Arquivo Origem	/trs/evt/orcamento_nacional_realizado
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_despesa_rateada_negocio
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas diretas e indiretas por Negócios SESI e SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Thomaz Moreira - 2020/08/17 
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
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

"""
MOCK FOR USING THIS IN DEV
Even though we're reading data from prod, sink must point to dev when in development
"""
"""
var_tables = {"origins":["/evt/orcamento_nacional_realizado", "/mtd/corp/entidade_regional", "/mtd/corp/entidade_nacional"],
  "destination":"/orcamento/fta_despesa_rateada_negocio", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/trs_biz_fta_despesa_rateada_negocio"}
  }

var_dls =  {"folders":
  { "landing":"/lnd", 
  "error":"/err", 
  "staging":"/stg", 
  "log":"/log", 
  "raw":"/raw", 
  "trusted": "/trs", 
  "business": "/biz"}
  }

var_adf =  {"adf_factory_name":"cnibigdatafactory",
"adf_pipeline_name":"trs_biz_fta_despesa_rateada_negocio",
"adf_pipeline_run_id":"development",
"adf_trigger_id":"development",
"adf_trigger_name":"thomaz",
"adf_trigger_time":"2020-08-17T14:00:00.8532201Z",
"adf_trigger_type":"Manual"
}

var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}
"""

# COMMAND ----------

"""
USE THIS FOR DEV PURPOSES

#var_sink = "{}{}{}".format(var_adls_uri, "/tmp/dev" + var_dls["folders"]["business"], var_tables["destination"]) 
"""
var_src_onr, var_src_reg, var_src_nac =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], t) for t in var_tables["origins"]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
var_src_onr, var_src_reg, var_src_nac, var_sink

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
# MAGIC ```
# MAGIC --====================================================================================================================================
# MAGIC -- STEP01: Leitura inicial despesa orçamento SESI e SENAI (excetuando as despesas de reversão para esta entidade) na camada Trusted
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE rateio_step01_despesa;
# MAGIC CREATE TABLE rateio_step01_despesa AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC SUM(CASE WHEN o.cd_conta_contabil LIKE '31%' THEN o.vl_lancamento ELSE 0 END) AS vl_despesa_corrente,
# MAGIC SUM(o.vl_lancamento) AS vl_despesa_total
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE o.cd_conta_contabil LIKE '3%' -- para despesas
# MAGIC -- em 06/11/2020 retirada do filtro abaixo a condição de reversão
# MAGIC AND   (e.cd_entidade_nacional = 2 OR e.cd_entidade_nacional = 3) -- SESI ou SENAI
# MAGIC -- AND YEAR(o.dt_lancamento) = #prm_ano_fechamento AND MONTH(o.dt_lancamento) <= #prm_mes_fechamento AND o.dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC -- filtro para teste fechamento jun 2020
# MAGIC AND   YEAR(o.dt_lancamento) = 2020 AND MONTH(o.dt_lancamento) <= 6 AND o.dt_ultima_atualizacao_oltp <= '2020-07-22'
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade
# MAGIC ```

# COMMAND ----------

"""
I'll try to keep the same notation as documentation when possible
I'll apply f.coalesce to all numeric values to avoid having to treat all the nulls later.
Using display and filter f.col("vl_lancamento").isNull(), no records are shown.
"""
var_o_columns = ["cd_centro_responsabilidade",
                 "cd_conta_contabil",
                 f.coalesce("vl_lancamento", f.lit(0)).alias("vl_lancamento"),
                 "cd_entidade_regional_erp_oltp",
                 "dt_lancamento",
                 "dt_ultima_atualizacao_oltp"                 
                ]

o = spark.read.parquet(var_src_onr)\
.select(*var_o_columns)\
.filter(
  (f.col("cd_conta_contabil").startswith('3'))
  & (f.year("dt_lancamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))
  & (f.month("dt_lancamento") <= f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))
  & (f.col("dt_ultima_atualizacao_oltp") <= f.lit(var_parameters["prm_data_corte"]).cast("timestamp"))
)\
.drop(*["dt_lancamento", "dt_ultima_atualizacao_oltp"])

# COMMAND ----------

"""
# e
This object will be reused a lot. It is really small: < 100 records. 
Cache it and keep in 1 partition.
"""

var_e_columns = ["cd_entidade_nacional",
                 "sg_entidade_regional",
                 "cd_entidade_regional",
                 "cd_entidade_regional_erp_oltp"
                ]

e = spark.read.parquet(var_src_reg)\
.select(*var_e_columns)\
.coalesce(1)\
.cache()

# Activate cache
e.count()

# COMMAND ----------

"""
I'm testing for the first load and the object is really small:
~3k records, 95KB in mem cached. Can be kept in 1 partition

No records here: display(rateio_step01_despesa.filter((f.col("vl_despesa_total").isNull()) | ((f.col("vl_despesa_corrente").isNull()))))  
"""

rateio_step01_despesa = o.join(e, on=["cd_entidade_regional_erp_oltp"], how="inner")\
.filter(
  (f.col("cd_entidade_nacional") == "2")
  | (f.col("cd_entidade_nacional") == "3")
)\
.drop(*["cd_entidade_regional_erp_oltp"])\
.withColumn("vl_despesa_corrente", f.when(f.col("cd_conta_contabil").startswith("31"), f.col("vl_lancamento")).otherwise(f.lit(0)))\
.drop(*["cd_conta_contabil"])\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(
  f.sum(f.col("vl_lancamento")).alias("vl_despesa_total"),
  f.sum(f.col("vl_despesa_corrente")).alias("vl_despesa_corrente")
)\
.coalesce(1)\
.cache()

# Activate cache
rateio_step01_despesa.count()

# COMMAND ----------

"""
# VALIDACAO - OK!

total: 2973767787.0999966
corrente: 2660255156.7899995
"""

"""
display(rateio_step01_despesa\
        .filter(f.col("sg_entidade_regional").startswith('SESI'))\
        .agg(f.sum(f.col("vl_despesa_total")), f.sum(f.col("vl_despesa_corrente")))
       )
"""

# COMMAND ----------

"""
# VALIDACAO - OK!

total: 2237161453.6
corrente: 2039310504.7500002
"""
"""
display(rateio_step01_despesa\
        .filter(f.col("sg_entidade_regional").startswith('SENAI'))\
        .agg(f.sum(f.col("vl_despesa_total")), f.sum(f.col("vl_despesa_corrente")))
       )
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --============================================================================================================================================
# MAGIC -- STEP02: Contabiliza as transferências regulamentares a serem descontadas do vl_despesa_indireta_desenvolvimento_institucional_bruta adiante
# MAGIC --============================================================================================================================================
# MAGIC -- rename em 06/11/2020 de zrateio_step02_transferencia_reg para zrateio_step02_01_transferencia_reg
# MAGIC DROP TABLE rateio_step02_01_transferencia_reg;
# MAGIC CREATE TABLE rateio_step02_01_transferencia_reg AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC SUM(o.vl_lancamento) AS vl_transferencia_reg
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC WHERE 
# MAGIC (
# MAGIC    cd_conta_contabil LIKE '31020101%' -- Federação/CNI
# MAGIC OR cd_conta_contabil LIKE '31020102%' -- Conselho Nacional do SESI
# MAGIC OR cd_conta_contabil LIKE '31020105%' -- IEL
# MAGIC OR cd_conta_contabil LIKE '31011001%' -- Receita Federal (3,5% Arrecadação Indireta)
# MAGIC )
# MAGIC AND e.cd_entidade_nacional IN (2, 3) -- SESI ou SENAI
# MAGIC --AND YEAR(dt_lancamento) = #prm_ano_fechamento AND MONTH(dt_lancamento) <= #prm_mes_fechamento AND dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC -- filtro para teste fechamento jun 2020
# MAGIC AND   YEAR(o.dt_lancamento) = 2020 AND MONTH(o.dt_lancamento) <= 6 AND o.dt_ultima_atualizacao_oltp <= '2020-07-22'
# MAGIC GROUP BY
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional
# MAGIC 
# MAGIC -- incluído em 06/11/2020
# MAGIC DROP TABLE zrateio_step02_02_reversao_senai;
# MAGIC CREATE TABLE zrateio_step02_02_reversao_senai AS
# MAGIC SELECT
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC CASE WHEN o.cd_centro_responsabilidade NOT LIKE '30303%'
# MAGIC      -- reversão SENAI em CR errado
# MAGIC      THEN '303030205' -- apropria neste CR específico para corrigir
# MAGIC      ELSE o.cd_centro_responsabilidade
# MAGIC END AS cd_centro_responsabilidade,
# MAGIC SUM(o.vl_lancamento) AS vl_reversao_contrib_industrial_senai
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC WHERE cd_conta_contabil LIKE '31010623%' -- reversão de contribuições industriais do SENAI
# MAGIC AND e.cd_entidade_nacional = 3 --SENAI
# MAGIC --AND YEAR(dt_lancamento) = #prm_ano_fechamento AND MONTH(dt_lancamento) <= #prm_mes_fechamento AND dt_ultima_atualizacao_oltp <= #prm_data_corte
# MAGIC -- filtro para teste fechamento jun 2020
# MAGIC AND   YEAR(o.dt_lancamento) = 2020 AND MONTH(o.dt_lancamento) <= 6 AND o.dt_ultima_atualizacao_oltp <= '2020-07-22'
# MAGIC GROUP BY
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC CASE WHEN o.cd_centro_responsabilidade NOT LIKE '30303%' THEN '303030205' ELSE o.cd_centro_responsabilidade END
# MAGIC ```

# COMMAND ----------

"""
# o
Even for the first load, This object is ~1k records, keep in 1 partition.
There should be no records here:
  display(o1.filter(f.col("vl_lancamento").isNull()))

"""

var_o1_columns = ["cd_centro_responsabilidade",
                 "cd_conta_contabil",
                 f.coalesce("vl_lancamento", f.lit(0)).alias("vl_lancamento"),
                 "cd_entidade_regional_erp_oltp",
                 "dt_lancamento",
                 "dt_ultima_atualizacao_oltp"                 
                ]

o1 = spark.read.parquet(var_src_onr)\
.select(*var_o1_columns)\
.filter(
  (
    (f.col("cd_conta_contabil").startswith("31020101"))
    | (f.col("cd_conta_contabil").startswith("31020102"))
    |(f.col("cd_conta_contabil").startswith("31020105"))
    |(f.col("cd_conta_contabil").startswith("31011001"))
  )
  & (f.year("dt_lancamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))
  & (f.month("dt_lancamento") <= f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))
  & (f.col("dt_ultima_atualizacao_oltp") <= f.lit(var_parameters["prm_data_corte"]).cast("timestamp"))
)\
.drop(*["dt_lancamento", "dt_ultima_atualizacao_oltp"])\
.coalesce(1)

# COMMAND ----------

"""
This object is <100 records, 1 partition. Caching it will be awesome!
There should be no records here:
  display(rateio_step02_01_transferencia_reg.filter(f.col("vl_transferencia_reg").isNull()))
"""

rateio_step02_01_transferencia_reg = o1.join(
  e.filter(f.col("cd_entidade_nacional").isin([2, 3])), 
  on=["cd_entidade_regional_erp_oltp"], 
  how="inner"
)\
.select("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "vl_lancamento")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum(f.col("vl_lancamento")).alias("vl_transferencia_reg"))\
.coalesce(1)\
.cache()

# Activate cache
rateio_step02_01_transferencia_reg.count()

# COMMAND ----------

"""
Validação - OK

SENAI-AC 68721.15000000001
SENAI-AL 546980.7500000001
"""

"""
display(
  rateio_step02_transferencia_reg\
  .select("sg_entidade_regional", "vl_transferencia_reg")\
  .groupBy("sg_entidade_regional")\
  .agg(f.sum("vl_transferencia_reg"))
  .orderBy(f.asc("sg_entidade_regional"))
)
"""

# COMMAND ----------

"""
# o
Even for the first load, This object is ~1k records, keep in 1 partition.
There should be no records here:
  display(o2.filter(f.col("vl_lancamento").isNull()))

"""

var_o2_columns = ["cd_centro_responsabilidade",
                 "cd_conta_contabil",
                 f.coalesce("vl_lancamento", f.lit(0)).alias("vl_lancamento"),
                 "cd_entidade_regional_erp_oltp",
                 "dt_lancamento",
                 "dt_ultima_atualizacao_oltp"                 
                ]

o2 = spark.read.parquet(var_src_onr)\
.select(*var_o2_columns)\
.filter(
  (f.col("cd_conta_contabil").startswith("31010623"))  
  & (f.year("dt_lancamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))
  & (f.month("dt_lancamento") <= f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))
  & (f.col("dt_ultima_atualizacao_oltp") <= f.lit(var_parameters["prm_data_corte"]).cast("timestamp"))
)\
.withColumn("cd_centro_responsabilidade", 
            f.when(~(f.col("cd_centro_responsabilidade").startswith("30303")), f.lit("303030205"))\
             .otherwise(f.col("cd_centro_responsabilidade")))\
.drop(*["dt_lancamento", "dt_ultima_atualizacao_oltp"])\
.coalesce(1)

# COMMAND ----------

"""
This object is <100 records, 1 partition. Caching it will be awesome!
There should be no records here:
  display(rateio_step02_02_reversao_senai.filter(f.col("vl_reversao_contrib_industrial_senai").isNull()))
"""

rateio_step02_02_reversao_senai = o2.join(
  e.filter(f.col("cd_entidade_nacional") == 3), 
  on=["cd_entidade_regional_erp_oltp"], 
  how="inner"
)\
.select("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade", "vl_lancamento")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum(f.col("vl_lancamento")).alias("vl_reversao_contrib_industrial_senai"))\
.coalesce(1)\
.cache()

# Activate cache
rateio_step02_02_reversao_senai.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC --=======================================================================================================
# MAGIC -- STEP03: Obtem despesa vira vida rateada SESI na educação básica e continuada
# MAGIC --=======================================================================================================*/
# MAGIC DROP TABLE   rateio_step03_01_despesa_vira_vida;
# MAGIC CREATE TABLE rateio_step03_01_despesa_vira_vida AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_despesa_total)    AS vl_desptot_vira_vida,
# MAGIC         SUM(vl_despesa_corrente) AS vl_despcor_vira_vida
# MAGIC FROM rateio_step01_despesa
# MAGIC WHERE cd_entidade_nacional =  2 -- SESI
# MAGIC AND cd_centro_responsabilidade LIKE '303070111%' -- vira vida
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step03_02_despesa_educacao_basica_cont;
# MAGIC CREATE TABLE rateio_step03_02_despesa_educacao_basica_cont AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        vl_despesa_total    AS vl_desptot_educacao_basica_continuada,
# MAGIC        vl_despesa_corrente AS vl_despcor_educacao_basica_continuada
# MAGIC FROM rateio_step01_despesa 
# MAGIC WHERE cd_entidade_nacional =  2 -- SESI
# MAGIC AND  (cd_centro_responsabilidade LIKE '30301%'     -- EDUCACAO_BASICA
# MAGIC OR     cd_centro_responsabilidade LIKE '3030201%') -- EDUCACAO_CONTINUADA sem eventos
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step03_03_despesa_vira_vida_rateada;
# MAGIC CREATE TABLE rateio_step03_03_despesa_vira_vida_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_educacao_basica_continuada / SUM(D.vl_desptot_educacao_basica_continuada) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_vira_vida 
# MAGIC AS vl_desptot_vira_vida_rateada,
# MAGIC ( D.vl_despcor_educacao_basica_continuada / SUM(D.vl_despcor_educacao_basica_continuada) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_vira_vida 
# MAGIC AS vl_despcor_vira_vida_rateada
# MAGIC FROM rateio_step03_02_despesa_educacao_basica_cont D
# MAGIC INNER JOIN rateio_step03_01_despesa_vira_vida V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC ```

# COMMAND ----------

rateio_step03_01_despesa_vira_vida =  rateio_step01_despesa\
.filter(
  (f.col("cd_entidade_nacional") ==  2) 
  & (f.col("cd_centro_responsabilidade").startswith("303070111"))
)\
.select("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "vl_despesa_total", "vl_despesa_corrente")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(
  f.sum("vl_despesa_total").alias("vl_desptot_vira_vida"), 
  f.sum("vl_despesa_corrente").alias("vl_despcor_vira_vida")
)\
.coalesce(1)

# COMMAND ----------

rateio_step03_02_despesa_educacao_basica_cont = rateio_step01_despesa\
.filter(
  (f.col("cd_entidade_nacional") == 2)
  & (f.col("cd_centro_responsabilidade").startswith("30301")) 
  | (f.col("cd_centro_responsabilidade").startswith("3030201"))
)\
.select(
  "cd_entidade_nacional", 
  "sg_entidade_regional", 
  "cd_entidade_regional", 
  "cd_centro_responsabilidade", 
  f.col("vl_despesa_total").alias("vl_desptot_educacao_basica_continuada"), 
  f.col("vl_despesa_corrente").alias("vl_despcor_educacao_basica_continuada")
)\
.coalesce(1)

# COMMAND ----------

"""
In the first load, this object must be already 1 partition and <200 records.
It will be used again in step 5, so keep in cache.

There should be no records here:
  display(rateio_step03_03_despesa_vira_vida_rateada.filter((f.col("vl_despcor_vira_vida_rateada").isNull()) |(f.col("vl_desptot_vira_vida_rateada").isNull())))
"""

window = Window.partitionBy("cd_entidade_regional")

rateio_step03_03_despesa_vira_vida_rateada = rateio_step03_02_despesa_educacao_basica_cont\
.select(
  "cd_entidade_nacional", 
  "cd_entidade_regional",
  "sg_entidade_regional", 
  "cd_centro_responsabilidade",
  "vl_desptot_educacao_basica_continuada",
  "vl_despcor_educacao_basica_continuada"
)\
.join(
  rateio_step03_01_despesa_vira_vida.select("cd_entidade_regional", "vl_desptot_vira_vida", "vl_despcor_vira_vida"), 
  on=["cd_entidade_regional"], 
  how="inner"
)\
.withColumn(
  "vl_desptot_vira_vida_rateada", 
  ((f.col("vl_desptot_educacao_basica_continuada") / f.sum("vl_desptot_educacao_basica_continuada").over(window)) * f.col("vl_desptot_vira_vida"))
)\
.withColumn(
  "vl_despcor_vira_vida_rateada", 
  ((f.col("vl_despcor_educacao_basica_continuada") / f.sum("vl_despcor_educacao_basica_continuada").over(window)) * f.col("vl_despcor_vira_vida"))
)\
.drop(*["vl_desptot_educacao_basica_continuada", "vl_despcor_educacao_basica_continuada", "vl_desptot_vira_vida", "vl_despcor_vira_vida"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step03_03_despesa_vira_vida_rateada.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP04: Obtem despesa olímpiada rateada SENAI na educação profissional
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rateio_step04_01_despesa_olimpiada;
# MAGIC CREATE TABLE rateio_step04_01_despesa_olimpiada AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_despesa_total)    AS vl_desptot_olimpiada,
# MAGIC         SUM(vl_despesa_corrente) AS vl_despcor_olimpiada
# MAGIC FROM  rateio_step01_despesa
# MAGIC WHERE cd_entidade_nacional = 3 -- SENAI
# MAGIC AND   cd_centro_responsabilidade LIKE '303070108%' -- Olimpíadas
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step04_02_despesa_educacao_profissional;
# MAGIC CREATE TABLE rateio_step04_02_despesa_educacao_profissional AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        vl_despesa_total    AS vl_desptot_educacao_profissional,
# MAGIC        vl_despesa_corrente AS vl_despcor_educacao_profissional
# MAGIC FROM rateio_step01_despesa 
# MAGIC WHERE cd_entidade_nacional = 3 -- SENAI
# MAGIC AND  (cd_centro_responsabilidade LIKE '30303%'  -- EDUCACAO PROFISSIONAL E TECNOLOGICA
# MAGIC OR    cd_centro_responsabilidade LIKE  '30304%') -- EDUCACAO SUPERIOR
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step04_03_despesa_olimpiada_rateada;
# MAGIC CREATE TABLE rateio_step04_03_despesa_olimpiada_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_educacao_profissional / SUM(D.vl_desptot_educacao_profissional) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_olimpiada 
# MAGIC AS vl_desptot_olimpiada_rateada,
# MAGIC ( D.vl_despcor_educacao_profissional / SUM(D.vl_despcor_educacao_profissional) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_olimpiada 
# MAGIC AS vl_despcor_olimpiada_rateada
# MAGIC FROM rateio_step04_02_despesa_educacao_profissional D
# MAGIC INNER JOIN rateio_step04_01_despesa_olimpiada V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC ```

# COMMAND ----------

"""
For the fisrt load, this object is <100 records and should be kept in 1 partition.
"""

rateio_step04_01_despesa_olimpiada = rateio_step01_despesa\
.filter(
  (f.col("cd_entidade_nacional") == 3)
  & (f.col("cd_centro_responsabilidade").startswith("303070108"))
)\
.select("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "vl_despesa_total", "vl_despesa_corrente")\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(
  f.sum("vl_despesa_total").alias("vl_desptot_olimpiada"), 
  f.sum("vl_despesa_corrente").alias("vl_despcor_olimpiada")
)\
.coalesce(1)

#rateio_step04_01_despesa_olimpiada.count(), rateio_step04_01_despesa_olimpiada.rdd.getNumPartitions()

# COMMAND ----------

"""
In first load, this object is <300 records.
1 partition will do.
"""

rateio_step04_02_despesa_educacao_profissional = rateio_step01_despesa\
.filter(
  (f.col("cd_entidade_nacional") == 3) 
  & (f.col("cd_centro_responsabilidade").startswith("30303"))
  | (f.col("cd_centro_responsabilidade").startswith("30304"))
)\
.select("cd_entidade_nacional",
       "sg_entidade_regional", 
       "cd_entidade_regional",
       "cd_centro_responsabilidade",
       f.col("vl_despesa_total").alias("vl_desptot_educacao_profissional"),
       f.col("vl_despesa_corrente").alias("vl_despcor_educacao_profissional")
       )\
.coalesce(1)

# rateio_step04_02_despesa_educacao_profissional.count(), rateio_step04_02_despesa_educacao_profissional.rdd.getNumPartitions()

# COMMAND ----------

"""
Uses the same window as before!
In first load, this object is <300 records.
1 partition will do.
There should be no records here:
  display(rateio_step04_03_despesa_olimpiada_rateada.filter((f.col("vl_despcor_olimpiada_rateada").isNull()) | (f.col("vl_desptot_olimpiada_rateada").isNull())))


Keep it in cache cause it will be used in step 5.
"""

rateio_step04_03_despesa_olimpiada_rateada = rateio_step04_02_despesa_educacao_profissional\
.join(rateio_step04_01_despesa_olimpiada.select("cd_entidade_regional", "vl_desptot_olimpiada", "vl_despcor_olimpiada"),
     on=["cd_entidade_regional"],
     how="inner"
 )\
.withColumn(
  "vl_desptot_olimpiada_rateada", 
  ((f.col("vl_desptot_educacao_profissional") / f.sum("vl_desptot_educacao_profissional").over(window)) * f.col("vl_desptot_olimpiada"))
)\
.withColumn(
  "vl_despcor_olimpiada_rateada", 
  ((f.col("vl_despcor_educacao_profissional") / f.sum("vl_despcor_educacao_profissional").over(window)) * f.col("vl_despcor_olimpiada"))
)\
.drop(*["vl_desptot_educacao_profissional", "vl_despcor_educacao_profissional", "vl_desptot_olimpiada", "vl_despcor_olimpiada"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step04_03_despesa_olimpiada_rateada.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP05: Atualiza a Tabela de Despesas com vira vida (SESI) e olimpíadas rateadas (SENAI) e retira
# MAGIC -- a reversão das despesas diretas
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   zrateio_step05_despesa;
# MAGIC CREATE TABLE zrateio_step05_despesa AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC -- subtraída reversão em 09/11/2020 da vl_despesa_total
# MAGIC D.vl_despesa_total    - NVL(V.vl_reversao_contrib_industrial_senai, 0) AS vl_despesa_total,
# MAGIC NVL(S3.vl_desptot_vira_vida_rateada, 0)                                AS vl_desptot_vira_vida_rateada,
# MAGIC NVL(S4.vl_desptot_olimpiada_rateada, 0)                                AS vl_desptot_olimpiada_rateada,
# MAGIC -- subtraída reversão em 09/11/2020 da vl_despesa_corrente
# MAGIC D.vl_despesa_corrente - NVL(V.vl_reversao_contrib_industrial_senai, 0) AS vl_despesa_corrente,
# MAGIC NVL(S3.vl_despcor_vira_vida_rateada, 0)                                AS vl_despcor_vira_vida_rateada,
# MAGIC NVL(S4.vl_despcor_olimpiada_rateada, 0)                                AS vl_despcor_olimpiada_rateada
# MAGIC FROM zrateio_step01_despesa D
# MAGIC LEFT JOIN zrateio_step03_03_despesa_vira_vida_rateada S3
# MAGIC ON  D.cd_entidade_regional = S3.cd_entidade_regional
# MAGIC AND D.cd_centro_responsabilidade = S3.cd_centro_responsabilidade
# MAGIC LEFT JOIN zrateio_step04_03_despesa_olimpiada_rateada S4
# MAGIC ON  D.cd_entidade_regional = S4.cd_entidade_regional
# MAGIC AND D.cd_centro_responsabilidade = S4.cd_centro_responsabilidade
# MAGIC -- incluído em 09/11/2020
# MAGIC LEFT JOIN zrateio_step02_02_reversao_senai V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional
# MAGIC AND D.cd_centro_responsabilidade = V.cd_centro_responsabilidade
# MAGIC -- vira vida do SESI e olimpiada do SENAI foram rateados, saem da tabela de despesas
# MAGIC WHERE NOT (D.cd_entidade_nacional = 2 AND D.cd_centro_responsabilidade LIKE '303070111%')
# MAGIC AND   NOT (D.cd_entidade_nacional = 3 AND D.cd_centro_responsabilidade LIKE '303070108%');
# MAGIC ```

# COMMAND ----------

"""
In the first load, this object is:
< 3k records, 1 partition. coalesce(1) to guarantee always 1 partition.
Will be used again in step06, can be cached.

The last .select() is really needed to avoid nulls
"""

rateio_step05_despesa = rateio_step01_despesa\
.filter(
  ~((f.col("cd_entidade_nacional") == 2) & (f.col("cd_centro_responsabilidade").startswith("303070111")))
  & ~((f.col("cd_entidade_nacional") == 3) & (f.col("cd_centro_responsabilidade").startswith("303070108"))) 
)\
.join(rateio_step03_03_despesa_vira_vida_rateada\
      .select("cd_entidade_regional", 
              "cd_centro_responsabilidade", 
              "vl_desptot_vira_vida_rateada", 
              "vl_despcor_vira_vida_rateada"
             ),
      on=["cd_entidade_regional", "cd_centro_responsabilidade"],
      how="left"
)\
.fillna(0.0, subset=["vl_desptot_vira_vida_rateada", "vl_despcor_vira_vida_rateada"])\
.join(
  rateio_step04_03_despesa_olimpiada_rateada\
  .select("cd_entidade_regional", 
          "cd_centro_responsabilidade", 
          "vl_desptot_olimpiada_rateada",
          "vl_despcor_olimpiada_rateada"
         ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=["vl_desptot_olimpiada_rateada", "vl_despcor_olimpiada_rateada"])\
.join(
  rateio_step02_02_reversao_senai\
  .select("cd_entidade_regional", 
          "cd_centro_responsabilidade", 
          "vl_reversao_contrib_industrial_senai"          
         ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=["vl_reversao_contrib_industrial_senai"])\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  (f.col("vl_despesa_total") - f.col("vl_reversao_contrib_industrial_senai")).alias("vl_despesa_total"),
  (f.col("vl_despesa_corrente") - f.col("vl_reversao_contrib_industrial_senai")).alias("vl_despesa_corrente"),
  "vl_desptot_vira_vida_rateada",
  "vl_despcor_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_despcor_olimpiada_rateada"
)\
.coalesce(1)\
.cache()

# Activate cache
rateio_step05_despesa.count()

#rateio_step05_despesa.rdd.getNumPartitions()

# COMMAND ----------

"""
# Validação - OK
"""
"""
display(rateio_step05_despesa\
.filter(f.col("sg_entidade_regional").endswith('SP'))\
.select(
  f.col("sg_entidade_regional").substr(1,4).alias("sg_entidade_regional"),
  "vl_despesa_total",
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_despesa_corrente",
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada"
)\
.withColumn("despesa_total", f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada"))\
.withColumn("despesa_corrente", f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada")+ f.col("vl_despcor_olimpiada_rateada"))\
.groupBy("sg_entidade_regional")\
.agg(
  f.sum(f.col("vl_despesa_total")).alias("vl_despesa_total"),
  f.sum(f.col("vl_desptot_vira_vida_rateada")).alias("vl_desptot_vira_vida_rateada"), 
  f.sum(f.col("vl_desptot_olimpiada_rateada")).alias("vl_desptot_olimpiada_rateada"),
  f.sum(f.col("despesa_total")).alias("despesa_total"),
  f.sum(f.col("vl_despesa_corrente")).alias("vl_despesa_corrente"),
  f.sum(f.col("vl_despcor_vira_vida_rateada")).alias("vl_despcor_vira_vida_rateada"), 
  f.sum(f.col("vl_despcor_olimpiada_rateada")).alias("vl_despcor_olimpiada_rateada"),
  f.sum(f.col("despesa_corrente")).alias("despesa_corrente")
))
"""

# COMMAND ----------

"""
Source e, step01, step2_02 step03 and atep04 are not needed anymore.
These objects can be removed from cache.
"""
rateio_step04_03_despesa_olimpiada_rateada = rateio_step04_03_despesa_olimpiada_rateada.unpersist()
rateio_step03_03_despesa_vira_vida_rateada = rateio_step03_03_despesa_vira_vida_rateada.unpersist()
rateio_step01_despesa = rateio_step01_despesa.unpersist()
rateio_step02_02_reversao_senai = rateio_step02_02_reversao_senai.unpersist()
e = e.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP06: Obtem despesas ETD e Gestão rateadas por Educação (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rateio_step06_01_despesa_etd_gestao_educacao;
# MAGIC CREATE TABLE rateio_step06_01_despesa_etd_gestao_educacao AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada)    AS vl_desptot_etd_gestao_educacao,
# MAGIC         SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada) AS vl_despcor_etd_gestao_educacao
# MAGIC FROM  rateio_step05_despesa
# MAGIC WHERE (cd_centro_responsabilidade LIKE '30310%' OR cd_centro_responsabilidade LIKE '30311%') -- gestão (10) e etd (11) em EDUCAÇÃO
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step06_02_despesa_educacao;
# MAGIC CREATE TABLE rateio_step06_02_despesa_educacao AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada)    AS vl_desptot_educacao,
# MAGIC        (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada) AS vl_despcor_educacao
# MAGIC FROM rateio_step05_despesa 
# MAGIC WHERE cd_centro_responsabilidade LIKE '303%' -- EDUCACAO 
# MAGIC AND NOT(cd_centro_responsabilidade LIKE '30310%' OR cd_centro_responsabilidade LIKE '30311%') -- retirando gestão (10) e etd (11) que serão rateados
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step06_03_despesa_etd_gestao_educacao_rateada;
# MAGIC CREATE TABLE rateio_step06_03_despesa_etd_gestao_educacao_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_educacao / SUM(D.vl_desptot_educacao) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_etd_gestao_educacao AS vl_desptot_etd_gestao_educacao_rateada,
# MAGIC ( D.vl_despcor_educacao / SUM(D.vl_despcor_educacao) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_etd_gestao_educacao AS vl_despcor_etd_gestao_educacao_rateada
# MAGIC FROM rateio_step06_02_despesa_educacao D
# MAGIC INNER JOIN rateio_step06_01_despesa_etd_gestao_educacao V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step06_despesa;
# MAGIC CREATE TABLE rateio_step06_despesa AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_despesa_total,
# MAGIC D.vl_desptot_vira_vida_rateada,
# MAGIC D.vl_desptot_olimpiada_rateada,
# MAGIC NVL(R.vl_desptot_etd_gestao_educacao_rateada, 0) AS vl_desptot_etd_gestao_educacao_rateada,
# MAGIC D.vl_despesa_corrente
# MAGIC ,D.vl_despcor_vira_vida_rateada,
# MAGIC D.vl_despcor_olimpiada_rateada,
# MAGIC NVL(R.vl_despcor_etd_gestao_educacao_rateada, 0) AS vl_despcor_etd_gestao_educacao_rateada
# MAGIC 
# MAGIC FROM rateio_step05_despesa D
# MAGIC 
# MAGIC LEFT JOIN rateio_step06_03_despesa_etd_gestao_educacao_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- gestão (10) e etd (11) foram rateados, saem da lista de despesas
# MAGIC WHERE NOT(D.cd_centro_responsabilidade LIKE '30310%' OR D.cd_centro_responsabilidade LIKE '30311%');
# MAGIC ```

# COMMAND ----------

"""
rateio_step05_despesa is already guaranteed against null values.

In first load, this object is <100 records. Keep in 1 partition.
"""

rateio_step06_01_despesa_etd_gestao_educacao = rateio_step05_despesa\
.filter(
  (f.col("cd_centro_responsabilidade").startswith("30310")) 
  | (f.col("cd_centro_responsabilidade").startswith("30311"))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional", 
  "cd_entidade_regional",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada")).alias("vl_desptot_etd_gestao_educacao"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada")).alias("vl_despcor_etd_gestao_educacao")
)\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(
  f.sum(f.col("vl_desptot_etd_gestao_educacao")).alias("vl_desptot_etd_gestao_educacao"),
  f.sum(f.col("vl_despcor_etd_gestao_educacao")).alias("vl_despcor_etd_gestao_educacao"),
)\
.coalesce(1)

#rateio_step06_01_despesa_etd_gestao_educacao.count(), rateio_step06_01_despesa_etd_gestao_educacao.rdd.getNumPartitions()

# COMMAND ----------

"""
VALIDAÇÃO - OK

display(rateio_step06_01_despesa_etd_gestao_educacao.filter(f.col("sg_entidade_regional").endswith("SP")))

%sql
select * from rateio_step06_01_despesa_etd_gestao_educacao
where sg_entidade_regional like ("%-SP");
"""

# COMMAND ----------

"""
In first load, this object is <1k records, keep it in 1 partition.

"""
rateio_step06_02_despesa_educacao = rateio_step05_despesa\
.filter(
  (f.col("cd_centro_responsabilidade").startswith("303"))
  & ~((f.col("cd_centro_responsabilidade").startswith("30310")) | f.col("cd_centro_responsabilidade").startswith("30311"))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada")).alias("vl_desptot_educacao"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada")).alias("vl_despcor_educacao")
)\
.coalesce(1)

#rateio_step06_02_despesa_educacao.count(), rateio_step06_02_despesa_educacao.rdd.getNumPartitions()

# COMMAND ----------

"""
For the first load, this object is <1k records. Keep it in 1 partition.
"""

window = Window.partitionBy(f.col("cd_entidade_regional"))

rateio_step06_03_despesa_etd_gestao_educacao_rateada = rateio_step06_02_despesa_educacao\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_desptot_educacao",
  "vl_despcor_educacao"
)\
.join(
  rateio_step06_01_despesa_etd_gestao_educacao\
  .select("cd_entidade_regional","vl_desptot_etd_gestao_educacao", "vl_despcor_etd_gestao_educacao"),
  on=["cd_entidade_regional"],
  how="inner"
)\
.withColumn(
  "vl_desptot_etd_gestao_educacao_rateada", 
  f.coalesce(((f.col("vl_desptot_educacao") / f.sum("vl_desptot_educacao").over(window)) * f.col("vl_desptot_etd_gestao_educacao")), f.lit(0))
)\
.withColumn(
  "vl_despcor_etd_gestao_educacao_rateada", 
  f.coalesce(((f.col("vl_despcor_educacao") / f.sum("vl_despcor_educacao").over(window)) * f.col("vl_despcor_etd_gestao_educacao")), f.lit(0))
)\
.drop(*["vl_desptot_educacao","vl_despcor_educacao", "vl_desptot_etd_gestao_educacao", "vl_despcor_etd_gestao_educacao"])\
.coalesce(1)

# COMMAND ----------

"""
For first load, this object is < 3k records, keep in 1 partition and cache it. 
It will be used again in step07.

Watch out and use fillna() cause spark is not dealing well with these joins and f.coalesce().
"""
rateio_step06_despesa = rateio_step05_despesa\
.filter(
  ~((f.col("cd_centro_responsabilidade").startswith("30310")) | (f.col("cd_centro_responsabilidade").startswith("30311")))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_despesa_total",
  "vl_despesa_corrente",
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada"
)\
.join(
  rateio_step06_03_despesa_etd_gestao_educacao_rateada\
  .select(
    "cd_entidade_regional", 
    "cd_centro_responsabilidade", 
    f.col("vl_desptot_etd_gestao_educacao_rateada").alias("vl_desptot_etd_gestao_educacao_rateada"),
    f.col("vl_despcor_etd_gestao_educacao_rateada").alias("vl_despcor_etd_gestao_educacao_rateada")
  ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=["vl_desptot_etd_gestao_educacao_rateada", "vl_despcor_etd_gestao_educacao_rateada"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step06_despesa.count()

# COMMAND ----------

"""
Now step05 can be unpersisted as it won't be needed anymore.
"""
rateio_step05_despesa = rateio_step05_despesa.unpersist()

# COMMAND ----------

"""
%sql

SELECT substring(sg_entidade_regional, 1, 4),
sum(vl_desptot_etd_gestao_educacao_rateada), 
SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada)    AS despesa_total,
SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada) AS despesa_corrente
FROM rateio_step06_despesa 
WHERE right(sg_entidade_regional, 2) = 'RJ'
GROUP BY substring(sg_entidade_regional, 1, 4)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP07: Obtem despesas ETD e Gestão rateadas por Outros Negócios (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rateio_step07_01_despesa_etd_gestao_outros;
# MAGIC CREATE TABLE rateio_step07_01_despesa_etd_gestao_outros AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUBSTRING(cd_centro_responsabilidade, 1, 3) AS cd_centro_responsabilidade_n2,
# MAGIC         SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada)    AS vl_desptot_etd_gestao_outros,
# MAGIC         SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada) AS vl_despcor_etd_gestao_outros        
# MAGIC FROM  rateio_step06_despesa
# MAGIC WHERE (cd_centro_responsabilidade LIKE '30_10%' OR cd_centro_responsabilidade LIKE '30_11%') -- gestao (10) e etd (11)
# MAGIC -- Filtro alterado em 06/11/2020
# MAGIC AND   (  (cd_centro_responsabilidade LIKE '302%' AND cd_entidade_nacional = 3) -- Tecnologia SENAI tem gestão e etd, no SESI não
# MAGIC       OR  cd_centro_responsabilidade LIKE '301%' -- defesa interesse SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '304%' -- ssi SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '305%' -- cultura SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '306%' -- cooperacao SESI e SENAI
# MAGIC       )
# MAGIC --
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional, SUBSTRING(cd_centro_responsabilidade, 1, 3);
# MAGIC 
# MAGIC DROP TABLE   rateio_step07_02_despesa_outros;
# MAGIC CREATE TABLE rateio_step07_02_despesa_outros AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada)    AS vl_desptot_outros,
# MAGIC        (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada) AS vl_despcor_outros     
# MAGIC FROM rateio_step06_despesa 
# MAGIC -- Filtro alterado em 06/11/2020
# MAGIC WHERE (  (cd_centro_responsabilidade LIKE '302%' AND cd_entidade_nacional = 3) -- Tecnologia SENAI tem gestão e etd, no SESI não
# MAGIC       OR  cd_centro_responsabilidade LIKE '301%' -- defesa interesse SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '304%' -- ssi SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '305%' -- cultura SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '306%' -- cooperacao SESI e SENAI
# MAGIC       )
# MAGIC --
# MAGIC AND  NOT(cd_centro_responsabilidade LIKE '30_10%' OR cd_centro_responsabilidade LIKE '30_11%') -- retirando gestão (10) e etd (11) que serão rateados
# MAGIC ;
# MAGIC 
# MAGIC  --
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step07_03_despesa_etd_gestao_outros_rateada;
# MAGIC CREATE TABLE rateio_step07_03_despesa_etd_gestao_outros_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_outros / SUM(D.vl_desptot_outros) OVER(PARTITION BY D.cd_entidade_regional, SUBSTRING(D.cd_centro_responsabilidade, 1, 3)) ) * V.vl_desptot_etd_gestao_outros 
# MAGIC AS vl_desptot_etd_gestao_outros_rateada,
# MAGIC ( D.vl_despcor_outros / SUM(D.vl_despcor_outros) OVER(PARTITION BY D.cd_entidade_regional, SUBSTRING(D.cd_centro_responsabilidade, 1, 3)) ) * V.vl_despcor_etd_gestao_outros 
# MAGIC AS vl_despcor_etd_gestao_outros_rateada
# MAGIC FROM rateio_step07_02_despesa_outros D
# MAGIC INNER JOIN rateio_step07_01_despesa_etd_gestao_outros V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional 
# MAGIC AND SUBSTRING(D.cd_centro_responsabilidade, 1, 3) = V.cd_centro_responsabilidade_n2;
# MAGIC 
# MAGIC DROP TABLE   rateio_step07_despesa;
# MAGIC CREATE TABLE rateio_step07_despesa AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_despesa_total,
# MAGIC D.vl_desptot_vira_vida_rateada,
# MAGIC D.vl_desptot_olimpiada_rateada,
# MAGIC D.vl_desptot_etd_gestao_educacao_rateada,
# MAGIC NVL(R.vl_desptot_etd_gestao_outros_rateada, 0) AS vl_desptot_etd_gestao_outros_rateada,
# MAGIC D.vl_despesa_corrente,
# MAGIC D.vl_despcor_vira_vida_rateada,
# MAGIC D.vl_despcor_olimpiada_rateada,
# MAGIC D.vl_despcor_etd_gestao_educacao_rateada,
# MAGIC NVL(R.vl_despcor_etd_gestao_outros_rateada, 0) AS vl_despcor_etd_gestao_outros_rateada
# MAGIC FROM rateio_step06_despesa D
# MAGIC 
# MAGIC LEFT JOIN rateio_step07_03_despesa_etd_gestao_outros_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- gestão (10) e etd (11) foram rateados, saem da lista de despesas
# MAGIC -- Filtro alterado em 06/11/2020
# MAGIC WHERE NOT
# MAGIC (
# MAGIC   (D.cd_centro_responsabilidade LIKE '30_10%' OR D.cd_centro_responsabilidade LIKE '30_11%') -- gestao (10) e etd (11)
# MAGIC    AND
# MAGIC    (  (D.cd_centro_responsabilidade LIKE '302%' AND D.cd_entidade_nacional = 3) -- Tecnologia SENAI tem gestão e etd, no SESI não
# MAGIC             OR  D.cd_centro_responsabilidade LIKE '301%' -- defesa interesse SESI e SENAI
# MAGIC             OR  D.cd_centro_responsabilidade LIKE '304%' -- ssi SESI e SENAI
# MAGIC             OR  D.cd_centro_responsabilidade LIKE '305%' -- cultura SESI e SENAI
# MAGIC             OR  D.cd_centro_responsabilidade LIKE '306%' -- cooperacao SESI e SENAI
# MAGIC    )
# MAGIC );
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
For first load, this object is <200 records.
Keep in 1 parition.
Filter must use like and not startswith(), cause _ is a 'like' operator

"""

rateio_step07_01_despesa_etd_gestao_outros = rateio_step06_despesa\
.filter(
  ((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%")))
  & (((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3"))
     | (f.col("cd_centro_responsabilidade").like("301%"))
     | (f.col("cd_centro_responsabilidade").like("304%"))
     | (f.col("cd_centro_responsabilidade").like("305%"))
     | (f.col("cd_centro_responsabilidade").like("306%"))
    )
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional", 
  "cd_entidade_regional", 
  f.col("cd_centro_responsabilidade").substr(1, 3).alias("cd_centro_responsabilidade_n2"),
  (f.col("vl_despesa_total") +f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada")).alias("vl_desptot_etd_gestao_outros"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada")).alias("vl_despcor_etd_gestao_outros")
)\
.groupBy(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade_n2"
)\
.agg(
  f.sum("vl_desptot_etd_gestao_outros").alias("vl_desptot_etd_gestao_outros"),
  f.sum("vl_despcor_etd_gestao_outros").alias("vl_despcor_etd_gestao_outros")
)\
.fillna(0.0, subset=["vl_desptot_etd_gestao_outros", "vl_despcor_etd_gestao_outros"])\
.coalesce(1)

#rateio_step07_01_despesa_etd_gestao_outros.count()

# COMMAND ----------

#display(rateio_step07_01_despesa_etd_gestao_outros.filter(f.col("sg_entidade_regional").endswith("CE")))

# COMMAND ----------

"""
%sql

select * from rateio_step07_01_despesa_etd_gestao_outros
where right(sg_entidade_regional, 2) = 'CE'
"""

# COMMAND ----------

"""
In first load, this object is < 1k records, keep in 1 parition.

Thre should be no records here: 
  rateio_step07_02_despesa_outros.filter((f.col("vl_desptot_outros").isNull()) | (f.col("vl_despcor_outros").isNull()))
"""

rateio_step07_02_despesa_outros = rateio_step06_despesa\
.filter(
  (
    ((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3"))
    | (f.col("cd_centro_responsabilidade").like("301%"))
    | (f.col("cd_centro_responsabilidade").like("304%"))
    | (f.col("cd_centro_responsabilidade").like("305%"))
    | (f.col("cd_centro_responsabilidade").like("306%"))
  )
  & ~((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%")))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional", 
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada")).alias("vl_desptot_outros"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada")).alias("vl_despcor_outros")
)\
.fillna(0.0, subset=["vl_desptot_outros", "vl_despcor_outros"])\
.coalesce(1)

# COMMAND ----------

#display(rateio_step07_02_despesa_outros.filter(f.col("sg_entidade_regional").endswith("DF")))

# COMMAND ----------

"""
%sql

select * from rateio_step07_02_despesa_outros
where right(sg_entidade_regional, 2) = 'DF'
"""

# COMMAND ----------

"""
For the fisrt load, this object is <1k records, keep in 1 partition.
"""

window = Window.partitionBy(f.col("cd_entidade_regional"), f.col("cd_centro_responsabilidade_n2"))

rateio_step07_03_despesa_etd_gestao_outros_rateada = rateio_step07_02_despesa_outros\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  f.col("cd_centro_responsabilidade").substr(1,3).alias("cd_centro_responsabilidade_n2"),
  "vl_desptot_outros",
  "vl_despcor_outros"
)\
.fillna(0.0, subset=["vl_desptot_outros", "vl_despcor_outros"])\
.join(
  rateio_step07_01_despesa_etd_gestao_outros\
  .select(
    "cd_entidade_regional",
    "cd_centro_responsabilidade_n2",
    "vl_desptot_etd_gestao_outros",
    "vl_despcor_etd_gestao_outros"
  )\
  .fillna(0.0, subset=["vl_desptot_etd_gestao_outros", "vl_desptot_etd_gestao_outros"]),
  on=["cd_entidade_regional", "cd_centro_responsabilidade_n2"],
  how="inner"
)\
.withColumn("vl_desptot_etd_gestao_outros_rateada", (f.col("vl_desptot_outros") /f.sum("vl_desptot_outros").over(window)) * f.col("vl_desptot_etd_gestao_outros"))\
.withColumn("vl_despcor_etd_gestao_outros_rateada", (f.col("vl_despcor_outros") /f.sum("vl_despcor_outros").over(window)) * f.col("vl_despcor_etd_gestao_outros"))\
.drop(*["cd_centro_responsabilidade_n2", "vl_desptot_outros", "vl_despcor_outros", "vl_desptot_etd_gestao_outros", "vl_despcor_etd_gestao_outros"])\
.fillna(0.0, subset=["vl_desptot_etd_gestao_outros_rateada", "vl_despcor_etd_gestao_outros_rateada"])\
.coalesce(1)

#rateio_step07_03_despesa_etd_gestao_outros_rateada.count(), rateio_step07_03_despesa_etd_gestao_outros_rateada.rdd.getNumPartitions()

# COMMAND ----------

"""
display(rateio_step07_03_despesa_etd_gestao_outros_rateada.filter(f.col("sg_entidade_regional").endswith("SP")).select("sg_entidade_regional", "cd_centro_responsabilidade", "vl_desptot_etd_gestao_outros_rateada"))
"""

# COMMAND ----------

"""
%sql
select 
sg_entidade_regional, cd_centro_responsabilidade, vl_desptot_etd_gestao_outros_rateada as  msql
from rateio_step07_03_despesa_etd_gestao_outros_rateada
where right(sg_entidade_regional, 2) = 'SP'
"""

# COMMAND ----------

"""
Seems like everything is perfect.
"""
"""
display(
  rateio_step07_03_despesa_etd_gestao_outros_rateada\
  .filter(f.col("sg_entidade_regional").endswith("SP"))\
  .select("sg_entidade_regional", "cd_centro_responsabilidade", "vl_desptot_etd_gestao_outros_rateada")\
  .join(spark.sql("select sg_entidade_regional, cd_centro_responsabilidade, vl_desptot_etd_gestao_outros_rateada as  msql from rateio_step07_03_despesa_etd_gestao_outros_rateada where right(sg_entidade_regional, 2) = 'SP'"),
     on=["sg_entidade_regional", "cd_centro_responsabilidade"],
     how="inner")
)
"""

# COMMAND ----------

#display(rateio_step07_03_despesa_etd_gestao_outros_rateada.filter(f.col("sg_entidade_regional").endswith("CE")))

# COMMAND ----------

"""
%sql

select *
from rateio_step07_03_despesa_etd_gestao_outros_rateada
where right(sg_entidade_regional, 2) = 'CE'
"""

# COMMAND ----------

"""
For first load this object is <3k records. Keep it in one partition.
It will be used again in step08.
"""

rateio_step07_despesa = rateio_step06_despesa\
.filter(
  ~(
    ((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%")))
    & (((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3"))
       | (f.col("cd_centro_responsabilidade").like("301%"))
       | (f.col("cd_centro_responsabilidade").like("304%"))
       | (f.col("cd_centro_responsabilidade").like("305%"))
       | (f.col("cd_centro_responsabilidade").like("306%"))
      )
  )
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_despesa_total",
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_desptot_etd_gestao_educacao_rateada",
  "vl_despesa_corrente", 
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada",
  "vl_despcor_etd_gestao_educacao_rateada"
)\
.join(
  rateio_step07_03_despesa_etd_gestao_outros_rateada\
  .select(
    "cd_entidade_regional",
    "cd_centro_responsabilidade",
    "vl_desptot_etd_gestao_outros_rateada",
    "vl_despcor_etd_gestao_outros_rateada"
  ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=[
  "vl_despesa_total",
  "vl_despesa_corrente",
  "vl_desptot_vira_vida_rateada",
  "vl_despcor_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_despcor_olimpiada_rateada",
  "vl_desptot_etd_gestao_educacao_rateada",
  "vl_despcor_etd_gestao_educacao_rateada",
  "vl_desptot_etd_gestao_outros_rateada", 
  "vl_despcor_etd_gestao_outros_rateada"
])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step07_despesa.count()

#rateio_step07_despesa.rdd.getNumPartitions()

# COMMAND ----------

"""
Step06 is not needed anymore. Can be unpersisted.
"""
rateio_step06_despesa = rateio_step06_despesa.unpersist()

# COMMAND ----------

#rateio_step07_despesa.filter(f.col("sg_entidade_regional").endswith("SP")).count()

# COMMAND ----------

"""
127 records, nothing lost.
This is perfect! How can spark not be able to calculate this?
"""
"""
display(rateio_step07_despesa\
        .filter(f.col("sg_entidade_regional").endswith("SP"))
        .select("sg_entidade_regional", "cd_centro_responsabilidade", "vl_desptot_etd_gestao_outros_rateada")
        .join(spark.sql("select sg_entidade_regional, cd_centro_responsabilidade, vl_desptot_etd_gestao_outros_rateada as mssql from  rateio_step07_despesa where right(sg_entidade_regional, 2) = 'SP'"),
             on=["sg_entidade_regional", "cd_centro_responsabilidade"],
             how="left")
       )
"""

# COMMAND ----------

"""
display(rateio_step07_despesa\
        .filter(f.col("sg_entidade_regional").endswith("SP"))
        .select("sg_entidade_regional", "cd_centro_responsabilidade", "vl_desptot_etd_gestao_outros_rateada")
        .join(spark.sql("select sg_entidade_regional, cd_centro_responsabilidade, vl_desptot_etd_gestao_outros_rateada as mssql from  rateio_step07_despesa where right(sg_entidade_regional, 2) = 'SP'"),
             on=["sg_entidade_regional", "cd_centro_responsabilidade"],
             how="left")\
        .select("sg_entidade_regional", "vl_desptot_etd_gestao_outros_rateada", "mssql")\
        .groupBy("sg_entidade_regional")\
        .agg(
          f.sum("vl_desptot_etd_gestao_outros_rateada"),
          f.sum("mssql")
        )
       )
"""

# COMMAND ----------

"""
%sql

select 
sg_entidade_regional,
sum(vl_desptot_etd_gestao_outros_rateada)
from rateio_step07_despesa
where right(sg_entidade_regional, 2) = 'SP'
group by sg_entidade_regional
"""

# COMMAND ----------

"""
This is right!!!!!
"""

"""
display(rateio_step07_despesa\
.filter(f.col("sg_entidade_regional").endswith("SP"))
.select(
  "sg_entidade_regional",
  f.coalesce("vl_desptot_etd_gestao_outros_rateada", f.lit(0.0)).alias("vl_desptot_etd_gestao_outros_rateada"),
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada")).alias("despesa_total"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada")).alias("despesa_corrente")
)\
.groupBy("sg_entidade_regional")\
.agg(
  f.sum(f.col("vl_desptot_etd_gestao_outros_rateada")).alias("vl_desptot_etd_gestao_outros_rateada"),
  f.sum(f.col("despesa_total")).alias("despesa_total"),
  f.sum(f.col("despesa_corrente")).alias("despesa_corrente")
))
"""

# COMMAND ----------

"""
Era mesmo o valor que foi calculado aqui!
"""
"""
%sql

select substring(sg_entidade_regional, 1, 4),
sum(vl_desptot_etd_gestao_outros_rateada), 
SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada+vl_desptot_etd_gestao_outros_rateada)    
AS despesa_total,
SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada+vl_despcor_etd_gestao_outros_rateada) 
AS despesa_corrente
from rateio_step07_despesa 
WHERE right(sg_entidade_regional, 2) = 'SP'
group by substring(sg_entidade_regional, 1, 4)
"""

# COMMAND ----------

"""
VALIDAÇÂO - OK
"""
"""
%sql

SELECT substring(cd_centro_responsabilidade, 1, 3),
sum(vl_desptot_etd_gestao_outros_rateada),
sum(vl_despcor_etd_gestao_outros_rateada)
from rateio_step07_despesa 
where sg_entidade_regional = 'SESI-SP' and vl_desptot_etd_gestao_outros_rateada > 0 
group by substring(cd_centro_responsabilidade, 1, 3)
"""

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
display(
  rateio_step07_despesa\
  .filter((f.col("sg_entidade_regional") == 'SESI-SP') & (f.col("vl_desptot_etd_gestao_outros_rateada") > f.lit(0)))\
  .select(
    f.col("cd_centro_responsabilidade").substr(1,3).alias("cr"),
    "vl_desptot_etd_gestao_outros_rateada",
    "vl_despcor_etd_gestao_outros_rateada"
  )\
  .groupBy("cr")\
  .agg(
    f.sum("vl_desptot_etd_gestao_outros_rateada").alias("vl_desptot_etd_gestao_outros_rateada"),
    f.sum("vl_despcor_etd_gestao_outros_rateada").alias("vl_despcor_etd_gestao_outros_rateada")
  )
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP08: Obtem despesa Suporte Negócio rateada por Negócio (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rateio_step08_01_despesa_suporte_negocio;
# MAGIC CREATE TABLE rateio_step08_01_despesa_suporte_negocio AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada+vl_desptot_etd_gestao_outros_rateada)    
# MAGIC         AS vl_desptot_suporte_negocio,
# MAGIC         SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada+vl_despcor_etd_gestao_outros_rateada) 
# MAGIC         AS vl_despcor_suporte_negocio        
# MAGIC FROM  rateio_step07_despesa
# MAGIC WHERE cd_centro_responsabilidade LIKE '307%'
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step08_02_despesa_negocio;
# MAGIC CREATE TABLE rateio_step08_02_despesa_negocio AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada+vl_desptot_etd_gestao_outros_rateada) 
# MAGIC        AS vl_desptot_negocio,
# MAGIC        (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada+vl_despcor_etd_gestao_outros_rateada) 
# MAGIC        AS vl_despcor_negocio    
# MAGIC FROM rateio_step07_despesa 
# MAGIC WHERE cd_centro_responsabilidade LIKE '3%' -- negócios
# MAGIC AND   cd_centro_responsabilidade NOT LIKE '307%' -- retirando suprte negócio que será rateado
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step08_03_despesa_suporte_negocio_rateada;
# MAGIC CREATE TABLE rateio_step08_03_despesa_suporte_negocio_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_negocio / SUM(D.vl_desptot_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_suporte_negocio AS vl_desptot_suporte_negocio_rateada,
# MAGIC ( D.vl_despcor_negocio / SUM(D.vl_despcor_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_suporte_negocio AS vl_despcor_suporte_negocio_rateada
# MAGIC FROM rateio_step08_02_despesa_negocio D
# MAGIC INNER JOIN rateio_step08_01_despesa_suporte_negocio V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step08_despesa;
# MAGIC CREATE TABLE rateio_step08_despesa AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_despesa_total,
# MAGIC D.vl_desptot_vira_vida_rateada,
# MAGIC D.vl_desptot_olimpiada_rateada,
# MAGIC D.vl_desptot_etd_gestao_educacao_rateada,
# MAGIC D.vl_desptot_etd_gestao_outros_rateada,
# MAGIC NVL(R.vl_desptot_suporte_negocio_rateada, 0) AS vl_desptot_suporte_negocio_rateada,
# MAGIC D.vl_despesa_corrente,
# MAGIC D.vl_despcor_vira_vida_rateada,
# MAGIC D.vl_despcor_olimpiada_rateada,
# MAGIC D.vl_despcor_etd_gestao_educacao_rateada,
# MAGIC D.vl_despcor_etd_gestao_outros_rateada,
# MAGIC NVL(R.vl_despcor_suporte_negocio_rateada, 0) AS vl_despcor_suporte_negocio_rateada
# MAGIC 
# MAGIC FROM rateio_step07_despesa D
# MAGIC 
# MAGIC LEFT JOIN rateio_step08_03_despesa_suporte_negocio_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- suporte negócio foi rateado, sai da lista de despesas
# MAGIC WHERE D.cd_centro_responsabilidade NOT LIKE '307%';
# MAGIC ```

# COMMAND ----------

"""
For the first load this object is <200 records, keep in 1 partition.
"""

rateio_step08_01_despesa_suporte_negocio = rateio_step07_despesa\
.filter(f.col("cd_centro_responsabilidade").startswith("307"))\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada")).alias("vl_desptot_suporte_negocio"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada")).alias("vl_despcor_suporte_negocio")
)\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(
  f.sum("vl_desptot_suporte_negocio").alias("vl_desptot_suporte_negocio"),
  f.sum("vl_despcor_suporte_negocio").alias("vl_despcor_suporte_negocio")
)\
.coalesce(1)

#rateio_step08_01_despesa_suporte_negocio.rdd.getNumPartitions()

# COMMAND ----------

#assert  rateio_step08_01_despesa_suporte_negocio.count() == spark.sql("select count(*) from rateio_step08_01_despesa_suporte_negocio").collect()[0][0]

# COMMAND ----------

"""
For the first load, this object is <2k records. Keep it in 1 partition.
"""

rateio_step08_02_despesa_negocio = rateio_step07_despesa\
.filter(
  (f.col("cd_centro_responsabilidade").startswith("3"))
  & ~(f.col("cd_centro_responsabilidade").startswith("307"))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada")).alias("vl_desptot_negocio"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada")).alias("vl_despcor_negocio") 
)\
.coalesce(1)

#rateio_step08_02_despesa_negocio.count(), rateio_step08_02_despesa_negocio.rdd.getNumPartitions()

# COMMAND ----------

#assert rateio_step08_02_despesa_negocio.count() ==  spark.sql("select count(*) from rateio_step08_02_despesa_negocio").collect()[0][0]

# COMMAND ----------

"""
For the first load, this object is <2k records. Keep in 1 partition
"""
window = Window.partitionBy(f.col("cd_entidade_regional"))

rateio_step08_03_despesa_suporte_negocio_rateada = rateio_step08_02_despesa_negocio\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_desptot_negocio",
  "vl_despcor_negocio"
)\
.fillna(0.0, subset=["vl_desptot_negocio", "vl_despcor_negocio"])\
.join(
  rateio_step08_01_despesa_suporte_negocio\
  .select(
    "cd_entidade_regional",
    "vl_desptot_suporte_negocio",
    "vl_despcor_suporte_negocio"
  )\
  .fillna(0.0, subset=["vl_desptot_suporte_negocio", "vl_despcor_suporte_negocio"]),
  on=["cd_entidade_regional"],
  how="inner"
)\
.withColumn("vl_desptot_suporte_negocio_rateada", (f.col("vl_desptot_negocio") / f.sum("vl_desptot_negocio").over(window)) * f.col("vl_desptot_suporte_negocio"))\
.withColumn("vl_despcor_suporte_negocio_rateada", (f.col("vl_despcor_negocio") / f.sum("vl_despcor_negocio").over(window)) * f.col("vl_despcor_suporte_negocio"))\
.drop(*["vl_desptot_negocio", "vl_despcor_negocio", "vl_desptot_suporte_negocio", "vl_despcor_suporte_negocio"])\
.coalesce(1)

#rateio_step08_03_despesa_suporte_negocio_rateada.count(), rateio_step08_03_despesa_suporte_negocio_rateada.rdd.getNumPartitions()

# COMMAND ----------

#assert rateio_step08_03_despesa_suporte_negocio_rateada.count() == spark.sql("select count(*) from rateio_step08_03_despesa_suporte_negocio_rateada").collect()[0][0]

# COMMAND ----------

"""
For the fisrt load, this object is <3k records. Keep in 1 partition. 
Cache it. It will be used for step09
"""

rateio_step08_despesa = rateio_step07_despesa\
.filter(~(f.col("cd_centro_responsabilidade").startswith("307")))\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_despesa_total",
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_desptot_etd_gestao_educacao_rateada",
  "vl_desptot_etd_gestao_outros_rateada",
  "vl_despesa_corrente",
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada",
  "vl_despcor_etd_gestao_educacao_rateada",
  "vl_despcor_etd_gestao_outros_rateada",
)\
.join(
  rateio_step08_03_despesa_suporte_negocio_rateada\
  .select(
    "cd_entidade_regional",
    "cd_centro_responsabilidade",
    "vl_desptot_suporte_negocio_rateada",
    "vl_despcor_suporte_negocio_rateada"
  ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=[
  "vl_despesa_total", 
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_desptot_etd_gestao_educacao_rateada",
  "vl_desptot_etd_gestao_outros_rateada",
  "vl_despesa_corrente",
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada",
  "vl_despcor_etd_gestao_educacao_rateada",
  "vl_despcor_etd_gestao_outros_rateada",
  "vl_desptot_suporte_negocio_rateada",
  "vl_despcor_suporte_negocio_rateada"
])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step08_despesa.count()

#rateio_step08_despesa.count(), rateio_step08_despesa.rdd.getNumPartitions()

# COMMAND ----------

"""
Step07 is not needed anymore. Can be unpersisted.
"""
rateio_step07_despesa = rateio_step07_despesa.unpersist()

# COMMAND ----------

#assert rateio_step08_despesa.count() == spark.sql("select count(*) from rateio_step08_despesa").collect()[0][0]

# COMMAND ----------

"""
Validação - OK
"""
"""
%sql 

select substring(sg_entidade_regional, 1, 4),
sum(vl_desptot_suporte_negocio_rateada), 
sum(vl_despcor_suporte_negocio_rateada), 
SUM(vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada+vl_desptot_etd_gestao_outros_rateada+
vl_desptot_suporte_negocio_rateada) AS despesa_total,
SUM(vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada+vl_despcor_etd_gestao_outros_rateada+
vl_despcor_suporte_negocio_rateada) AS despesa_corrente
from rateio_step08_despesa 
WHERE right(sg_entidade_regional, 2) = 'SP'
group by substring(sg_entidade_regional, 1, 4)
"""

# COMMAND ----------

"""
Validação - OK
"""

"""
display(
  rateio_step08_despesa\
  .filter(f.col("sg_entidade_regional").endswith("SP"))\
  .select(
    "sg_entidade_regional",
    "vl_desptot_suporte_negocio_rateada",
    "vl_despcor_suporte_negocio_rateada",
    (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada") + f.col("vl_desptot_suporte_negocio_rateada")).alias("despesa_total"),
    (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada") + f.col("vl_despcor_suporte_negocio_rateada")).alias("despesa_corrente")
  )\
  .groupBy("sg_entidade_regional")\
  .agg(
    f.sum("vl_desptot_suporte_negocio_rateada").alias("vl_desptot_suporte_negocio_rateada"),
    f.sum("vl_despcor_suporte_negocio_rateada").alias("vl_despcor_suporte_negocio_rateada"),
    f.sum("despesa_total").alias("despesa_total"),
    f.sum("despesa_corrente").alias("despesa_corrente")
  )
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP09: Obtem despesa indireta rateada por Negócio (vale para SESI e SENAI)
# MAGIC -- Manutenção em 06/11/2020
# MAGIC --======================================================================================================
# MAGIC 
# MAGIC DROP TABLE   rateio_step09_01_despesa_indireta;
# MAGIC CREATE TABLE rateio_step09_01_despesa_indireta AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional, 
# MAGIC D.cd_entidade_regional, 
# MAGIC D.vl_desptot_indireta_gestao,
# MAGIC D.vl_desptot_indireta_desenv_institucional_bruta - T.vl_transferencia_reg as vl_desptot_indireta_desenv_institucional,
# MAGIC D.vl_desptot_indireta_apoio,
# MAGIC D.vl_despcor_indireta_gestao,
# MAGIC D.vl_despcor_indireta_desenv_institucional_bruta - T.vl_transferencia_reg as vl_despcor_indireta_desenv_institucional,
# MAGIC D.vl_despcor_indireta_apoio
# MAGIC 
# MAGIC FROM
# MAGIC (
# MAGIC 	SELECT 
# MAGIC         cd_entidade_nacional,
# MAGIC         sg_entidade_regional,
# MAGIC 		cd_entidade_regional,
# MAGIC         
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '1%'
# MAGIC             THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
# MAGIC                   +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
# MAGIC             ELSE 0 END) AS vl_desptot_indireta_gestao,
# MAGIC             
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '1%'
# MAGIC             THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
# MAGIC                   +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
# MAGIC             ELSE 0 END) AS vl_despcor_indireta_gestao,
# MAGIC             
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '2%'
# MAGIC             THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
# MAGIC                   +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
# MAGIC             ELSE 0 END) AS vl_desptot_indireta_desenv_institucional_bruta,
# MAGIC             
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '2%'
# MAGIC             THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
# MAGIC                   +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
# MAGIC             ELSE 0 END) AS vl_despcor_indireta_desenv_institucional_bruta,
# MAGIC             
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '4%'
# MAGIC             THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
# MAGIC                   +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
# MAGIC             ELSE 0 END) AS vl_desptot_indireta_apoio,
# MAGIC             
# MAGIC 	    SUM(CASE WHEN cd_centro_responsabilidade LIKE '4%'
# MAGIC             THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
# MAGIC                   +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
# MAGIC             ELSE 0 END) AS vl_despcor_indireta_apoio
# MAGIC             
# MAGIC 		FROM  rateio_step08_despesa
# MAGIC 		WHERE cd_centro_responsabilidade LIKE '1%' OR cd_centro_responsabilidade LIKE '2%' OR cd_centro_responsabilidade LIKE '4%'
# MAGIC 		GROUP BY 
# MAGIC         cd_entidade_nacional,
# MAGIC         sg_entidade_regional,
# MAGIC 		cd_entidade_regional    
# MAGIC ) D
# MAGIC LEFT JOIN rateio_step02_01_transferencia_reg T
# MAGIC ON D.cd_entidade_regional = T.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rateio_step09_02_despesa_negocio;
# MAGIC CREATE TABLE rateio_step09_02_despesa_negocio AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        
# MAGIC        -- em 06/11/2020 incuída a subtração da reversão nos cálculos abaixo
# MAGIC        ( D.vl_despesa_total
# MAGIC        + D.vl_desptot_vira_vida_rateada
# MAGIC        + D.vl_desptot_olimpiada_rateada
# MAGIC        + D.vl_desptot_etd_gestao_educacao_rateada
# MAGIC        + D.vl_desptot_etd_gestao_outros_rateada
# MAGIC        + D.vl_desptot_suporte_negocio_rateada) AS vl_desptot_negocio,
# MAGIC        
# MAGIC 	   ( D.vl_despesa_corrente
# MAGIC        + D.vl_despcor_vira_vida_rateada
# MAGIC        + D.vl_despcor_olimpiada_rateada
# MAGIC        + D.vl_despcor_etd_gestao_educacao_rateada
# MAGIC        + D.vl_despcor_etd_gestao_outros_rateada
# MAGIC        + D.vl_despcor_suporte_negocio_rateada) AS vl_despcor_negocio    
# MAGIC FROM rateio_step08_despesa 
# MAGIC WHERE cd_centro_responsabilidade LIKE '3%' -- negócios
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rateio_step09_03_despesa_indireta_rateada;
# MAGIC CREATE TABLE rateio_step09_03_despesa_indireta_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_desptot_negocio / SUM(D.vl_desptot_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_indireta_gestao AS vl_desptot_indireta_gestao_rateada,
# MAGIC ( D.vl_despcor_negocio / SUM(D.vl_despcor_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_indireta_gestao AS vl_despcor_indireta_gestao_rateada,
# MAGIC 
# MAGIC ( D.vl_desptot_negocio / SUM(D.vl_desptot_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_indireta_desenv_institucional
# MAGIC AS vl_desptot_indireta_desenv_institucional_rateada,
# MAGIC ( D.vl_despcor_negocio / SUM(D.vl_despcor_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_indireta_desenv_institucional 
# MAGIC AS vl_despcor_indireta_desenv_institucional_rateada,
# MAGIC 
# MAGIC ( D.vl_desptot_negocio / SUM(D.vl_desptot_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_desptot_indireta_apoio AS vl_desptot_indireta_apoio_rateada,
# MAGIC ( D.vl_despcor_negocio / SUM(D.vl_despcor_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_despcor_indireta_apoio AS vl_despcor_indireta_apoio_rateada
# MAGIC 
# MAGIC FROM rateio_step09_02_despesa_negocio D
# MAGIC INNER JOIN rateio_step09_01_despesa_indireta V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------------
# MAGIC -- Final: Append - Substituição Parcial Ano/Mês em trs_biz_fta_despesa_rateada_negocio
# MAGIC --------------------------------------------------------------------------------------
# MAGIC --DROP TABLE   rateio_step09_despesa;
# MAGIC --CREATE TABLE rateio_step09_despesa AS
# MAGIC SELECT
# MAGIC #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC #prm_data_corte AS dt_fechamento,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_despesa_total    AS vl_despesa_total, -- em 06/11/2020 incuída a subtração da reversão
# MAGIC D.vl_desptot_vira_vida_rateada,
# MAGIC D.vl_desptot_olimpiada_rateada,
# MAGIC D.vl_desptot_etd_gestao_educacao_rateada,
# MAGIC D.vl_desptot_etd_gestao_outros_rateada,
# MAGIC D.vl_desptot_suporte_negocio_rateada,
# MAGIC NVL(R.vl_desptot_indireta_gestao_rateada, 0)               AS vl_desptot_indireta_gestao_rateada,
# MAGIC NVL(R.vl_desptot_indireta_desenv_institucional_rateada, 0) AS vl_desptot_indireta_desenv_institucional_rateada,
# MAGIC NVL(R.vl_desptot_indireta_apoio_rateada, 0)                AS vl_desptot_indireta_apoio_rateada,
# MAGIC D.vl_despesa_corrente AS vl_despesa_corrente, -- em 06/11/2020 incuída a subtração da reversão
# MAGIC D.vl_despcor_vira_vida_rateada,
# MAGIC D.vl_despcor_olimpiada_rateada,
# MAGIC D.vl_despcor_etd_gestao_educacao_rateada,
# MAGIC D.vl_despcor_etd_gestao_outros_rateada,
# MAGIC D.vl_despcor_suporte_negocio_rateada,
# MAGIC NVL(R.vl_despcor_indireta_gestao_rateada, 0)                AS vl_despcor_indireta_gestao_rateada,
# MAGIC NVL(R.vl_despcor_indireta_desenv_institucional_rateada, 0)  AS vl_despcor_indireta_desenv_institucional_rateada,
# MAGIC NVL(R.vl_despcor_indireta_apoio_rateada, 0)                 AS vl_despcor_indireta_apoio_rateada
# MAGIC 
# MAGIC FROM rateio_step08_despesa D
# MAGIC 
# MAGIC LEFT JOIN rateio_step09_03_despesa_indireta_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- Na lista de despesas ficam só valores de negócio com os respectivos rateios
# MAGIC WHERE D.cd_centro_responsabilidade like '3%'-- somente negocio
# MAGIC ;
# MAGIC ```

# COMMAND ----------

"""
For the first load, this object is <100 records. Keep it in 1 partition.
"""

var_desptot = (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada") + f.col("vl_desptot_suporte_negocio_rateada"))
var_despcor = (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada") + f.col("vl_despcor_suporte_negocio_rateada"))

d = rateio_step08_despesa\
.filter(
  (f.col("cd_centro_responsabilidade").startswith("1")) 
  | (f.col("cd_centro_responsabilidade").startswith("2"))
  | (f.col("cd_centro_responsabilidade").startswith("4"))
)\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  f.when(
    f.col("cd_centro_responsabilidade").startswith("1"),
    var_desptot    
  ).otherwise(0.0).alias("vl_desptot_indireta_gestao"),
  f.when(
    f.col("cd_centro_responsabilidade").startswith("1"),
    var_despcor
  ).otherwise(0.0).alias("vl_despcor_indireta_gestao"),
  f.when(
    f.col("cd_centro_responsabilidade").startswith("2"),
    var_desptot
  ).otherwise(0.0).alias("vl_desptot_indireta_desenv_institucional_bruta"),
  f.when(
    f.col("cd_centro_responsabilidade").startswith("2"),
    var_despcor
  ).otherwise(0.0).alias("vl_despcor_indireta_desenv_institucional_bruta"),
  f.when(
    f.col("cd_centro_responsabilidade").startswith("4"),
    var_desptot
  ).otherwise(0.0).alias("vl_desptot_indireta_apoio"),
  f.when(
    f.col("cd_centro_responsabilidade").startswith("4"),
    var_despcor
  ).otherwise(0.0).alias("vl_despcor_indireta_apoio")
)\
.groupBy(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional"
)\
.agg(
  f.sum("vl_desptot_indireta_gestao").alias("vl_desptot_indireta_gestao"),
  f.sum("vl_despcor_indireta_gestao").alias("vl_despcor_indireta_gestao"),
  f.sum("vl_desptot_indireta_desenv_institucional_bruta").alias("vl_desptot_indireta_desenv_institucional_bruta"),
  f.sum("vl_despcor_indireta_desenv_institucional_bruta").alias("vl_despcor_indireta_desenv_institucional_bruta"),
  f.sum("vl_desptot_indireta_apoio").alias("vl_desptot_indireta_apoio"),
  f.sum("vl_despcor_indireta_apoio").alias("vl_despcor_indireta_apoio")
)\
.fillna(0.0, subset=[
  "vl_desptot_indireta_gestao",
  "vl_despcor_indireta_gestao",
  "vl_desptot_indireta_desenv_institucional_bruta",
  "vl_despcor_indireta_desenv_institucional_bruta",
  "vl_desptot_indireta_apoio",
  "vl_despcor_indireta_apoio"
])\
.coalesce(1)

#d.count(), d.rdd.getNumPartitions()

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
%sql

SELECT 
    cd_entidade_nacional,
    sg_entidade_regional,
    cd_entidade_regional,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '1%'
        THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
              +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
        ELSE 0 END) AS vl_desptot_indireta_gestao,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '1%'
        THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
              +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
        ELSE 0 END) AS vl_despcor_indireta_gestao,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '2%'
        THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
              +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
        ELSE 0 END) AS vl_desptot_indireta_desenv_institucional_bruta,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '2%'
        THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
              +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
        ELSE 0 END) AS vl_despcor_indireta_desenv_institucional_bruta,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '4%'
        THEN (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada
              +vl_desptot_etd_gestao_outros_rateada+vl_desptot_suporte_negocio_rateada) 
        ELSE 0 END) AS vl_desptot_indireta_apoio,

    SUM(CASE WHEN cd_centro_responsabilidade LIKE '4%'
        THEN (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada
              +vl_despcor_etd_gestao_outros_rateada+vl_despcor_suporte_negocio_rateada)
        ELSE 0 END) AS vl_despcor_indireta_apoio

    FROM  rateio_step08_despesa
    WHERE cd_centro_responsabilidade LIKE '1%' OR cd_centro_responsabilidade LIKE '2%' OR cd_centro_responsabilidade LIKE '4%'
    GROUP BY 
    cd_entidade_nacional,
    sg_entidade_regional,
    cd_entidade_regional    
"""

# COMMAND ----------

"""
For first load, this object is <100 records. Keep it in 1 partition.
"""

rateio_step09_01_despesa_indireta = d\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional", 
  "cd_entidade_regional",
  "vl_desptot_indireta_gestao",
  "vl_desptot_indireta_desenv_institucional_bruta",
  "vl_desptot_indireta_apoio",
  "vl_despcor_indireta_gestao",
  "vl_despcor_indireta_desenv_institucional_bruta",
  "vl_despcor_indireta_apoio"
)\
.join(
  rateio_step02_01_transferencia_reg\
  .select(
    "cd_entidade_regional",
    "vl_transferencia_reg"
  ),
  on=["cd_entidade_regional"],
  how="left"
)\
.fillna(0.0, subset=[
  "vl_desptot_indireta_desenv_institucional_bruta",
  "vl_desptot_indireta_apoio",
  "vl_despcor_indireta_gestao",
  "vl_despcor_indireta_desenv_institucional_bruta",
  "vl_despcor_indireta_apoio",
  "vl_transferencia_reg"
])\
.withColumn("vl_desptot_indireta_desenv_institucional", f.col("vl_desptot_indireta_desenv_institucional_bruta") - f.col("vl_transferencia_reg"))\
.withColumn("vl_despcor_indireta_desenv_institucional", f.col("vl_despcor_indireta_desenv_institucional_bruta") - f.col("vl_transferencia_reg"))\
.drop(*["vl_desptot_indireta_desenv_institucional_bruta", "vl_despcor_indireta_desenv_institucional_bruta", "vl_transferencia_reg"])\
.coalesce(1)\
.cache()

# Activate cache
rateio_step09_01_despesa_indireta.count()

# COMMAND ----------

rateio_step02_01_transferencia_reg = rateio_step02_01_transferencia_reg.unpersist()

# COMMAND ----------

#assert rateio_step09_01_despesa_indireta.count(), spark.sql("select count(*) from rateio_step09_01_despesa_indireta").colllect()[0][0]

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
%sql
select
cd_entidade_regional,
cd_entidade_nacional,
sg_entidade_regional,
vl_desptot_indireta_gestao,
vl_desptot_indireta_apoio,
vl_despcor_indireta_gestao,
vl_despcor_indireta_apoio,
vl_desptot_indireta_desenv_institucional,
vl_despcor_indireta_desenv_institucional
from rateio_step09_01_despesa_indireta
"""

# COMMAND ----------

rateio_step09_02_despesa_negocio = rateio_step08_despesa\
.filter(f.col("cd_centro_responsabilidade").startswith("3"))\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada") + f.col("vl_desptot_suporte_negocio_rateada")).alias("vl_desptot_negocio"),
  (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada") + f.col("vl_despcor_suporte_negocio_rateada")).alias("vl_despcor_negocio")  
)\
.fillna(0.0, subset=["vl_desptot_negocio", "vl_despcor_negocio"])\
.coalesce(1)

# COMMAND ----------

#assert rateio_step09_02_despesa_negocio.count() == spark.sql("select count(*) from rateio_step09_02_despesa_negocio").collect()[0][0]

# COMMAND ----------

"""
For fisrt load this object is < 2k records. Keep it in 1 partition.
"""

window = Window.partitionBy(f.col("cd_entidade_regional"))

rateio_step09_03_despesa_indireta_rateada = rateio_step09_02_despesa_negocio\
.select(
  "cd_entidade_nacional",
  "sg_entidade_regional",
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  "vl_desptot_negocio",
  "vl_despcor_negocio"
)\
.fillna(0.0, subset=["vl_desptot_negocio", "vl_despcor_negocio"])\
.join(
  rateio_step09_01_despesa_indireta
  .select(
    "cd_entidade_regional",
    "vl_desptot_indireta_gestao",
    "vl_despcor_indireta_gestao",
    "vl_desptot_indireta_desenv_institucional",
    "vl_despcor_indireta_desenv_institucional",
    "vl_desptot_indireta_apoio",
    "vl_despcor_indireta_apoio"
  )\
  .fillna(0.0, subset=[
    "vl_desptot_indireta_gestao",
    "vl_despcor_indireta_gestao",
    "vl_desptot_indireta_desenv_institucional",
    "vl_despcor_indireta_desenv_institucional",
    "vl_desptot_indireta_apoio",
    "vl_despcor_indireta_apoio"
  ]),
  on=["cd_entidade_regional"],
  how="inner"
)\
.fillna(0.0, subset=[
  "vl_desptot_negocio",
  "vl_despcor_negocio",
  "vl_desptot_indireta_gestao",
  "vl_despcor_indireta_gestao",
  "vl_desptot_indireta_desenv_institucional",
  "vl_despcor_indireta_desenv_institucional",
  "vl_desptot_indireta_apoio",
  "vl_despcor_indireta_apoio"
])\
.withColumn("vl_desptot_indireta_gestao_rateada", (f.col("vl_desptot_negocio") / f.sum("vl_desptot_negocio").over(window)) * f.col("vl_desptot_indireta_gestao"))\
.withColumn("vl_despcor_indireta_gestao_rateada", (f.col("vl_despcor_negocio") / f.sum("vl_despcor_negocio").over(window)) * f.col("vl_despcor_indireta_gestao"))\
.withColumn("vl_desptot_indireta_desenv_institucional_rateada", (f.col("vl_desptot_negocio") / f.sum("vl_desptot_negocio").over(window)) * f.col("vl_desptot_indireta_desenv_institucional"))\
.withColumn("vl_despcor_indireta_desenv_institucional_rateada", (f.col("vl_despcor_negocio") / f.sum("vl_despcor_negocio").over(window)) * f.col("vl_despcor_indireta_desenv_institucional"))\
.withColumn("vl_desptot_indireta_apoio_rateada", (f.col("vl_desptot_negocio") / f.sum("vl_desptot_negocio").over(window)) * f.col("vl_desptot_indireta_apoio"))\
.withColumn("vl_despcor_indireta_apoio_rateada", (f.col("vl_despcor_negocio") / f.sum("vl_despcor_negocio").over(window)) * f.col("vl_despcor_indireta_apoio"))\
.drop(
  *["vl_desptot_negocio",
    "vl_despcor_negocio",
    "vl_desptot_indireta_gestao",
    "vl_despcor_indireta_gestao",
    "vl_desptot_indireta_desenv_institucional",
    "vl_despcor_indireta_desenv_institucional",
    "vl_desptot_indireta_apoio",
    "vl_despcor_indireta_apoio"
   ]
)\
.coalesce(1)

# COMMAND ----------

#assert rateio_step09_03_despesa_indireta_rateada.count() == spark.sql("select count(*) from rateio_step09_03_despesa_indireta_rateada").collect()[0][0]

# COMMAND ----------

"""
For the first load, this object is <3k, keep it in 1 partition.
Cache it!
"""

var_step08_num_columns = [
  "vl_despesa_total",
  "vl_desptot_vira_vida_rateada",
  "vl_desptot_olimpiada_rateada",
  "vl_desptot_etd_gestao_educacao_rateada",
  "vl_desptot_etd_gestao_outros_rateada",
  "vl_desptot_suporte_negocio_rateada",
  "vl_despesa_corrente",
  "vl_despcor_vira_vida_rateada",
  "vl_despcor_olimpiada_rateada",
  "vl_despcor_etd_gestao_educacao_rateada",
  "vl_despcor_etd_gestao_outros_rateada",
  "vl_despcor_suporte_negocio_rateada"
]

var_step09_num_columns = [
  "vl_desptot_indireta_gestao_rateada",
  "vl_desptot_indireta_desenv_institucional_rateada",
  "vl_desptot_indireta_apoio_rateada",
  "vl_despcor_indireta_gestao_rateada",
  "vl_despcor_indireta_desenv_institucional_rateada",
  "vl_despcor_indireta_apoio_rateada"
]

sink = rateio_step08_despesa\
.filter(f.col("cd_centro_responsabilidade").startswith("3"))\
.select(
  "cd_entidade_regional",
  "cd_centro_responsabilidade",
  *var_step08_num_columns  
)\
.join(
  rateio_step09_03_despesa_indireta_rateada\
  .select(
    "cd_entidade_regional",
    "cd_centro_responsabilidade",
    *var_step09_num_columns
  ),
  on=["cd_entidade_regional", "cd_centro_responsabilidade"],
  how="left"
)\
.fillna(0.0, subset=list(set(var_step08_num_columns + var_step09_num_columns)))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.coalesce(1)\
.cache()

# Activate cache
sink.count()

# COMMAND ----------

"""
Step08 and step09_01 are not needed anymore. Unpersist them.
"""
rateio_step09_01_despesa_indireta = rateio_step09_01_despesa_indireta.unpersist()
rateio_step08_despesa = rateio_step08_despesa.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
%sql

SELECT
sg_entidade_regional,
cd_entidade_regional,
SUM(despesa_corrente) AS despesa_corrente,
SUM(CASE WHEN cd_centro_responsabilidade like '30303%' 
         OR cd_centro_responsabilidade like '30304%'
         THEN despesa_corrente END) as educacao_profissional,
SUM(CASE WHEN cd_centro_responsabilidade like '303%' 
         AND cd_centro_responsabilidade not like '30303%' 
         AND cd_centro_responsabilidade not like '30304%' 
         THEN despesa_corrente END) as educacao_outros,
SUM(CASE WHEN cd_centro_responsabilidade like '302%' THEN despesa_corrente END) as tecnol_inov,
SUM(CASE WHEN cd_centro_responsabilidade like '3%'
         AND substring(cd_centro_responsabilidade, 1, 3) not in ('303','302')
         THEN despesa_corrente END) as outros_servicos
     
FROM
(
   select sg_entidade_regional, cd_centro_responsabilidade, cd_entidade_regional,
   (vl_despesa_total+vl_desptot_vira_vida_rateada+vl_desptot_olimpiada_rateada+vl_desptot_etd_gestao_educacao_rateada+vl_desptot_etd_gestao_outros_rateada+
   vl_desptot_suporte_negocio_rateada+vl_desptot_indireta_gestao_rateada+vl_desptot_indireta_desenv_institucional_rateada+vl_desptot_indireta_apoio_rateada) AS despesa_total,
   (vl_despesa_corrente+vl_despcor_vira_vida_rateada+vl_despcor_olimpiada_rateada+vl_despcor_etd_gestao_educacao_rateada+vl_despcor_etd_gestao_outros_rateada+
   vl_despcor_suporte_negocio_rateada+vl_despcor_indireta_gestao_rateada+vl_despcor_indireta_desenv_institucional_rateada+vl_despcor_indireta_apoio_rateada) AS despesa_corrente
   from rateio_step09_despesa  
   where sg_entidade_regional in ('SENAI-SP','SENAI-DF','SENAI-RJ','SENAI-AC','SENAI-RS')
)
group by sg_entidade_regional, cd_entidade_regional
"""

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
display(
  sink\
  .filter(f.col("cd_entidade_regional").isin(['1117285','1117266','1117260','1117278','1117282']))\
  .select(
    "cd_entidade_regional",
    "cd_centro_responsabilidade",
    (f.col("vl_despesa_total") + f.col("vl_desptot_vira_vida_rateada") + f.col("vl_desptot_olimpiada_rateada") + f.col("vl_desptot_etd_gestao_educacao_rateada") + f.col("vl_desptot_etd_gestao_outros_rateada") + f.col("vl_desptot_suporte_negocio_rateada") + f.col("vl_desptot_indireta_gestao_rateada") + f.col("vl_desptot_indireta_desenv_institucional_rateada") + f.col("vl_desptot_indireta_apoio_rateada")).alias("despesa_total"),
    (f.col("vl_despesa_corrente") + f.col("vl_despcor_vira_vida_rateada") + f.col("vl_despcor_olimpiada_rateada") + f.col("vl_despcor_etd_gestao_educacao_rateada") + f.col("vl_despcor_etd_gestao_outros_rateada") + f.col("vl_despcor_suporte_negocio_rateada") + f.col("vl_despcor_indireta_gestao_rateada") + f.col("vl_despcor_indireta_desenv_institucional_rateada") + f.col("vl_despcor_indireta_apoio_rateada")).alias("despesa_corrente")  
  )\
  .fillna(0.0, subset=["despesa_total", "despesa_corrente"])
  .select(
    "cd_entidade_regional",
    "despesa_corrente",
    f.when(
      f.col("cd_centro_responsabilidade").startswith("30303") | f.col("cd_centro_responsabilidade").startswith("30304"),
      f.col("despesa_corrente")
    ).otherwise(f.lit(0.0)).alias("educacao_profissional"),
    f.when(
      f.col("cd_centro_responsabilidade").startswith("303") 
      & ~(f.col("cd_centro_responsabilidade").startswith("30303")) 
      & ~(f.col("cd_centro_responsabilidade").startswith("30304")),
      f.col("despesa_corrente")
    ).otherwise(f.lit(0.0)).alias("educacao_outros"),
    f.when(
      f.col("cd_centro_responsabilidade").startswith("302"),
      f.col("despesa_corrente")
    ).otherwise(f.lit(0.0)).alias("tecnol_inov"),
    f.when(
      f.col("cd_centro_responsabilidade").startswith("3") 
      & ~(f.col("cd_centro_responsabilidade").substr(1,3).isin(["303", "302"])),
      f.col("despesa_corrente")
    ).otherwise(f.lit(0.0)).alias("outros_servicos")
  )\
  .groupBy("cd_entidade_regional")\
  .agg(
    f.sum("despesa_corrente").alias("despesa_corrente"),
    f.sum("educacao_profissional").alias("educacao_profissional"),
    f.sum("educacao_outros").alias("educacao_outros"),
    f.sum("tecnol_inov").alias("tecnol_inov"),
    f.sum("outros_servicos").alias("outros_servicos"),
  )
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields

# COMMAND ----------

sink = tcf.add_control_fields(sink, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Write in ADLS

# COMMAND ----------

sink.write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC Clear cache

# COMMAND ----------

sink = sink.unpersist()

# COMMAND ----------

