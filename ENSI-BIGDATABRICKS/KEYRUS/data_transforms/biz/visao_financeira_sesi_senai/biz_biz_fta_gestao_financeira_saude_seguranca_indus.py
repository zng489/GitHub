# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	biz_biz_fta_gestao_financeira_saude_seguranca_indus
# MAGIC Tabela/Arquivo Origem	/biz/orcamento/fta_despesa_rateada_negocio
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_gestao_financeira_saude_seguranca_indus
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de despesas do SSI_SESI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
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
var_tables = {"origins":["/mtd/corp/centro_responsabilidade_caracteristica", "/orcamento/fta_despesa_rateada_negocio", "/corporativo/dim_hierarquia_entidade_regional"],
  "destination":"/orcamento/fta_gestao_financeira_saude_seguranca_indus", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/biz_biz_fta_gestao_financeira_saude_seguranca_indus"}
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
"adf_trigger_time":"2020-09-30T13:00:00.8532201Z",
"adf_trigger_type":"Manual"
}

var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}
"""

# COMMAND ----------

"""
USE THIS FOR DEV PURPOSES
#var_sink = "{}{}{}".format(var_adls_uri, "/tmp/dev" + var_dls["folders"]["business"], var_tables["destination"]) 
"""
var_src_crc =  "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["origins"][0])
var_src_drn, var_src_her =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], t) for t in var_tables["origins"][-2:]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
var_src_crc, var_src_drn, var_src_her, var_sink

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
# MAGIC ========================================================================================================================================================
# MAGIC -- STEP01: Obtém todos os CRs Ativos, mesmo que não possuam despesas, que fazem parte do rateio das contas de gestão e ETD (304__10, 304__11)
# MAGIC --========================================================================================================================================================
# MAGIC DROP TABLE rateio_step01_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step01_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT distinct a.*,
# MAGIC CASE
# MAGIC WHEN b.cd_centro_responsabilidade_n3 IS NULL THEN 0 ELSE 1 END fl_indica_rateio
# MAGIC FROM
# MAGIC (
# MAGIC SELECT SUBSTRING(cd_centro_responsabilidade, 1, 5) AS cd_centro_responsabilidade_n3, count(*) AS qt_centro_responsabilidade
# MAGIC FROM centro_responsabilidade_caracteristica
# MAGIC WHERE cd_ano_referencia = #prm_ano_fechamento
# MAGIC --cd_ano_referencia = '20'
# MAGIC AND cd_centro_responsabilidade LIKE'304%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '304__10%' 
# MAGIC AND cd_centro_responsabilidade NOT LIKE '304__11%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040802%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040803%'
# MAGIC and cd_centro_responsabilidade not like '30410%'  ---- incluir
# MAGIC and cd_centro_responsabilidade not like '30411%'  ---- incluir
# MAGIC GROUP BY SUBSTRING(cd_centro_responsabilidade, 1, 5) 
# MAGIC ORDER BY SUBSTRING(cd_centro_responsabilidade, 1, 5)
# MAGIC ) a
# MAGIC LEFT JOIN
# MAGIC (SELECT SUBSTRING(cd_centro_responsabilidade, 1, 5) AS cd_centro_responsabilidade_n3
# MAGIC FROM centro_responsabilidade_caracteristica
# MAGIC WHERE cd_ano_referencia = #prm_ano_fechamento
# MAGIC -- cd_ano_referencia = '20'
# MAGIC AND (cd_centro_responsabilidade LIKE '304__10%'
# MAGIC OR cd_centro_responsabilidade   LIKE '304__11%'
# MAGIC OR cd_centro_responsabilidade   LIKE '3040802%' 
# MAGIC OR cd_centro_responsabilidade   LIKE '3040803%') 
# MAGIC ) b ON a.cd_centro_responsabilidade_n3 = b.cd_centro_responsabilidade_n3 )
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
I'll try to keep the same notation as documentation when possible

This is a small table, in test fase it has only ~200 records and ~3k in mem cache. Keep in 1 partition.
"""
var_centro_responsabilidade_caracteristica_columns = ["cd_centro_responsabilidade", "cd_ano_referencia"]

centro_responsabilidade_caracteristica = spark.read.parquet(var_src_crc)\
.select(*var_centro_responsabilidade_caracteristica_columns)\
.filter(f.col("cd_ano_referencia") == f.substring(f.lit(var_parameters["prm_ano_fechamento"]), 3, 2).cast("int"))\
.drop("cd_ano_referencia")\
.coalesce(1)\
.cache()

# Activate cache
centro_responsabilidade_caracteristica.count()

# COMMAND ----------

"""
I'm testing for the first load and the object is really small: 5 records and ~500B in mem cache. Can be kept in 1 partition
"""
a = centro_responsabilidade_caracteristica\
.filter((f.col("cd_centro_responsabilidade").startswith("304")) &\
        ~(f.col("cd_centro_responsabilidade").like("304__10%")) &\
        ~(f.col("cd_centro_responsabilidade").like("304__11%")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040802")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040803")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("30410")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("30411")))\
.withColumn("cd_centro_responsabilidade_n3_a", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.drop("cd_centro_responsabilidade")\
.groupBy("cd_centro_responsabilidade_n3_a")\
.agg(f.count(f.col("cd_centro_responsabilidade_n3_a")).alias("qt_centro_responsabilidade"))\
.coalesce(1)\
.cache()

# Activate cache
a.count()

# COMMAND ----------

"""
I'm testing for the first load and the object is really small: 8 records and ~370B in mem cache. Can be kept in 1 partition
"""
b = centro_responsabilidade_caracteristica\
.filter((f.col("cd_centro_responsabilidade").like("304__10%")) |\
        (f.col("cd_centro_responsabilidade").like("304__11%")) |\
        (f.col("cd_centro_responsabilidade").startswith("3040802")) |\
        (f.col("cd_centro_responsabilidade").startswith("3040803")))\
.withColumn("cd_centro_responsabilidade_n3_b", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.drop("cd_centro_responsabilidade")\
.coalesce(1)\
.cache()

# Activate cache
b.count()

# COMMAND ----------

"""
This is a really small table, in test fase it has only 5 records and ~600B in mem cache. Keep in 1 partition.

"""

rateio_step01_despesa_cr_ssi = a\
.join(b, 
      a.cd_centro_responsabilidade_n3_a == b.cd_centro_responsabilidade_n3_b,
      "left")\
.withColumn("fl_indica_rateio", f.when(f.col("cd_centro_responsabilidade_n3_b").isNull(), f.lit(0).cast("int"))\
                                   .otherwise(f.lit(1).cast("int")))\
.withColumnRenamed("cd_centro_responsabilidade_n3_a", "cd_centro_responsabilidade_n3")\
.drop("cd_centro_responsabilidade_n3_b")\
.distinct()\
.orderBy("cd_centro_responsabilidade_n3")\
.coalesce(1)\
.cache()

# Activate cache
rateio_step01_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step01_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step01_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source a and b are not needed anymore.
These objects can be removed from cache.
"""
a = a.unpersist()
b = b.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

display(rateio_step01_despesa_cr_ssi)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 02
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC - STEP02  ---- Totaliza todas as despesa corrente por CR 
# MAGIC --========================================================================================================================================================
# MAGIC 
# MAGIC DROP TABLE rateio_step02_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step02_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT 
# MAGIC --e.cd_entidade_nacional,  ---- Retirado 30/09/2020
# MAGIC --e.sg_entidade_regional,  ---- Retirado 30/09/2020
# MAGIC e.cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC   SUM(vl_despesa_corrente)                  AS vl_despesa_corrente
# MAGIC , SUM(vl_despcor_suporte_negocio_rateada)   AS vl_despcor_suporte_negocio 
# MAGIC , SUM(vl_despcor_etd_gestao_outros_rateada) AS vl_despcor_etd_gestao_outros 
# MAGIC 
# MAGIC ----- INCLUIDO 30/09
# MAGIC , SUM(vl_despcor_indireta_gestao_rateada)   AS vl_despcor_indireta_gestao -- vl_despesa_corrente_indireta_gestao
# MAGIC , SUM(vl_despcor_indireta_desenv_institucional_rateada) AS vl_despcor_indireta_desenv_institucional -- vl_despesa_corrente_indireta_desenv_institucional
# MAGIC , SUM(vl_despcor_indireta_apoio_rateada)    AS vl_despcor_indireta_apoio -- vl_despesa_corrente_indireta_apoio
# MAGIC ----- INCLUIDO 30/09
# MAGIC 
# MAGIC FROM fta_despesa_rateada_negocio o
# MAGIC INNER JOIN dim_hierarquia_entidade_regional e 
# MAGIC ON (o.cd_entidade_regional = e.cd_entidade_regional)
# MAGIC 
# MAGIC WHERE e.cd_entidade_nacional = 2 -- SESI
# MAGIC AND  (cd_centro_responsabilidade like '304%')
# MAGIC AND cd_ano_fechamento = #prm_ano_fechamento AND cd_mes_fechamento <= #prm_mes_fechamento
# MAGIC GROUP BY 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC cd_centro_responsabilidade
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has 30 records. Keep in 1 partition.
"""

var_her_columns = ["cd_entidade_nacional", "cd_entidade_regional"]

dim_hierarquia_entidade_regional = spark.read.parquet(var_src_her)\
.select(*var_her_columns)\
.filter(f.col("cd_entidade_nacional") == 2)\
.drop("cd_entidade_nacional")\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only ~550 records and ~30K in mem cache. Keep in 1 partition.
"""

var_drn_columns = ["cd_entidade_regional", "cd_centro_responsabilidade", "vl_despesa_corrente", "vl_despcor_etd_gestao_outros_rateada", "vl_despcor_suporte_negocio_rateada", "vl_despcor_indireta_gestao_rateada", "vl_despcor_indireta_desenv_institucional_rateada", "vl_despcor_indireta_apoio_rateada", "cd_ano_fechamento", "cd_mes_fechamento"]

rateio_step02_despesa_cr_ssi = spark.read.parquet(var_src_drn)\
.select(*var_drn_columns)\
.filter((f.col("cd_centro_responsabilidade").startswith("304")) &\
        (f.col("cd_ano_fechamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.col("cd_mes_fechamento") == f.lit(var_parameters["prm_mes_fechamento"]).cast("int")))\
.join(dim_hierarquia_entidade_regional, ["cd_entidade_regional"], "inner")\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(f.sum("vl_despesa_corrente").alias("vl_despesa_corrente"),
     f.sum("vl_despcor_suporte_negocio_rateada").alias("vl_despcor_suporte_negocio"),
     f.sum("vl_despcor_etd_gestao_outros_rateada").alias("vl_despcor_etd_gestao_outros"),
     f.sum("vl_despcor_indireta_gestao_rateada").alias("vl_despcor_indireta_gestao"),
     f.sum("vl_despcor_indireta_desenv_institucional_rateada").alias("vl_despcor_indireta_desenv_institucional"),
     f.sum("vl_despcor_indireta_apoio_rateada").alias("vl_despcor_indireta_apoio"))\
.fillna(0, subset=["vl_despesa_corrente",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_etd_gestao_outros",
                   "vl_despcor_indireta_gestao",
                   "vl_despcor_indireta_desenv_institucional",
                   "vl_despcor_indireta_apoio"])\
.coalesce(1)\
.cache()

#Activate cache
rateio_step02_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step02_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step02_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step02_despesa_cr_ssi
where cd_entidade_regional in (2, 1117258)
order by cd_entidade_regional, cd_centro_responsabilidade

display(rateio_step02_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int"), "cd_centro_responsabilidade"))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 03
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP03  ---- Totaliza as despesa por grupo de CR exceto Gestão (304__10) e Apoio (304__11)
# MAGIC --========================================================================================================================================================
# MAGIC 
# MAGIC DROP TABLE rateio_step03_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step03_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT 
# MAGIC --cd_entidade_nacional, ---- Retirado 30/09/2020
# MAGIC --sg_entidade_regional, ---- Retirado 30/09/2020
# MAGIC cd_entidade_regional,
# MAGIC SUBSTRING(cd_centro_responsabilidade, 1, 5) AS cd_centro_responsabilidade_n3,
# MAGIC   SUM(vl_despesa_corrente)          AS vl_despesa_corrente
# MAGIC , SUM(vl_despcor_suporte_negocio)   AS vl_despcor_suporte_negocio 
# MAGIC , SUM(vl_despcor_etd_gestao_outros) AS vl_despcor_etd_gestao_outros 
# MAGIC FROM  rateio_step02_despesa_cr_ssi o
# MAGIC WHERE cd_entidade_nacional = 2 -- SESI
# MAGIC AND cd_centro_responsabilidade NOT LIKE '304__10%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '304__11%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040802%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040803%'
# MAGIC GROUP BY
# MAGIC --cd_entidade_nacional, ---- Retirado 30/09/2020
# MAGIC --sg_entidade_regional, ---- Retirado 30/09/2020
# MAGIC cd_entidade_regional,
# MAGIC SUBSTRING(o.cd_centro_responsabilidade, 1, 5) 
# MAGIC )
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has 103 records. Keep in 1 partition.
"""

rateio_step03_despesa_cr_ssi = rateio_step02_despesa_cr_ssi\
.filter(~(f.col("cd_centro_responsabilidade").like('304__10%')) &\
        ~(f.col("cd_centro_responsabilidade").like('304__11%')) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040802")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040803")))\
.withColumn("cd_centro_responsabilidade_n3", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.drop("cd_centro_responsabilidade")\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade_n3")\
.agg(f.sum("vl_despesa_corrente").alias("vl_despesa_corrente"),
     f.sum("vl_despcor_suporte_negocio").alias("vl_despcor_suporte_negocio"),
     f.sum("vl_despcor_etd_gestao_outros").alias("vl_despcor_etd_gestao_outros"))\
.fillna(0, subset=["vl_despesa_corrente",
                   "vl_despcor_suporte_negocio",
                   "vl_despcor_etd_gestao_outros"])\
.coalesce(1)

# COMMAND ----------

#assert  rateio_step03_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step03_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step03_despesa_cr_ssi  --- usar no calculo da despesa rateada
where cd_entidade_regional in (2, 1117258)
order by cd_entidade_regional -- , cd_centro_responsabilidade

display(rateio_step03_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int")))
"""


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 04
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP04  ---- Sumariza e rateia por todos os CRs ativos as despesas de Apoio e Gestão (304__10, 304__11) por grupo de CRs
# MAGIC -- OBS: O valor deste rateio será utilizado quanto o total das despesas for igual zero na hora de gravar a tabela.
# MAGIC --========================================================================================================================================================
# MAGIC 
# MAGIC --DROP TABLE rateio_step04_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step04_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT
# MAGIC a.cd_entidade_regional,
# MAGIC a.cd_centro_responsabilidade_n3,
# MAGIC a.vl_despesa_corrente, --- 28/09
# MAGIC --vl_despcor_suporte_negocio,
# MAGIC --vl_despcor_etd_gestao_outros,
# MAGIC 
# MAGIC ---- Alterado 30/09 os dois campos abaixo:
# MAGIC ---qt_centro_responsabilidade,
# MAGIC ---vl_despesa_corrente / qt_centro_responsabilidade AS vl_despesa_corrente_rateada_n3
# MAGIC CASE
# MAGIC WHEN b.vl_despesa_corrente <> 0 
# MAGIC   THEN 0 
# MAGIC   ELSE qt_centro_responsabilidade
# MAGIC   END AS qt_centro_responsabilidade,
# MAGIC 
# MAGIC CASE
# MAGIC WHEN b.vl_despesa_corrente <> 0 
# MAGIC   THEN a.vl_despesa_corrente 
# MAGIC   ELSE a.vl_despesa_corrente / qt_centro_responsabilidade 
# MAGIC   END AS vl_despesa_corrente_n3_rateada
# MAGIC 
# MAGIC FROM
# MAGIC (
# MAGIC SELECT
# MAGIC cd_entidade_regional, 
# MAGIC SUBSTRING(cd_centro_responsabilidade, 1, 5) AS cd_centro_responsabilidade_n3,
# MAGIC SUM(vl_despesa_corrente)          AS vl_despesa_corrente,
# MAGIC SUM(vl_despcor_suporte_negocio)   AS vl_despcor_suporte_negocio,
# MAGIC SUM(vl_despcor_etd_gestao_outros) AS vl_despcor_etd_gestao_outros
# MAGIC FROM rateio_step02_despesa_cr_ssi
# MAGIC WHERE (cd_centro_responsabilidade LIKE '304__10%'
# MAGIC OR cd_centro_responsabilidade LIKE '304__11%'
# MAGIC OR cd_centro_responsabilidade LIKE '3040802%'
# MAGIC OR cd_centro_responsabilidade LIKE '3040803%')
# MAGIC GROUP BY cd_entidade_regional, 
# MAGIC SUBSTRING(cd_centro_responsabilidade, 1, 5)
# MAGIC )
# MAGIC a
# MAGIC --- NOVO ----
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC select 
# MAGIC cd_entidade_regional, 
# MAGIC substring(cd_centro_responsabilidade, 1, 5) as cd_centro_responsabilidade_n3,
# MAGIC sum(vl_despesa_corrente) as vl_despesa_corrente,
# MAGIC sum(vl_despcor_suporte_negocio) as vl_despcor_suporte_negocio,
# MAGIC sum(vl_despcor_etd_gestao_outros) as vl_despcor_etd_gestao_outros
# MAGIC from rateio_step02_despesa_cr_ssi
# MAGIC where (cd_centro_responsabilidade NOT LIKE '304__10%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '304__11%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040802%'
# MAGIC AND cd_centro_responsabilidade NOT LIKE '3040803%')
# MAGIC group by cd_entidade_regional, 
# MAGIC substring(cd_centro_responsabilidade, 1, 5)
# MAGIC )
# MAGIC b ON a.cd_entidade_regional = b.cd_entidade_regional AND a.cd_centro_responsabilidade_n3 = b.cd_centro_responsabilidade_n3
# MAGIC 
# MAGIC LEFT JOIN rateio_step01_despesa_cr_ssi r ON a.cd_centro_responsabilidade_n3 = r.cd_centro_responsabilidade_n3 
# MAGIC )
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has 59 records. Keep in 1 partition.
"""

a = rateio_step02_despesa_cr_ssi\
.filter((f.col("cd_centro_responsabilidade").like('304__10%')) |\
        (f.col("cd_centro_responsabilidade").like('304__11%')) |\
        (f.col("cd_centro_responsabilidade").startswith("3040802")) |\
        (f.col("cd_centro_responsabilidade").startswith("3040803")))\
.withColumn("cd_centro_responsabilidade_n3", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.drop("cd_centro_responsabilidade", "cd_entidade_nacional", "sg_entidade_regional", "vl_despcor_suporte_negocio", "vl_despcor_etd_gestao_outros")\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade_n3")\
.agg(f.sum("vl_despesa_corrente").alias("vl_despesa_corrente_a"))\
.fillna(0, subset=["vl_despesa_corrente_a"])\
.coalesce(1)

# COMMAND ----------

"""
This is a really small table, in test fase it has 59 records. Keep in 1 partition.
"""

b = rateio_step02_despesa_cr_ssi\
.filter(~(f.col("cd_centro_responsabilidade").like('304__10%')) &\
        ~(f.col("cd_centro_responsabilidade").like('304__11%')) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040802")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040803")))\
.withColumn("cd_centro_responsabilidade_n3", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.drop("cd_centro_responsabilidade", "cd_entidade_nacional", "sg_entidade_regional", "vl_despcor_suporte_negocio", "vl_despcor_etd_gestao_outros")\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade_n3")\
.agg(f.sum("vl_despesa_corrente").alias("vl_despesa_corrente_b"))\
.fillna(0, subset=["vl_despesa_corrente_b"])\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only 59 records and 2.4B in mem cache. Keep in 1 partition.
"""

rateio_step04_despesa_cr_ssi = a\
.join(b, ["cd_entidade_regional", "cd_centro_responsabilidade_n3"], "left")\
.join(rateio_step01_despesa_cr_ssi, ["cd_centro_responsabilidade_n3"], "left")\
.withColumn("vl_despesa_corrente_n3_rateada", 
            f.when(f.col("vl_despesa_corrente_b") != 0, f.col("vl_despesa_corrente_a"))\
            .otherwise(f.when(f.col("qt_centro_responsabilidade") != 0, 
                             f.col("vl_despesa_corrente_a") / f.col("qt_centro_responsabilidade"))\
                      .otherwise(f.lit(0))))\
.withColumn("qt_centro_responsabilidade",
           f.when(f.col("vl_despesa_corrente_b") != 0, f.lit(0))\
           .otherwise(f.col("qt_centro_responsabilidade")))\
.withColumnRenamed("vl_despesa_corrente_a", "vl_despesa_corrente")\
.drop("fl_indica_rateio", "vl_despesa_corrente_b")\
.fillna(0, subset=["vl_despesa_corrente",
                   "vl_despesa_corrente_n3_rateada"])\
.coalesce(1)\
.cache()

#Activate cache
rateio_step04_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step04_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step04_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step01_despesa_cr_ssi is not needed anymore.
This object can be removed from cache.
"""
rateio_step01_despesa_cr_ssi = rateio_step01_despesa_cr_ssi.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step04_despesa_cr_ssi
where cd_entidade_regional in (2,1117258 )
order by cd_entidade_regional --, cd_centro_responsabilidade_n3

display(rateio_step04_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int")))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 05
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP05  ---- Calcula valor despesas correntes diretas - rateada (para os grupos de despesas que tem apoio e gestão à ratear)
# MAGIC -- OBS: O valor deste rateio será utilizado quanto o total das despesas for diferente zero na hora de gravar a tabela.
# MAGIC --========================================================================================================================================================
# MAGIC DROP TABLE rateio_step05_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step05_despesa_cr_ssi
# MAGIC --select *
# MAGIC from 
# MAGIC (
# MAGIC SELECT cr.cd_entidade_regional,
# MAGIC cr.cd_centro_responsabilidade,
# MAGIC NVL(rtapo.vl_despesa_corrente_n3_rateada,0) AS vl_despesa_corrente_rateada,
# MAGIC NVL(cr.vl_despesa_corrente, 0) + (NVL(rtapo.vl_despesa_corrente, 0) * (NVL(cr.vl_despesa_corrente, 0)/NVL(totgr.vl_despesa_corrente, 0))) AS vl_despesa_corrente_direta
# MAGIC 
# MAGIC FROM rateio_step02_despesa_cr_ssi cr
# MAGIC INNER JOIN rateio_step03_despesa_cr_ssi totgr ON cr.cd_entidade_regional = totgr.cd_entidade_regional and substring(cr.cd_centro_responsabilidade, 1, 5) = totgr.cd_centro_responsabilidade_n3
# MAGIC LEFT JOIN rateio_step04_despesa_cr_ssi rtapo ON rtapo.cd_entidade_regional = totgr.cd_entidade_regional and substring(cr.cd_centro_responsabilidade, 1, 5) = rtapo.cd_centro_responsabilidade_n3
# MAGIC WHERE cr.cd_centro_responsabilidade NOT LIKE '304__10%'
# MAGIC AND cr.cd_centro_responsabilidade NOT LIKE '304__11%'
# MAGIC AND cr.cd_centro_responsabilidade NOT LIKE '3040802%'
# MAGIC AND cr.cd_centro_responsabilidade NOT LIKE '3040803%'
# MAGIC ORDER BY cr.cd_centro_responsabilidade
# MAGIC ) a
# MAGIC UNION
# MAGIC (
# MAGIC SELECT a.cd_entidade_regional,
# MAGIC        b.cd_centro_responsabilidade,
# MAGIC        vl_despesa_corrente_n3_rateada AS vl_despesa_corrente_rateada,
# MAGIC        vl_despesa_corrente_n3_rateada AS vl_despesa_corrente_direta
# MAGIC FROM rateio_step04_despesa_cr_ssi a
# MAGIC INNER JOIN 
# MAGIC (SELECT cd_centro_responsabilidade, substring(cd_centro_responsabilidade, 1, 5) AS cd_centro_responsabilidade_n3
# MAGIC FROM centro_responsabilidade_caracteristica
# MAGIC --where cd_ano_referencia = #prm_ano_fechamento
# MAGIC WHERE cd_ano_referencia = '20'
# MAGIC AND cd_centro_responsabilidade like '304%'
# MAGIC AND cd_centro_responsabilidade not like '304__10%' 
# MAGIC AND cd_centro_responsabilidade not like '304__11%'
# MAGIC AND cd_centro_responsabilidade not like '3040802%'
# MAGIC AND cd_centro_responsabilidade not like '3040803%'
# MAGIC AND cd_centro_responsabilidade not like '30410%'  
# MAGIC AND cd_centro_responsabilidade not like '30411%'
# MAGIC ) b ON a.cd_centro_responsabilidade_n3 = b.cd_centro_responsabilidade_n3
# MAGIC WHERE qt_centro_responsabilidade <> 0 )
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only 475 records. Keep in 1 partition.
"""

a = rateio_step02_despesa_cr_ssi\
.select("cd_entidade_regional", "cd_centro_responsabilidade", f.col("vl_despesa_corrente").alias("vl_despesa_corrente_cr"))\
.filter(~(f.col("cd_centro_responsabilidade").like('304__10%')) &\
        ~(f.col("cd_centro_responsabilidade").like('304__11%')) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040802")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("3040803")))\
.withColumn("cd_centro_responsabilidade_n3", f.substring(f.col("cd_centro_responsabilidade"), 1, 5))\
.join(rateio_step03_despesa_cr_ssi\
      .select("cd_entidade_regional", "cd_centro_responsabilidade_n3", f.col("vl_despesa_corrente").alias("vl_despesa_corrente_totgr")),
      ["cd_entidade_regional", "cd_centro_responsabilidade_n3"], "inner")\
.join(rateio_step04_despesa_cr_ssi\
      .select("cd_entidade_regional", "cd_centro_responsabilidade_n3", "vl_despesa_corrente_n3_rateada", f.col("vl_despesa_corrente").alias("vl_despesa_corrente_rtapo"))
      , ["cd_entidade_regional", "cd_centro_responsabilidade_n3"], "left")\
.fillna(0, subset=["vl_despesa_corrente_cr",
                   "vl_despesa_corrente_totgr",
                   "vl_despesa_corrente_n3_rateada",
                   "vl_despesa_corrente_rtapo"])\
.withColumn("vl_despesa_corrente_direta", 
           f.col("vl_despesa_corrente_cr") + 
           (f.col("vl_despesa_corrente_rtapo") *
            f.when(f.col("vl_despesa_corrente_totgr") != 0,
                   f.col("vl_despesa_corrente_cr") / f.col("vl_despesa_corrente_totgr"))\
            .otherwise(f.lit(0))))\
.withColumnRenamed("vl_despesa_corrente_n3_rateada", "vl_despesa_corrente_rateada")\
.drop("cd_centro_responsabilidade_n3", "vl_despesa_corrente_cr", "vl_despesa_corrente_totgr", "vl_despesa_corrente_rtapo")\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only 28 records. Keep in 1 partition.
"""

b = rateio_step04_despesa_cr_ssi\
.select("cd_entidade_regional", 
        "cd_centro_responsabilidade_n3",
        f.col("vl_despesa_corrente_n3_rateada").alias("vl_despesa_corrente_rateada"),
        f.col("vl_despesa_corrente_n3_rateada").alias("vl_despesa_corrente_direta"),
        "qt_centro_responsabilidade")\
.filter(f.col("qt_centro_responsabilidade") != 0)\
.join(centro_responsabilidade_caracteristica\
      .filter((f.col("cd_centro_responsabilidade").startswith("304")) &\
              ~(f.col("cd_centro_responsabilidade").like("304__10%")) &\
              ~(f.col("cd_centro_responsabilidade").like("304__11%")) &\
              ~(f.col("cd_centro_responsabilidade").startswith("3040802")) &\
              ~(f.col("cd_centro_responsabilidade").startswith("3040803")) &\
              ~(f.col("cd_centro_responsabilidade").startswith("30410")) &\
              ~(f.col("cd_centro_responsabilidade").startswith("30411")))\
      .withColumn("cd_centro_responsabilidade_n3", f.substring(f.col("cd_centro_responsabilidade"), 1, 5)),
      ["cd_centro_responsabilidade_n3"],
      "inner")\
.drop("cd_centro_responsabilidade_n3", "qt_centro_responsabilidade")\
.coalesce(1)


# COMMAND ----------

"""
This is a small table, in test fase it has only 503 records and ~11K in mem cache. Keep in 1 partition.
"""

rateio_step05_despesa_cr_ssi = a.union(b.select(a.columns))\
.coalesce(1)\
.cache()

# Activate cache
rateio_step05_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step05_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step05_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step04_despesa_cr_ssi and centro_responsabilidade_caracteristica are not needed anymore.
These objects can be removed from cache.
"""
rateio_step04_despesa_cr_ssi = rateio_step04_despesa_cr_ssi.unpersist()
centro_responsabilidade_caracteristica = centro_responsabilidade_caracteristica.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step05_despesa_cr_ssi
where cd_entidade_regional in (2,1117258 )
order by cd_entidade_regional, cd_centro_responsabilidade

display(rateio_step05_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int"), "cd_centro_responsabilidade"))
"""


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 06
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP06  ---- Une as tabelas com os centros de responsabilidade com despesas rateadas e com centros de responsabilidades com valores de gestão e apoio 
# MAGIC -- sem despesas à ratear.
# MAGIC --========================================================================================================================================================
# MAGIC --DROP TABLE rateio_step06_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step06_despesa_cr_ssi
# MAGIC 
# MAGIC (
# MAGIC SELECT
# MAGIC  a.cd_entidade_regional, 
# MAGIC  a.cd_centro_responsabilidade,
# MAGIC  vl_despesa_corrente_direta ,
# MAGIC  vl_despesa_corrente_total ,
# MAGIC  vl_despesa_corrente_direta / vl_despesa_corrente_total AS pc_despesa_corrente
# MAGIC FROM
# MAGIC (
# MAGIC SELECT
# MAGIC  cd_entidade_regional,
# MAGIC  cd_centro_responsabilidade,
# MAGIC  vl_despesa_corrente_direta
# MAGIC FROM rateio_step05_despesa_cr_ssi a
# MAGIC )  a           
# MAGIC INNER JOIN  (SELECT cd_entidade_regional, SUM(vl_despesa_corrente_direta) AS vl_despesa_corrente_total
# MAGIC              FROM rateio_step05_despesa_cr_ssi
# MAGIC              GROUP BY cd_entidade_regional) b ON a.cd_entidade_regional = b.cd_entidade_regional)
# MAGIC 
# MAGIC --where a.cd_entidade_regional in (2,1117258 )
# MAGIC --order by a.cd_entidade_regional, a.cd_centro_responsabilidade
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only 28 records. Keep in 1 partition.
"""

b = rateio_step05_despesa_cr_ssi\
.select("cd_entidade_regional", "vl_despesa_corrente_direta")\
.groupBy("cd_entidade_regional")\
.agg(f.sum("vl_despesa_corrente_direta").alias("vl_despesa_corrente_total"))\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only 503 records and ~15K in mem cache. Keep in 1 partition.
"""

rateio_step06_despesa_cr_ssi = rateio_step05_despesa_cr_ssi\
.select("cd_entidade_regional", "cd_centro_responsabilidade", "vl_despesa_corrente_direta")\
.join(b, ["cd_entidade_regional"], "inner")\
.fillna(0, subset=["vl_despesa_corrente_direta",
                   "vl_despesa_corrente_total"])\
.withColumn("pc_despesa_corrente", 
            f.when(f.col("vl_despesa_corrente_total") != 0,
                   f.col("vl_despesa_corrente_direta") / f.col("vl_despesa_corrente_total"))\
            .otherwise(f.lit(0)))\
.coalesce(1)\
.cache()

# Activate cache
rateio_step06_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step06_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step06_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step05_despesa_cr_ssi is not needed anymore.
This objects can be removed from cache.
"""
rateio_step05_despesa_cr_ssi = rateio_step05_despesa_cr_ssi.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step06_despesa_cr_ssi
where cd_entidade_regional in (2,1117258 )
order by cd_entidade_regional, cd_centro_responsabilidade

display(rateio_step06_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int"), "cd_centro_responsabilidade"))
"""


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 07
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP07  ---- Calcula todos os totais de Gestão e ETD, despesas correntes rateadas do negócio (suporte ao negócio), gestão
# MAGIC -- Desenvolvimento institucional (deduzidas as Transferencias regulamentares) e apoio
# MAGIC --========================================================================================================================================================
# MAGIC DROP TABLE rateio_step07_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step07_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC sum(vl_despcor_suporte_negocio)   AS vl_despcor_suporte_negocio,
# MAGIC sum(vl_despcor_etd_gestao_outros) AS vl_despcor_etd_gestao_outros,
# MAGIC sum(vl_despcor_indireta_gestao)   AS vl_despcor_indireta_gestao,
# MAGIC sum(vl_despcor_indireta_desenv_institucional) AS vl_despcor_indireta_desenv_institucional,
# MAGIC sum(vl_despcor_indireta_apoio)    AS vl_despcor_indireta_apoio
# MAGIC from rateio_step02_despesa_cr_ssi 
# MAGIC group by 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional
# MAGIC )
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only 28 records and ~2K in mem cache. Keep in 1 partition.
"""

rateio_step07_despesa_cr_ssi = rateio_step02_despesa_cr_ssi\
.drop("vl_despesa_corrente")\
.groupBy("cd_entidade_regional")\
.agg(f.sum("vl_despcor_suporte_negocio").alias("vl_despcor_suporte_negocio"),
     f.sum("vl_despcor_etd_gestao_outros").alias("vl_despcor_etd_gestao_outros"),
     f.sum("vl_despcor_indireta_gestao").alias("vl_despcor_indireta_gestao"),
     f.sum("vl_despcor_indireta_desenv_institucional").alias("vl_despcor_indireta_desenv_institucional"),
     f.sum("vl_despcor_indireta_apoio").alias("vl_despcor_indireta_apoio"))\
.coalesce(1)\
.cache()

# Activate cache
rateio_step07_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step07_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step07_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step02_despesa_cr_ssi is not needed anymore.
This object can be removed from cache.
"""
rateio_step02_despesa_cr_ssi = rateio_step02_despesa_cr_ssi.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step07_despesa_cr_ssi
where cd_entidade_regional in (2,1117258 )
order by cd_entidade_regional --, cd_centro_responsabilidade

display(rateio_step07_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int")))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 08
# MAGIC ```
# MAGIC --========================================================================================================================================================
# MAGIC -- STEP08  ---- Calcula o percentual de rateio, e os seguintes valores: gestão e ETD, Rateadas (suporte ao negócio)
# MAGIC -- Despesas correntes de gestão + Desenvolvimento Institucional + apoio para gravar tabela com as despesas de SSI rateadas
# MAGIC --========================================================================================================================================================
# MAGIC 
# MAGIC DROP TABLE rateio_step08_despesa_cr_ssi; 
# MAGIC CREATE TABLE rateio_step08_despesa_cr_ssi
# MAGIC (
# MAGIC SELECT
# MAGIC a.cd_entidade_regional,
# MAGIC a.cd_centro_responsabilidade,
# MAGIC --pc_despesa_corrente AS pc_despcor_ssi_rateada, --Retirado em 22/10
# MAGIC vl_despesa_corrente_direta AS vl_despcor_ssi_rateada,
# MAGIC vl_despcor_etd_gestao_outros * pc_despesa_corrente  AS vl_despcor_etd_gestao_outros_ssi_rateada,
# MAGIC vl_despcor_suporte_negocio * pc_despesa_corrente AS vl_despcor_suporte_negocio_ssi_rateada,
# MAGIC -- (b.vl_despcor_indireta_gestao + b.vl_despcor_indireta_desenv_institucional + b.vl_despcor_indireta_apoio) * pc_despesa_corrente AS vl_despcor_gestao_desenvinst_apoio_ssi_rateada --- eliminado em 22/10
# MAGIC (b.vl_despcor_indireta_gestao) * pc_despesa_corrente AS vl_despcor_gestao_ssi_rateada,   --- inclusao 22/10
# MAGIC (b.vl_despcor_indireta_desenv_institucional) * pc_despesa_corrente AS vl_despcor_desenv_institucional_ssi_rateada,   --- inclusao 22/10
# MAGIC (b.vl_despcor_indireta_apoio) * pc_despesa_corrente AS vl_despcor_apoio_ssi_rateada   --- inclusao 22/10
# MAGIC FROM rateio_step06_despesa_cr_ssi a 
# MAGIC INNER JOIN rateio_step07_despesa_cr_ssi b ON a.cd_entidade_regional = b.cd_entidade_regional
# MAGIC ORDER BY cd_centro_responsabilidade
# MAGIC )
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only 503 records and ~23K in mem cache. Keep in 1 partition.
"""

rateio_step08_despesa_cr_ssi = rateio_step06_despesa_cr_ssi\
.join(rateio_step07_despesa_cr_ssi, ["cd_entidade_regional"], "inner")\
.fillna(0, subset=["vl_despcor_etd_gestao_outros", 
                   "vl_despcor_suporte_negocio", 
                   "vl_despcor_indireta_gestao", 
                   "vl_despcor_indireta_desenv_institucional", 
                   "vl_despcor_indireta_apoio",
                   "vl_despesa_corrente_direta",
                   "pc_despesa_corrente"])\
.withColumn("vl_despcor_etd_gestao_outros_ssi_rateada",
            f.col("vl_despcor_etd_gestao_outros") * f.col("pc_despesa_corrente"))\
.withColumn("vl_despcor_suporte_negocio_ssi_rateada",
            f.col("vl_despcor_suporte_negocio") * f.col("pc_despesa_corrente"))\
.withColumn("vl_despcor_indireta_gestao_ssi_rateada",
            f.col("vl_despcor_indireta_gestao") * f.col("pc_despesa_corrente"))\
.withColumn("vl_despcor_indireta_desenv_institucional_ssi_rateada",
            f.col("vl_despcor_indireta_desenv_institucional") * f.col("pc_despesa_corrente"))\
.withColumn("vl_despcor_indireta_apoio_ssi_rateada",
            f.col("vl_despcor_indireta_apoio") * f.col("pc_despesa_corrente"))\
.withColumnRenamed("vl_despesa_corrente_direta", "vl_despcor_ssi_rateada")\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.drop("pc_despesa_corrente","vl_despesa_corrente_total", "vl_despcor_etd_gestao_outros", "vl_despcor_suporte_negocio", "vl_despcor_indireta_gestao", "vl_despcor_indireta_desenv_institucional", "vl_despcor_indireta_apoio")\
.coalesce(1)\
.cache()

# Activate cache
rateio_step08_despesa_cr_ssi.count()

# COMMAND ----------

#assert  rateio_step08_despesa_cr_ssi.count() == spark.sql("select count(*) from rateio_step08_despesa_cr_ssi").collect()[0][0]

# COMMAND ----------

"""
Source rateio_step06_despesa_cr_ssi and rateio_step07_despesa_cr_ssi are not needed anymore.
These objects can be removed from cache.
"""
rateio_step06_despesa_cr_ssi = rateio_step06_despesa_cr_ssi.unpersist()
rateio_step07_despesa_cr_ssi = rateio_step07_despesa_cr_ssi.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

select * from rateio_step08_despesa_cr_ssi
where cd_entidade_regional in (2,1117258 )
order by cd_entidade_regional, cd_centro_responsabilidade

display(rateio_step08_despesa_cr_ssi\
        .filter(f.col("cd_entidade_regional").isin(2, 1117258))\
        .orderBy(f.col("cd_entidade_regional").cast("int"), "cd_centro_responsabilidade"))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz

# COMMAND ----------

fta_gestao_financeira_ssi = tcf.add_control_fields(rateio_step08_despesa_cr_ssi, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjusting schema

# COMMAND ----------

var_column_type_map = {"cd_entidade_regional": "int",
                       "cd_centro_responsabilidade": "string",
                       "vl_despcor_ssi_rateada": "decimal(18,6)",                       
                       "vl_despcor_etd_gestao_outros_ssi_rateada": "decimal(18,6)",
                       "vl_despcor_suporte_negocio_ssi_rateada": "decimal(18,6)",
                       "vl_despcor_indireta_gestao_ssi_rateada": "decimal(18,6)",                       
                       "vl_despcor_indireta_desenv_institucional_ssi_rateada": "decimal(18,6)",                       
                       "vl_despcor_indireta_apoio_ssi_rateada": "decimal(18,6)",                       
                       "cd_ano_fechamento": "int",
                       "cd_mes_fechamento": "int",
                       "dt_fechamento": "date"}

for c in var_column_type_map:
  fta_gestao_financeira_ssi = fta_gestao_financeira_ssi.withColumn(c, fta_gestao_financeira_ssi[c].cast(var_column_type_map[c]))

# COMMAND ----------

#display(rateio_step08_despesa_cr_ssi)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

fta_gestao_financeira_ssi.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

"""
Source rateio_step08_despesa_cr_ssi is not needed anymore.
This object can be removed from cache.
"""
rateio_step08_despesa_cr_ssi = rateio_step08_despesa_cr_ssi.unpersist()

# COMMAND ----------

