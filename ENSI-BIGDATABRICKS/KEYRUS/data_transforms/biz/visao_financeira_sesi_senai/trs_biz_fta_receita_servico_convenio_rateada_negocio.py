# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_receita_servico_convenio_rateada_negocio
# MAGIC Tabela/Arquivo Origem	/trs/evt/orcamento_nacional_realizado
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_receita_servico_convenio_rateada_negocio
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores rateados de receitas de serviços e convênio por Negócios SESI e SENAI
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Tiago Shin / Marcela
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
var_tables = {"origins":["/evt/orcamento_nacional_realizado", "/mtd/corp/entidade_regional", "/mtd/corp/conta_contabil"],
  "destination":"/orcamento/fta_receita_servico_convenio_rateada_negocio", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/trs_biz_fta_receita_servico_convenio_rateada_negocio"}
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
var_src_onr, var_src_reg, var_src_cc =  ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], t) for t in var_tables["origins"]]
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
var_src_onr, var_src_reg, var_src_cc, var_sink

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
# MAGIC -- STEP01: Leitura inicial Receita orçamento SESI e SENAI na camada Trusted
# MAGIC --====================================================================================================================================
# MAGIC DROP TABLE rrateio_step01_receita;
# MAGIC CREATE TABLE rrateio_step01_receita AS
# MAGIC SELECT 
# MAGIC e.cd_entidade_nacional,
# MAGIC e.sg_entidade_regional,
# MAGIC e.cd_entidade_regional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC SUM(o.vl_lancamento) AS vl_receita_servico_convenio
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN entidade_regional e 
# MAGIC ON (o.cd_entidade_regional_erp_oltp = e.cd_entidade_regional_erp_oltp)
# MAGIC 
# MAGIC WHERE (   o.cd_conta_contabil LIKE '410104%%' -- Receita Serviço
# MAGIC        OR o.cd_conta_contabil LIKE '410202%%' -- Receita Convênio
# MAGIC       )
# MAGIC AND   e.cd_entidade_nacional IN (2, 3) -- SESI ou SENAI
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
"""
var_o_columns = ["cd_centro_responsabilidade", "cd_conta_contabil", "vl_lancamento", "cd_entidade_regional_erp_oltp", "dt_lancamento", "dt_ultima_atualizacao_oltp", "dt_lancamento"]

o = spark.read.parquet(var_src_onr)\
.select(*var_o_columns)\
.fillna(0.0, subset=["vl_lancamento"])\
.filter(((f.col("cd_conta_contabil").startswith("410104")) | (f.col("cd_conta_contabil").startswith("410202"))) &\
        (f.year("dt_lancamento") == f.lit(var_parameters["prm_ano_fechamento"]).cast("int")) &\
        (f.month("dt_lancamento") <= f.lit(var_parameters["prm_mes_fechamento"]).cast("int")) &\
        (f.col("dt_ultima_atualizacao_oltp") <= f.lit(var_parameters["prm_data_corte"]).cast("timestamp")))\
.drop(*["dt_lancamento", "dt_ultima_atualizacao_oltp"])

# COMMAND ----------

"""
# e
This object will be reused a lot. It is really small: < 100 records. 
Cache it and keep in 1 partition
"""

var_e_columns = ["cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_entidade_regional_erp_oltp"]

e = spark.read.parquet(var_src_reg)\
.select(*var_e_columns)\
.filter(f.col("cd_entidade_nacional").isin(2,3))\
.coalesce(1)

# COMMAND ----------

"""
I'm testing for the first load and the object is really small:
  ~1,3k records, 25KB in mem cached. Can be kept in 1 partition
"""
rrateio_step01_receita = o.join(e, on=["cd_entidade_regional_erp_oltp"], how="inner")\
.drop(*["cd_entidade_regional_erp_oltp"])\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade")\
.agg(
  f.sum(f.col("vl_lancamento")).alias("vl_receita_servico_convenio")
)\
.fillna(0, subset=["vl_receita_servico_convenio"])\
.coalesce(1)\
.cache()

# Activate cache
rrateio_step01_receita.count()

# COMMAND ----------

#assert  rrateio_step01_receita.count() == spark.sql("select count(*) from rrateio_step01_receita").collect()[0][0]

# COMMAND ----------

"""
Validation: ok

SESI
sum(vl_receita_servico_convenio) = 808721264.7899995
sum(vl_receita_servico_convenio) (planilha) = 808.721.264,79

display(rrateio_step01_receita.filter(f.col("sg_entidade_regional").startswith("SESI")).agg(f.sum("vl_receita_servico_convenio")))

SENAI
sum(vl_receita_servico_convenio) = 452025908.9200001
sum(vl_receita_servico_convenio) (planilha) = 452.025.908,92


display(rrateio_step01_receita.filter(f.col("sg_entidade_regional").startswith("SENAI")).agg(f.sum("vl_receita_servico_convenio")))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 02
# MAGIC ```
# MAGIC --=======================================================================================================
# MAGIC -- STEP02: Obtem receita vira vida rateada SESI na educação básica e continuada
# MAGIC --=======================================================================================================*/
# MAGIC DROP TABLE   rrateio_step02_01_receita_vira_vida;
# MAGIC CREATE TABLE rrateio_step02_01_receita_vira_vida AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_receita_servico_convenio)    AS vl_receita_vira_vida
# MAGIC FROM rrateio_step01_receita
# MAGIC WHERE cd_entidade_nacional =  2 -- SESI
# MAGIC AND cd_centro_responsabilidade LIKE '303070111%' -- vira vida
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step02_02_receita_educacao_basica_cont;
# MAGIC CREATE TABLE rrateio_step02_02_receita_educacao_basica_cont AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        vl_receita_servico_convenio    AS vl_receita_educacao_basica_continuada
# MAGIC FROM rrateio_step01_receita
# MAGIC WHERE cd_entidade_nacional =  2 -- SESI
# MAGIC AND  (cd_centro_responsabilidade LIKE '30301%'     -- EDUCACAO_BASICA
# MAGIC OR     cd_centro_responsabilidade LIKE '3030201%') -- EDUCACAO_CONTINUADA sem eventos
# MAGIC ;
# MAGIC 
# MAGIC --DROP TABLE   rrateio_step02_03_receita_vira_vida_rateada;
# MAGIC CREATE TABLE rrateio_step02_03_receita_vira_vida_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_educacao_basica_continuada / SUM(D.vl_receita_educacao_basica_continuada) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_vira_vida 
# MAGIC AS vl_receita_vira_vida_rateada
# MAGIC FROM rrateio_step02_02_receita_educacao_basica_cont D
# MAGIC INNER JOIN rrateio_step02_01_receita_vira_vida V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only 1 record. Keep in 1 partition.
"""

rrateio_step02_01_receita_vira_vida = rrateio_step01_receita\
.filter((f.col("cd_entidade_nacional") == 2) &\
        (f.col("cd_centro_responsabilidade").startswith("303070111")))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum("vl_receita_servico_convenio").alias("vl_receita_vira_vida"))\
.fillna(0, subset=["vl_receita_vira_vida"])\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step02_01_receita_vira_vida.count() == spark.sql("select count(*) from rrateio_step02_01_receita_vira_vida").collect()[0][0]

# COMMAND ----------

"""
This is a really small table, in test fase it has only 205 records. Keep in 1 partition.
"""

rrateio_step02_02_receita_educacao_basica_cont = rrateio_step01_receita\
.filter((f.col("cd_entidade_nacional") == 2) &\
        ((f.col("cd_centro_responsabilidade").startswith("30301")) |(f.col("cd_centro_responsabilidade").startswith("3030201"))))\
.withColumnRenamed("vl_receita_servico_convenio", "vl_receita_educacao_basica_continuada")\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step02_02_receita_educacao_basica_cont.count() == spark.sql("select count(*) from rrateio_step02_02_receita_educacao_basica_cont").collect()[0][0]

# COMMAND ----------

"""
This window will be re-used during the process.
"""

window = Window.partitionBy("cd_entidade_regional")

# COMMAND ----------

"""
This is a really small table, in test fase it has only 12 records. Keep in 1 partition.
"""

rrateio_step02_03_receita_vira_vida_rateada = rrateio_step02_02_receita_educacao_basica_cont\
.join(rrateio_step02_01_receita_vira_vida.select("cd_entidade_regional", "vl_receita_vira_vida"), ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_vira_vida_rateada",
            ((f.col("vl_receita_educacao_basica_continuada") / f.sum("vl_receita_educacao_basica_continuada").over(window)) *\
             f.col("vl_receita_vira_vida")))\
.drop("vl_receita_educacao_basica_continuada", "vl_receita_vira_vida")\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step02_03_receita_vira_vida_rateada.count() == spark.sql("select count(*) from rrateio_step02_03_receita_vira_vida_rateada").collect()[0][0]

# COMMAND ----------

"""
Validation: ok

sum(vl_receita_vira_vida_rateada) = 487.04
sum(vl_receita_vira_vida_rateada) (planilha) = 487.04

display(rrateio_step02_03_receita_vira_vida_rateada.groupBy("cd_entidade_nacional", "sg_entidade_regional").agg(f.sum("vl_receita_vira_vida_rateada")))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 03
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP03: Obtem receita olímpiada rateada SENAI na educação profissional
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step03_01_receita_olimpiada;
# MAGIC CREATE TABLE rrateio_step03_01_receita_olimpiada AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_receita_servico_convenio)    AS vl_receita_olimpiada
# MAGIC FROM  rrateio_step01_receita
# MAGIC WHERE cd_entidade_nacional = 3 -- SENAI
# MAGIC AND   cd_centro_responsabilidade LIKE '303070108%' -- Olimpíadas
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step03_02_receita_educacao_profissional;
# MAGIC CREATE TABLE rrateio_step03_02_receita_educacao_profissional AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        vl_receita_servico_convenio    AS vl_receita_educacao_profissional
# MAGIC FROM rrateio_step01_receita
# MAGIC WHERE cd_entidade_nacional = 3 -- SENAI
# MAGIC AND  (cd_centro_responsabilidade like '30303%'  -- EDUCACAO PROFISSIONAL E TECNOLOGICA
# MAGIC OR    cd_centro_responsabilidade like  '30304%') -- EDUCACAO SUPERIOR
# MAGIC ;
# MAGIC 
# MAGIC --DROP TABLE   rrateio_step03_03_receita_olimpiada_rateada;
# MAGIC CREATE TABLE rrateio_step03_03_receita_olimpiada_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_educacao_profissional / SUM(D.vl_receita_educacao_profissional) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_olimpiada 
# MAGIC AS vl_receita_olimpiada_rateada
# MAGIC FROM rrateio_step03_02_receita_educacao_profissional D
# MAGIC INNER JOIN rrateio_step03_01_receita_olimpiada V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has 0 records. Keep in 1 partition.
"""

rrateio_step03_01_receita_olimpiada = rrateio_step01_receita\
.filter((f.col("cd_entidade_nacional") == 3) &\
        (f.col("cd_centro_responsabilidade").startswith("303070108")))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum("vl_receita_servico_convenio").alias("vl_receita_olimpiada"))\
.fillna(0, subset=["vl_receita_olimpiada"])\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step03_01_receita_olimpiada.count() == spark.sql("select count(*) from rrateio_step03_01_receita_olimpiada").collect()[0][0]

# COMMAND ----------

"""
This is a really small table, in test fase it has 241 records. Keep in 1 partition.
"""

rrateio_step03_02_receita_educacao_profissional = rrateio_step01_receita\
.filter((f.col("cd_entidade_nacional") == 3) &\
        ((f.col("cd_centro_responsabilidade").startswith("30303")) | (f.col("cd_centro_responsabilidade").startswith("30304"))))\
.withColumnRenamed("vl_receita_servico_convenio", "vl_receita_educacao_profissional")\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step03_02_receita_educacao_profissional.count() == spark.sql("select count(*) from rrateio_step03_02_receita_educacao_profissional").collect()[0][0]

# COMMAND ----------

"""
This is a really small table, in test fase it has 0 records. Keep in 1 partition.
"""

rrateio_step03_03_receita_olimpiada_rateada = rrateio_step03_02_receita_educacao_profissional\
.join(rrateio_step03_01_receita_olimpiada.select("cd_entidade_regional", "vl_receita_olimpiada"), ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_olimpiada_rateada",
            ((f.col("vl_receita_educacao_profissional") / f.sum("vl_receita_educacao_profissional").over(window)) *\
             f.col("vl_receita_olimpiada")))\
.drop("vl_receita_educacao_profissional", "vl_receita_olimpiada")\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step03_03_receita_olimpiada_rateada.count() == spark.sql("select count(*) from rrateio_step03_03_receita_olimpiada_rateada").collect()[0][0]

# COMMAND ----------

"""
Validation: Ok

It has no records
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 04
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP04: Atualiza a Tabela de Receitas com vira vida (SESI) e olimpíadas rateadas (SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step04_receita;
# MAGIC CREATE TABLE rrateio_step04_receita AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_receita_servico_convenio,
# MAGIC NVL(S2.vl_receita_vira_vida_rateada, 0) AS vl_receita_vira_vida_rateada,
# MAGIC NVL(S3.vl_receita_olimpiada_rateada, 0) AS vl_receita_olimpiada_rateada
# MAGIC FROM rrateio_step01_receita D
# MAGIC 
# MAGIC LEFT JOIN rrateio_step02_03_receita_vira_vida_rateada S2
# MAGIC ON  D.cd_entidade_regional = S2.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = S2.cd_centro_responsabilidade
# MAGIC 
# MAGIC LEFT JOIN rrateio_step03_03_receita_olimpiada_rateada S3
# MAGIC ON  D.cd_entidade_regional = S3.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = S3.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- vira vida do SESI e olimpiada do SENAI foram rateados, saem da tabela de receitas
# MAGIC WHERE NOT (D.cd_entidade_nacional = 2 AND D.cd_centro_responsabilidade LIKE '303070111%') 
# MAGIC AND   NOT (D.cd_entidade_nacional = 3 AND D.cd_centro_responsabilidade LIKE '303070108%'); 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only ~1,3k records and ~38kb in mem cache. Keep in 1 partition.

.withColumn("vl_receita_vira_vida_rateada", f.when(f.col("vl_receita_vira_vida_rateada").isNull(), 0)\
            .otherwise(f.col("vl_receita_vira_vida_rateada")))\
.withColumn("vl_receita_olimpiada_rateada", f.when(f.col("vl_receita_olimpiada_rateada").isNull(), 0)\
            .otherwise(f.col("vl_receita_olimpiada_rateada")))\
"""

rrateio_step04_receita = rrateio_step01_receita\
.filter(~((f.col("cd_entidade_nacional") == 2) & (f.col("cd_centro_responsabilidade").startswith("303070111"))) &\
        ~((f.col("cd_entidade_nacional") == 3) & (f.col("cd_centro_responsabilidade").startswith("303070108"))))\
.join(rrateio_step02_03_receita_vira_vida_rateada.select("cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_vira_vida_rateada"), 
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.join(rrateio_step03_03_receita_olimpiada_rateada.select("cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_olimpiada_rateada"), 
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0.0, subset=["vl_receita_vira_vida_rateada", "vl_receita_olimpiada_rateada"])\
.coalesce(1)\
.cache()

rrateio_step04_receita.count()

# COMMAND ----------

#assert  rrateio_step04_receita.count() == spark.sql("select count(*) from rrateio_step04_receita").collect()[0][0]

# COMMAND ----------

"""
Source rrateio_step01_receita is not needed anymore.
This object can be removed from cache.
"""
rrateio_step01_receita = rrateio_step01_receita.unpersist()

# COMMAND ----------

"""
Validation: ok

SESI SP
receita_total = 166452936.01000008
receita_total (planilha) = 166452936.01000002

SENAI SP
receita_total = 89962303.89000002
receita_total (planilha) = 89962303.88999999

display(rrateio_step04_receita\
.filter(f.substring(f.col("sg_entidade_regional"), -2, 2) == "SP")\
.groupBy(f.substring(f.col("sg_entidade_regional"), 1, 4).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_servico_convenio"),
     f.sum("vl_receita_vira_vida_rateada"), 
     f.sum("vl_receita_olimpiada_rateada"),
     f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + \
           f.col("vl_receita_olimpiada_rateada")).alias("receita_total")))       
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 05
# MAGIC ```
# MAGIC %sql
# MAGIC --======================================================================================================
# MAGIC -- STEP05: Obtem receitas ETD e Gestão rateadas por Educação (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step05_01_receita_etd_gestao_educacao;
# MAGIC CREATE TABLE rrateio_step05_01_receita_etd_gestao_educacao AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada)    AS vl_receita_etd_gestao_educacao
# MAGIC FROM  rrateio_step04_receita
# MAGIC WHERE (cd_centro_responsabilidade LIKE '30310%' OR cd_centro_responsabilidade LIKE '30311%') -- gestão (10) e etd (11) em EDUCAÇÃO
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step05_02_receita_educacao;
# MAGIC CREATE TABLE rrateio_step05_02_receita_educacao AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada)    AS vl_receita_educacao
# MAGIC FROM rrateio_step04_receita
# MAGIC WHERE cd_centro_responsabilidade LIKE '303%' -- EDUCACAO 
# MAGIC AND NOT(cd_centro_responsabilidade LIKE '30310%' OR cd_centro_responsabilidade LIKE '30311%') -- retirando gestão (10) e etd (11) que serão rateados
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step05_03_receita_etd_gestao_educacao_rateada;
# MAGIC CREATE TABLE rrateio_step05_03_receita_etd_gestao_educacao_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_educacao / SUM(D.vl_receita_educacao) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_etd_gestao_educacao AS vl_receita_etd_gestao_educacao_rateada
# MAGIC FROM rrateio_step05_02_receita_educacao D
# MAGIC INNER JOIN rrateio_step05_01_receita_etd_gestao_educacao V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step05_receita;
# MAGIC CREATE TABLE rrateio_step05_receita AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_receita_servico_convenio,
# MAGIC D.vl_receita_vira_vida_rateada,
# MAGIC D.vl_receita_olimpiada_rateada,
# MAGIC NVL(R.vl_receita_etd_gestao_educacao_rateada, 0) AS vl_receita_etd_gestao_educacao_rateada
# MAGIC 
# MAGIC FROM rrateio_step04_receita D
# MAGIC 
# MAGIC LEFT JOIN rrateio_step05_03_receita_etd_gestao_educacao_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- gestão (10) e etd (11) foram rateados, saem da lista de receitas
# MAGIC WHERE NOT(D.cd_centro_responsabilidade LIKE '30310%' OR D.cd_centro_responsabilidade LIKE '30311%');
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a really small table, in test fase it has only 30 records. Keep in 1 partition.
"""

rrateio_step05_01_receita_etd_gestao_educacao = rrateio_step04_receita\
.filter((f.col("cd_centro_responsabilidade").startswith("30310")) | (f.col("cd_centro_responsabilidade").startswith("30311")))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada")).alias("vl_receita_etd_gestao_educacao"))\
.fillna(0, subset=["vl_receita_etd_gestao_educacao"])\
.drop("vl_receita_servico_convenio", "vl_receita_vira_vida_rateada", "vl_receita_olimpiada_rateada")\
.coalesce(1)

# COMMAND ----------

"""
This is a really small table, in test fase it has only 56 records. Keep in 1 partition.
"""

rrateio_step05_02_receita_educacao = rrateio_step04_receita\
.filter((f.col("cd_centro_responsabilidade").startswith("303")) &\
        ~((f.col("cd_centro_responsabilidade").startswith("30310")) | (f.col("cd_centro_responsabilidade").startswith("30311"))))\
.withColumn("vl_receita_educacao", f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada"))\
.drop("vl_receita_servico_convenio", "vl_receita_vira_vida_rateada", "vl_receita_olimpiada_rateada")\
.coalesce(1)

# COMMAND ----------

"""
This is a really small table, in test fase it has only 29 records. Keep in 1 partition.
"""

rrateio_step05_03_receita_etd_gestao_educacao_rateada = rrateio_step05_02_receita_educacao\
.join(rrateio_step05_01_receita_etd_gestao_educacao.select("vl_receita_etd_gestao_educacao", "cd_entidade_regional"), ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_etd_gestao_educacao_rateada",
            ((f.col("vl_receita_educacao") / f.sum("vl_receita_educacao").over(window)) *\
             f.col("vl_receita_etd_gestao_educacao")))\
.drop("vl_receita_educacao", "vl_receita_etd_gestao_educacao")\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only ~1,2k records and ~47kb in mem cache. Keep in 1 partition.
"""

rrateio_step05_receita = rrateio_step04_receita\
.filter(~((f.col("cd_centro_responsabilidade").like("30310%")) | (f.col("cd_centro_responsabilidade").like("30311%"))))\
.join(rrateio_step05_03_receita_etd_gestao_educacao_rateada\
      .select("vl_receita_etd_gestao_educacao_rateada", "cd_entidade_regional", "cd_centro_responsabilidade"),
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0, subset=["vl_receita_etd_gestao_educacao_rateada"])\
.coalesce(1)\
.cache()

rrateio_step05_receita.count()

# COMMAND ----------

#assert  rrateio_step05_receita.count() == spark.sql("select count(*) from rrateio_step05_receita").collect()[0][0]

# COMMAND ----------

"""
Source rrateio_step04_receita is not needed anymore.
This object can be removed from cache.
"""
rrateio_step04_receita = rrateio_step04_receita.unpersist()

# COMMAND ----------

"""
Validation: ok

SESI SP
sum(vl_receita_etd_gestao_educacao_rateada) = 5.000000000000002
sum(vl_receita_etd_gestao_educacao_rateada) (planilha) = 5
receita_total = 166452936.01000005
receita_total (planilha) = 166452936.01000002

SENAI SP
sum(vl_receita_etd_gestao_educacao_rateada) = 0
sum(vl_receita_etd_gestao_educacao_rateada) (planilha) = 0
receita_total = 89962303.88999999
receita_total (planilha) = 89962303.89

display(rrateio_step05_receita\
.filter(f.substring(f.col("sg_entidade_regional"), -2, 2) == "SP")\
.groupBy(f.substring(f.col("sg_entidade_regional"), 1, 4).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_etd_gestao_educacao_rateada"),
     f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + \
           f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada")).alias("receita_total")))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 06
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP06: Obtem receitas ETD e Gestão rateadas por Outros Negócios (vale para SESI e SENAI)
# MAGIC -- Manutenção em 10/11/2020
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step06_01_receita_etd_gestao_outros;
# MAGIC CREATE TABLE rrateio_step06_01_receita_etd_gestao_outros AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUBSTRING(cd_centro_responsabilidade, 1, 3) AS cd_centro_responsabilidade_n2,
# MAGIC         SUM(vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada)    AS vl_receita_etd_gestao_outros       
# MAGIC FROM  rrateio_step05_receita
# MAGIC WHERE (cd_centro_responsabilidade LIKE '30_10%' OR cd_centro_responsabilidade LIKE '30_11%') -- gestao (10) e etd (11)
# MAGIC -- Filtro alterado em 10/11/2020
# MAGIC AND   (  (cd_centro_responsabilidade LIKE '302%' AND cd_entidade_nacional = 3) -- Tecnologia SENAI tem gestão e etd, no SESI não
# MAGIC       OR  cd_centro_responsabilidade LIKE '301%' -- defesa interesse SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '304%' -- ssi SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '305%' -- cultura SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '306%' -- cooperacao SESI e SENAI
# MAGIC       )
# MAGIC --     
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional, SUBSTRING(cd_centro_responsabilidade, 1, 3);
# MAGIC 
# MAGIC DROP TABLE   rrateio_step06_02_receita_outros;
# MAGIC CREATE TABLE rrateio_step06_02_receita_outros AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada)    AS vl_receita_outros    
# MAGIC FROM rrateio_step05_receita 
# MAGIC -- Filtro alterado em 10/11/2020
# MAGIC WHERE (  (cd_centro_responsabilidade LIKE '302%' AND cd_entidade_nacional = 3) -- Tecnologia SENAI tem gestão e etd, no SESI não
# MAGIC       OR  cd_centro_responsabilidade LIKE '301%' -- defesa interesse SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '304%' -- ssi SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '305%' -- cultura SESI e SENAI
# MAGIC       OR  cd_centro_responsabilidade LIKE '306%' -- cooperacao SESI e SENAI
# MAGIC       )
# MAGIC --
# MAGIC AND   NOT(cd_centro_responsabilidade LIKE '30_10%' OR cd_centro_responsabilidade LIKE '30_11%') -- retirando gestão (10) e etd (11) que serão rateados
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step06_03_receita_etd_gestao_outros_rateada;
# MAGIC CREATE TABLE rrateio_step06_03_receita_etd_gestao_outros_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_outros / SUM(D.vl_receita_outros) OVER(PARTITION BY D.cd_entidade_regional, SUBSTRING(D.cd_centro_responsabilidade, 1, 3)) ) * V.vl_receita_etd_gestao_outros 
# MAGIC AS vl_receita_etd_gestao_outros_rateada
# MAGIC FROM rrateio_step06_02_receita_outros D
# MAGIC INNER JOIN rrateio_step06_01_receita_etd_gestao_outros V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional 
# MAGIC AND SUBSTRING(D.cd_centro_responsabilidade, 1, 3) = V.cd_centro_responsabilidade_n2;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step06_receita;
# MAGIC CREATE TABLE rrateio_step06_receita AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_receita_servico_convenio,
# MAGIC D.vl_receita_vira_vida_rateada,
# MAGIC D.vl_receita_olimpiada_rateada,
# MAGIC D.vl_receita_etd_gestao_educacao_rateada,
# MAGIC NVL(R.vl_receita_etd_gestao_outros_rateada, 0) AS vl_receita_etd_gestao_outros_rateada
# MAGIC FROM rrateio_step05_receita D
# MAGIC 
# MAGIC LEFT JOIN rrateio_step06_03_receita_etd_gestao_outros_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- gestão (10) e etd (11) foram rateados, saem da lista de receitas
# MAGIC 
# MAGIC -- Filtro alterado em 10/11/2020
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
This is a really small table, in test fase it has only 18 records. Keep in 1 partition.
"""

rrateio_step06_01_receita_etd_gestao_outros = rrateio_step05_receita\
.filter(((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%"))) &\
        (((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3")) |\
         (f.col("cd_centro_responsabilidade").like("301%")) |\
         (f.col("cd_centro_responsabilidade").like("304%")) |\
         (f.col("cd_centro_responsabilidade").like("305%")) |\
         (f.col("cd_centro_responsabilidade").like("306%"))))\
.withColumn("cd_centro_responsabilidade_n2", f.substring(f.col("cd_centro_responsabilidade"), 1, 3))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional", "cd_centro_responsabilidade_n2")\
.agg(f.sum(f.col("vl_receita_servico_convenio") +\
           f.col("vl_receita_vira_vida_rateada") +\
           f.col("vl_receita_olimpiada_rateada") +\
           f.col("vl_receita_etd_gestao_educacao_rateada"))\
     .alias("vl_receita_etd_gestao_outros"))\
.fillna(0, subset=["vl_receita_etd_gestao_outros"])\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only ~1k records. Keep in 1 partition.
"""

rrateio_step06_02_receita_outros = rrateio_step05_receita\
.filter((((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3")) |\
         (f.col("cd_centro_responsabilidade").like("301%")) |\
         (f.col("cd_centro_responsabilidade").like("304%")) |\
         (f.col("cd_centro_responsabilidade").like("305%")) |\
         (f.col("cd_centro_responsabilidade").like("306%"))) &\
        ~((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%"))))\
.withColumn("vl_receita_outros", f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") +
            f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada"))\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only 449 records. Keep in 1 partition.
"""

window2 = Window.partitionBy("cd_entidade_regional", "cd_centro_responsabilidade_n2")

rrateio_step06_03_receita_etd_gestao_outros_rateada = rrateio_step06_02_receita_outros\
.select("cd_entidade_regional", "cd_centro_responsabilidade", "sg_entidade_regional", "cd_entidade_nacional", "vl_receita_outros")\
.withColumn("cd_centro_responsabilidade_n2",f.substring(f.col("cd_centro_responsabilidade"), 1, 3))\
.join(rrateio_step06_01_receita_etd_gestao_outros\
     .select("vl_receita_etd_gestao_outros", "cd_centro_responsabilidade_n2", "cd_entidade_regional"), 
     ["cd_entidade_regional", "cd_centro_responsabilidade_n2"], "inner")\
.withColumn("vl_receita_etd_gestao_outros_rateada",
            ((f.col("vl_receita_outros") / f.sum("vl_receita_outros").over(window2)) *\
             f.col("vl_receita_etd_gestao_outros")))\
.drop("cd_centro_responsabilidade_n2", "vl_receita_outros", "vl_receita_etd_gestao_outros")\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only ~1,2k records and 47kb in mem cache. Keep in 1 partition.
"""

rrateio_step06_receita = rrateio_step05_receita\
.filter(~(((f.col("cd_centro_responsabilidade").like("30_10%")) | (f.col("cd_centro_responsabilidade").like("30_11%"))) &\
          (((f.col("cd_centro_responsabilidade").like("302%")) & (f.col("cd_entidade_nacional") == "3")) |\
           (f.col("cd_centro_responsabilidade").like("301%")) |\
           (f.col("cd_centro_responsabilidade").like("304%")) |\
           (f.col("cd_centro_responsabilidade").like("305%")) |\
           (f.col("cd_centro_responsabilidade").like("306%")))))\
.join(rrateio_step06_03_receita_etd_gestao_outros_rateada\
      .select("cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_etd_gestao_outros_rateada"), 
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0, subset=["vl_receita_etd_gestao_outros_rateada"])\
.cache()\
.coalesce(1)

rrateio_step06_receita.count()

# COMMAND ----------

#assert  rrateio_step06_receita.count() == spark.sql("select count(*) from rrateio_step06_receita").collect()[0][0]

# COMMAND ----------

"""
Source rrateio_step05_receita is not needed anymore.
This object can be removed from cache.
"""
rrateio_step05_receita = rrateio_step05_receita.unpersist()

# COMMAND ----------

"""
Validation: ok

SESI - SP
sum("vl_receita_etd_gestao_outros_rateada") = 1241.97
sum("vl_receita_etd_gestao_outros_rateada") (planilha) = 1241.97
receita_total = 166452936.01000002
receita_total (planilha) = 166452936.01000002

SENAI - SP
sum("vl_receita_etd_gestao_outros_rateada") = 0
sum("vl_receita_etd_gestao_outros_rateada") (planilha) = 0
receita_total = 89962303.88999999
receita_total (planilha) = 89962303.88999999

display(rrateio_step06_receita\
.filter(f.substring(f.col("sg_entidade_regional"), -2, 2) == "SP")\
.groupBy(f.substring(f.col("sg_entidade_regional"), 1, 4).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_etd_gestao_outros_rateada"),
     f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + \
           f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
           f.col("vl_receita_etd_gestao_outros_rateada")).alias("receita_total")))
"""         

# COMMAND ----------

"""
Validation: ok

sum("vl_receita_etd_gestao_outros_rateada") = 1241.97
sum("vl_receita_etd_gestao_outros_rateada") (planilha) = 1241.97

display(rrateio_step06_receita\
.filter((f.col("sg_entidade_regional") == "SESI-SP") & (f.col("vl_receita_etd_gestao_outros_rateada") > 0))\
.groupBy(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_etd_gestao_outros_rateada")))
"""


# COMMAND ----------

"""
Validation: ok

no records

display(rrateio_step06_receita\
.filter((f.col("sg_entidade_regional") == "SENAI-SP") & (f.col("vl_receita_etd_gestao_outros_rateada") > 0))\
.groupBy(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_etd_gestao_outros_rateada")))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP07: Obtem receita Suporte Negócio rateada por Negócio (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step07_01_receita_suporte_negocio;
# MAGIC CREATE TABLE rrateio_step07_01_receita_suporte_negocio AS
# MAGIC SELECT  cd_entidade_nacional,
# MAGIC         sg_entidade_regional, 
# MAGIC         cd_entidade_regional, 
# MAGIC         SUM(vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada+vl_receita_etd_gestao_outros_rateada)    
# MAGIC         AS vl_receita_suporte_negocio        
# MAGIC FROM  rrateio_step06_receita
# MAGIC WHERE cd_centro_responsabilidade LIKE '307%'
# MAGIC GROUP BY cd_entidade_nacional, sg_entidade_regional, cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step07_02_receita_negocio;
# MAGIC CREATE TABLE rrateio_step07_02_receita_negocio AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada+vl_receita_etd_gestao_outros_rateada) 
# MAGIC        AS vl_receita_negocio    
# MAGIC FROM rrateio_step06_receita 
# MAGIC WHERE cd_centro_responsabilidade LIKE '3%' -- negócios
# MAGIC AND   cd_centro_responsabilidade NOT LIKE '307%' -- retirando suprte negócio que será rateado
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step07_03_receita_suporte_negocio_rateada;
# MAGIC CREATE TABLE rrateio_step07_03_receita_suporte_negocio_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_negocio / SUM(D.vl_receita_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_suporte_negocio AS vl_receita_suporte_negocio_rateada
# MAGIC FROM rrateio_step07_02_receita_negocio D
# MAGIC INNER JOIN rrateio_step07_01_receita_suporte_negocio V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step07_receita;
# MAGIC CREATE TABLE rrateio_step07_receita AS
# MAGIC SELECT
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_receita_servico_convenio,
# MAGIC D.vl_receita_vira_vida_rateada,
# MAGIC D.vl_receita_olimpiada_rateada,
# MAGIC D.vl_receita_etd_gestao_educacao_rateada,
# MAGIC D.vl_receita_etd_gestao_outros_rateada,
# MAGIC NVL(R.vl_receita_suporte_negocio_rateada, 0) AS vl_receita_suporte_negocio_rateada
# MAGIC 
# MAGIC FROM rrateio_step06_receita D
# MAGIC 
# MAGIC LEFT JOIN rrateio_step07_03_receita_suporte_negocio_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- suporte negócio foi rateado, sai da lista de receitas
# MAGIC WHERE D.cd_centro_responsabilidade NOT LIKE '307%';
# MAGIC 
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only 38 records. Keep in 1 partition.
"""

rrateio_step07_01_receita_suporte_negocio = rrateio_step06_receita\
.filter(f.col("cd_centro_responsabilidade").startswith("307"))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + f.col("vl_receita_etd_gestao_outros_rateada")).alias("vl_receita_suporte_negocio"))\
.fillna(0, subset=["vl_receita_suporte_negocio"])\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only ~1.1k records. Keep in 1 partition.
"""

rrateio_step07_02_receita_negocio = rrateio_step06_receita\
.filter((f.col("cd_centro_responsabilidade").startswith("3")) &\
        ~(f.col("cd_centro_responsabilidade").startswith("307")))\
.withColumn("vl_receita_negocio", f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + f.col("vl_receita_etd_gestao_outros_rateada"))\
.drop("vl_receita_servico_convenio", "vl_receita_vira_vida_rateada", "vl_receita_olimpiada_rateada", "vl_receita_etd_gestao_educacao_rateada", "vl_receita_etd_gestao_outros_rateada")\
.fillna(0, subset=["vl_receita_negocio"])\
.coalesce(1)

# COMMAND ----------

"""
This is a small table, in test fase it has only 872 records. Keep in 1 partition.
"""

rrateio_step07_03_receita_suporte_negocio_rateada = rrateio_step07_02_receita_negocio\
.join(rrateio_step07_01_receita_suporte_negocio.select("vl_receita_suporte_negocio", "cd_entidade_regional"),
      ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_suporte_negocio_rateada",
            ((f.col("vl_receita_negocio") / f.sum("vl_receita_negocio").over(window)) *\
             f.col("vl_receita_suporte_negocio")))\
.coalesce(1)

# COMMAND ----------

"""
Ok, this is a so small table, in test fase it has ~1,2k records and 75kb in mem cache, we can keep it in 1 partitions.
"""

rrateio_step07_receita = rrateio_step06_receita\
.filter(~(f.col("cd_centro_responsabilidade").startswith("307")))\
.join(rrateio_step07_03_receita_suporte_negocio_rateada\
      .select("cd_entidade_regional","cd_centro_responsabilidade", "vl_receita_suporte_negocio_rateada"),
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0, subset=["vl_receita_suporte_negocio_rateada"])\
.coalesce(1)\
.cache()

rrateio_step07_receita.count()

# COMMAND ----------

#assert  rrateio_step07_receita.count() == spark.sql("select count(*) from rrateio_step07_receita").collect()[0][0]

# COMMAND ----------

"""
Source rrateio_step06_receita is not needed anymore.
This object can be removed from cache.
"""
rrateio_step06_receita = rrateio_step06_receita.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

vl_receita_suporte_negocio = 62157.7
vl_receita_suporte_negocio (planilha) = 62157.7

display(rrateio_step07_01_receita_suporte_negocio.filter(f.col("sg_entidade_regional") == "SENAI-SP"))
"""

# COMMAND ----------

"""
VALIDAÇÃO - OK

SESI - SP
sum("vl_receita_suporte_negocio_rateada") = 229208.56999999998
sum("vl_receita_suporte_negocio_rateada") (planilha) = 229208.5699999999
receita_total = 166452936.01
receita_total (planilha) = 166452936.01

SENAI - SP
sum("vl_receita_suporte_negocio_rateada") = 62157.70000000001
sum("vl_receita_suporte_negocio_rateada") (planilha) =62157.7
receita_total = 89962303.89
receita_total (planilha) = 89962303.89

display(rrateio_step07_receita\
.filter(f.substring(f.col("sg_entidade_regional"), -2, 2) == "SP")\
.groupBy(f.substring(f.col("sg_entidade_regional"), 1, 4).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_suporte_negocio_rateada"),
     f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + \
           f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
           f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada")).alias("receita_total")))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC --======================================================================================================
# MAGIC -- STEP08: Obtem receita indireta rateada por Negócio (vale para SESI e SENAI)
# MAGIC --======================================================================================================
# MAGIC DROP TABLE   rrateio_step08_01_receita_indireta;
# MAGIC CREATE TABLE rrateio_step08_01_receita_indireta AS
# MAGIC SELECT 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional,
# MAGIC    
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE '1%'
# MAGIC     THEN (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada
# MAGIC           +vl_receita_etd_gestao_outros_rateada+vl_receita_suporte_negocio_rateada) 
# MAGIC     ELSE 0 END) AS vl_receita_indireta_gestao,
# MAGIC             
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE '2%'
# MAGIC     THEN (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada
# MAGIC           +vl_receita_etd_gestao_outros_rateada+vl_receita_suporte_negocio_rateada) 
# MAGIC     ELSE 0 END) AS vl_receita_indireta_desenv_institucional,
# MAGIC          
# MAGIC SUM(CASE WHEN cd_centro_responsabilidade LIKE '4%'
# MAGIC     THEN (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada
# MAGIC           +vl_receita_etd_gestao_outros_rateada+vl_receita_suporte_negocio_rateada) 
# MAGIC     ELSE 0 END) AS vl_receita_indireta_apoio
# MAGIC             
# MAGIC FROM  rrateio_step07_receita
# MAGIC WHERE cd_centro_responsabilidade LIKE '1%' OR cd_centro_responsabilidade LIKE '2%' OR cd_centro_responsabilidade LIKE '4%'
# MAGIC GROUP BY 
# MAGIC cd_entidade_nacional,
# MAGIC sg_entidade_regional,
# MAGIC cd_entidade_regional;    
# MAGIC 
# MAGIC DROP TABLE   rrateio_step08_02_receita_negocio;
# MAGIC CREATE TABLE rrateio_step08_02_receita_negocio AS
# MAGIC SELECT cd_entidade_nacional,
# MAGIC        sg_entidade_regional, 
# MAGIC        cd_entidade_regional,
# MAGIC        cd_centro_responsabilidade,
# MAGIC        (vl_receita_servico_convenio+vl_receita_vira_vida_rateada+vl_receita_olimpiada_rateada+vl_receita_etd_gestao_educacao_rateada+vl_receita_etd_gestao_outros_rateada
# MAGIC        +vl_receita_suporte_negocio_rateada) AS vl_receita_negocio  
# MAGIC FROM rrateio_step07_receita 
# MAGIC WHERE cd_centro_responsabilidade LIKE '3%' -- negócios
# MAGIC ;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step08_03_receita_indireta_rateada;
# MAGIC CREATE TABLE rrateio_step08_03_receita_indireta_rateada AS
# MAGIC SELECT 
# MAGIC D.cd_entidade_nacional,
# MAGIC D.sg_entidade_regional,
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC ( D.vl_receita_negocio / SUM(D.vl_receita_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_indireta_gestao AS vl_receita_indireta_gestao_rateada,
# MAGIC ( D.vl_receita_negocio / SUM(D.vl_receita_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_indireta_desenv_institucional
# MAGIC AS vl_receita_indireta_desenv_institucional_rateada,
# MAGIC ( D.vl_receita_negocio / SUM(D.vl_receita_negocio) OVER(PARTITION BY D.cd_entidade_regional) ) * V.vl_receita_indireta_apoio AS vl_receita_indireta_apoio_rateada
# MAGIC 
# MAGIC FROM rrateio_step08_02_receita_negocio D
# MAGIC INNER JOIN rrateio_step08_01_receita_indireta V
# MAGIC ON  D.cd_entidade_regional = V.cd_entidade_regional;
# MAGIC 
# MAGIC DROP TABLE   rrateio_step08_receita;
# MAGIC CREATE TABLE rrateio_step08_receita AS
# MAGIC SELECT
# MAGIC --D.cd_entidade_nacional, --Retirado em 20/10
# MAGIC --D.sg_entidade_regional, --Retirado em 20/10
# MAGIC D.cd_entidade_regional,
# MAGIC D.cd_centro_responsabilidade,
# MAGIC D.vl_receita_servico_convenio,
# MAGIC D.vl_receita_vira_vida_rateada,
# MAGIC D.vl_receita_olimpiada_rateada,
# MAGIC D.vl_receita_etd_gestao_educacao_rateada,
# MAGIC D.vl_receita_etd_gestao_outros_rateada,
# MAGIC D.vl_receita_suporte_negocio_rateada,
# MAGIC NVL(R.vl_receita_indireta_gestao_rateada, 0)               AS vl_receita_indireta_gestao_rateada,
# MAGIC NVL(R.vl_receita_indireta_desenv_institucional_rateada, 0) AS vl_receita_indireta_desenv_institucional_rateada,
# MAGIC NVL(R.vl_receita_indireta_apoio_rateada, 0)                AS vl_receita_indireta_apoio_rateada
# MAGIC 
# MAGIC FROM rrateio_step07_receita D
# MAGIC 
# MAGIC LEFT JOIN rrateio_step08_03_receita_indireta_rateada R
# MAGIC ON  D.cd_entidade_regional = R.cd_entidade_regional 
# MAGIC AND D.cd_centro_responsabilidade = R.cd_centro_responsabilidade
# MAGIC 
# MAGIC -- Na lista de receitas ficam só valores de negócio com os respectivos rateios
# MAGIC WHERE D.cd_centro_responsabilidade like '3%'-- somente negocio
# MAGIC ;
# MAGIC ```

# COMMAND ----------

"""
This is a small table, in test fase it has only ~31 records,  2,5KB in mem cached. Keep in 1 partition.
"""

rrateio_step08_01_receita_indireta = rrateio_step07_receita\
.filter((f.col("cd_centro_responsabilidade").startswith("1")) |\
        (f.col("cd_centro_responsabilidade").startswith("2")) |\
        (f.col("cd_centro_responsabilidade").startswith("4")))\
.groupBy("cd_entidade_nacional", "sg_entidade_regional", "cd_entidade_regional")\
.agg(f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("1"), 
                  f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") +\
                  f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
                  f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada"))\
           .otherwise(f.lit(0))).alias("vl_receita_indireta_gestao"),
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("2"), 
                  f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") +\
                  f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
                  f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada"))\
           .otherwise(f.lit(0))).alias("vl_receita_indireta_desenv_institucional"),     
     f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("4"), 
                  f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") +\
                  f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
                  f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada"))\
           .otherwise(f.lit(0))).alias("vl_receita_indireta_apoio"))\
.fillna(0, subset=["vl_receita_indireta_gestao", "vl_receita_indireta_desenv_institucional", "vl_receita_indireta_apoio"])\
.coalesce(1)\
.cache()

rrateio_step08_01_receita_indireta.count()

# COMMAND ----------

#assert  rrateio_step08_01_receita_indireta.count() == spark.sql("select count(*) from rrateio_step08_01_receita_indireta").collect()[0][0]

# COMMAND ----------

"""
Ok, this is a so small table, in test fase it has ~1,1k records. Keep it in 1 partitions.
"""

rrateio_step08_02_receita_negocio = rrateio_step07_receita\
.filter(f.col("cd_centro_responsabilidade").startswith("3"))\
.withColumn("vl_receita_negocio", f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") +\
            f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
            f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada"))\
.drop("vl_receita_servico_convenio", "vl_receita_vira_vida_rateada", "vl_receita_olimpiada_rateada", "vl_receita_etd_gestao_educacao_rateada", "vl_receita_etd_gestao_outros_rateada", "vl_receita_suporte_negocio_rateada")\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step08_02_receita_negocio.count() == spark.sql("select count(*) from rrateio_step08_02_receita_negocio").collect()[0][0]

# COMMAND ----------

"""
Ok, this is a so small table, in test fase it has 690 records. Keep it in 8 partitions.
"""

rrateio_step08_03_receita_indireta_rateada = rrateio_step08_02_receita_negocio\
.join(rrateio_step08_01_receita_indireta\
      .select("cd_entidade_regional", "vl_receita_indireta_gestao",\
              "vl_receita_indireta_desenv_institucional", "vl_receita_indireta_apoio"),
      ["cd_entidade_regional"], "inner")\
.withColumn("vl_receita_indireta_gestao_rateada",
            ((f.col("vl_receita_negocio") / f.sum("vl_receita_negocio").over(window)) *\
             f.col("vl_receita_indireta_gestao")))\
.withColumn("vl_receita_indireta_desenv_institucional_rateada",
            ((f.col("vl_receita_negocio") / f.sum("vl_receita_negocio").over(window)) *\
             f.col("vl_receita_indireta_desenv_institucional")))\
.withColumn("vl_receita_indireta_apoio_rateada",
            ((f.col("vl_receita_negocio") / f.sum("vl_receita_negocio").over(window)) *\
             f.col("vl_receita_indireta_apoio")))\
.drop("vl_receita_negocio", "vl_receita_indireta_gestao", "vl_receita_indireta_desenv_institucional", "vl_receita_indireta_apoio")\
.fillna(0, subset=["vl_receita_indireta_gestao_rateada", "vl_receita_indireta_desenv_institucional_rateada", "vl_receita_indireta_apoio_rateada"])\
.coalesce(1)

# COMMAND ----------

#assert  rrateio_step08_03_receita_indireta_rateada.count() == spark.sql("select count(*) from rrateio_step08_03_receita_indireta_rateada").collect()[0][0]

# COMMAND ----------

"""
This is the final object
For the first load, this object is ~91k, keep it in 1 partition.
"""

rrateio_step08_receita = rrateio_step07_receita\
.filter(f.col("cd_centro_responsabilidade").startswith("3"))\
.drop("sg_entidade_regional", "cd_entidade_nacional")\
.join(rrateio_step08_03_receita_indireta_rateada\
      .select("cd_entidade_regional", "cd_centro_responsabilidade", "vl_receita_indireta_gestao_rateada", 
              "vl_receita_indireta_desenv_institucional_rateada", "vl_receita_indireta_apoio_rateada"),
      ["cd_entidade_regional", "cd_centro_responsabilidade"], "left")\
.fillna(0, subset=["vl_receita_indireta_gestao_rateada", "vl_receita_indireta_desenv_institucional_rateada", "vl_receita_indireta_apoio_rateada"])\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.coalesce(1)\
.cache()

rrateio_step08_receita.count()

# COMMAND ----------

"""
Source rrateio_step07_receita and rrateio_step08_01_receita_indireta_rateada is not needed anymore.
This object can be removed from cache.
"""
rrateio_step07_receita = rrateio_step07_receita.unpersist()
rrateio_step08_01_receita_indireta = rrateio_step08_01_receita_indireta.unpersist()

# COMMAND ----------

"""
VALIDAÇÃO - OK

SESI - SP
sum("vl_receita_indireta_gestao_rateada") = 0
sum("vl_receita_indireta_gestao_rateada") (planilha) = 0
sum("vl_receita_indireta_desenv_institucional_rateada") = 3293.0700000000134
sum("vl_receita_indireta_desenv_institucional_rateada") (planilha) = 3293.0699999999997
sum("vl_receita_indireta_apoio_rateada") = 0
sum("vl_receita_indireta_apoio_rateada") (planilha) = 0
vl_receita_servico_convenio = 166452936.00999996
vl_receita_servico_convenio (planilha) = 166452936.01000002

SENAI - SP
sum("vl_receita_indireta_gestao_rateada") = 0
sum("vl_receita_indireta_gestao_rateada") (planilha) = 0
sum("vl_receita_indireta_desenv_institucional_rateada") = 0
sum("vl_receita_indireta_desenv_institucional_rateada") (planilha) = 0
sum("vl_receita_indireta_apoio_rateada") = 0
sum("vl_receita_indireta_apoio_rateada") (planilha) = 0
vl_receita_servico_convenio = 89962303.89
vl_receita_servico_convenio (planilha) = 89962303.89

display(rrateio_step08_receita\
.filter(f.substring(f.col("sg_entidade_regional"), -2, 2) == "SP")\
.groupBy(f.substring(f.col("sg_entidade_regional"), 1, 4).alias("sg_entidade_regional"))\
.agg(f.sum("vl_receita_indireta_gestao_rateada"), 
     f.sum("vl_receita_indireta_desenv_institucional_rateada"),
     f.sum("vl_receita_indireta_apoio_rateada"),
     f.sum(f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + \
           f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + \
           f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada") +\
           f.col("vl_receita_indireta_gestao_rateada") + f.col("vl_receita_indireta_desenv_institucional_rateada") +\
           f.col("vl_receita_indireta_apoio_rateada")).alias("vl_receita_servico_convenio")))
"""

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
display(
  rrateio_step08_receita\
  .filter(f.col("sg_entidade_regional").isin('SESI-SP','SESI-DF','SESI-RJ','SESI-AC','SESI-RS'))\
  .select(
    "sg_entidade_regional",
    "cd_centro_responsabilidade",
    (f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada") + f.col("vl_receita_indireta_gestao_rateada") + f.col("vl_receita_indireta_desenv_institucional_rateada") + f.col("vl_receita_indireta_apoio_rateada")).alias("receita_total"))\
  .groupBy("sg_entidade_regional")\
  .agg(
    f.sum("receita_total").alias("receita_total"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("30301") | f.col("cd_centro_responsabilidade").startswith("3030201"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("educacao_basica_continuada"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("303") & ~(f.col("cd_centro_responsabilidade").startswith("30301")) & ~(f.col("cd_centro_responsabilidade").startswith("3030201")),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("educacao_outros"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("304"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("ssi"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("305"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("cultura"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("306"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("cooperacao_social"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("3") & ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','304','305', '306')),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("outros_servicos")
  )
)
"""

# COMMAND ----------

"""
VALIDAÇÃO - OK
"""

"""
display(
  rrateio_step08_receita\
  .filter(f.col("sg_entidade_regional").isin('SENAI-SP','SENAI-DF','SENAI-RJ','SENAI-AC','SENAI-RS'))\
  .select(
    "sg_entidade_regional",
    "cd_centro_responsabilidade",
    (f.col("vl_receita_servico_convenio") + f.col("vl_receita_vira_vida_rateada") + f.col("vl_receita_olimpiada_rateada") + f.col("vl_receita_etd_gestao_educacao_rateada") + f.col("vl_receita_etd_gestao_outros_rateada") + f.col("vl_receita_suporte_negocio_rateada") + f.col("vl_receita_indireta_gestao_rateada") + f.col("vl_receita_indireta_desenv_institucional_rateada") + f.col("vl_receita_indireta_apoio_rateada")).alias("receita_total"))\
  .groupBy("sg_entidade_regional")\
  .agg(
    f.sum("receita_total").alias("receita_total"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("30303") | f.col("cd_centro_responsabilidade").startswith("30304"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("educacao_profissional"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("303") & ~(f.col("cd_centro_responsabilidade").startswith("30303")) & ~(f.col("cd_centro_responsabilidade").startswith("30304")),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("educacao_outros"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("302"),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("tecnol_inov"),
    f.sum(f.when(f.col("cd_centro_responsabilidade").startswith("3") & ~(f.substring(f.col("cd_centro_responsabilidade"), 1, 3).isin('303','302')),
                 f.col("receita_total")).otherwise(f.lit(0.0))).alias("outros_servicos")
  )
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz

# COMMAND ----------

rrateio_step08_receita = tcf.add_control_fields(rrateio_step08_receita, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Adjusting schema

# COMMAND ----------

var_column_type_map = {"cd_entidade_regional" : "int", 
                       "cd_centro_responsabilidade" : "string",                                            
                       "vl_receita_servico_convenio": "decimal(18,6)", 
                       "vl_receita_vira_vida_rateada": "decimal(18,6)", 
                       "vl_receita_olimpiada_rateada": "decimal(18,6)",
                       "vl_receita_etd_gestao_educacao_rateada": "decimal(18,6)", 
                       "vl_receita_etd_gestao_outros_rateada": "decimal(18,6)",
                       "vl_receita_suporte_negocio_rateada": "decimal(18,6)",
                       "vl_receita_indireta_gestao_rateada": "decimal(18,6)",
                       "vl_receita_indireta_desenv_institucional_rateada": "decimal(18,6)", 
                       "vl_receita_indireta_apoio_rateada": "decimal(18,6)", 
                       "cd_mes_fechamento": "int",
                       "cd_ano_fechamento": "int", 
                       "dt_fechamento": "date"}

for c in var_column_type_map:
  rrateio_step08_receita = rrateio_step08_receita.withColumn(c, rrateio_step08_receita[c].cast(var_column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

rrateio_step08_receita.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

"""
Source rrateio_step01_receita is not needed anymore.
This object can be removed from cache.
"""
rrateio_step08_receita = rrateio_step08_receita.unpersist()