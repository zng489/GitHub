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
# MAGIC Processo	raw_biz_fta_kpi_ipca_kpi_pivot
# MAGIC Tabela/Arquivo Origem	/raw/crw/ibge/ipca
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_kpi_ipca_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção vindo do crawler - Base IBGE
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atualização	
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

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

# #USE THIS ONLY FOR DEVELOPMENT PURPOSES

# var_tables = {'origins': ['/crw/ibge/ipca'], 'destination': '/orcamento/fta_kpi_ipca_kpi_pivot', 'databricks': {'notebook': '/biz/visao_financeira_sesi_senai/raw_biz_fta_kpi_ipca_kpi_pivot'}}

# var_dls = {'folders': {'landing': '/lnd', 'error': '/err', 'staging': '/stg', 'log': '/log', 'raw': '/raw', 'trusted': '/trs', 'business': '/biz'}}

# var_adf = {'adf_factory_name': 'cnibigdatafactory', 
#        'adf_pipeline_name': 'biz_biz_fta_gestao_financeira_kpi_pivot',
#        'adf_pipeline_run_id': 'p1',
#        'adf_trigger_id': 't1',
#        'adf_trigger_name': 'author_dev',
#        'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
#        'adf_trigger_type': 'Manual'
#       }

# var_user_parameters = {'closing': {'year': 2020, 'month': 6, 'dt_closing': '2020-07-22'}}

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_ipca  = "{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],var_tables["origins"][0])
print(src_ipca)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from typing import Iterable 
from itertools import chain
from datetime import datetime, timedelta, date
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

def last_month_fn(year, month):
  first_day_of_month = date(year, month, 1)
  last_month = first_day_of_month - timedelta(days=1)  # this will never fail
  return last_month.strftime("%Y%m")

# COMMAND ----------

last_month_udf = f.udf(lambda y,m: last_month_fn(y,m), StringType())

# COMMAND ----------

var_parameters = {}
if "closing" in var_user_parameters:
  if "year" and "month" and "dt_closing" in var_user_parameters["closing"] :
    var_parameters["prm_ano_fechamento"] = var_user_parameters["closing"]["year"]
    var_parameters["prm_mes_fechamento"] = var_user_parameters["closing"]["month"]
    splited_date = var_user_parameters["closing"]["dt_closing"].split('-', 2)
    var_parameters["prm_data_corte"] = date(int(splited_date[0]), int(splited_date[1]), int(splited_date[2]))
else:
  #if there are no closing parameters, run for the previous month of the current month
  year_now = datetime.now().year
  month_now = datetime.now().month
  last_month = last_month_fn(year_now, month_now)
  var_parameters["prm_ano_fechamento"] = int(last_month[:4])
  var_parameters["prm_mes_fechamento"] = int(last_month[4:])
  var_parameters["prm_data_corte"] = datetime.now()
  
print(var_parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* Obter os parâmtros informados pela UNIGEST para fechamento do indice de inflação IPCA: 
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou obter #prm_ano_fechamento, #prm_mes_fechamento e
# MAGIC #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */ 
# MAGIC 
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 1: Obter os indices de inflação do mês corrente e dos últimos dozes meses:
# MAGIC - cuja data de inicio seja anterior ao último dia do mês de parâmetro #prm_ano_fechamento + #prm_mes_fechamenteo
# MAGIC - cuja data de fim seja posterior ao primeiro dia do ano de parâmetro #prm_ano_fechamento 
# MAGIC */ ====================================================================================================================
# MAGIC (
# MAGIC SELECT *
# MAGIC FROM 
# MAGIC (
# MAGIC select  
# MAGIC mes_ano, 
# MAGIC right(mes_ano, 4) as cd_ano_referencia, 
# MAGIC CASE
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JANEIRO' THEN 01
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'FEVEREIRO' THEN 02
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MARÇO' THEN 03
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'ABRIL' THEN 04
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MAIO' THEN 05
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JUNHO' THEN 06
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JULHO' THEN 07
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'AGOSTO' THEN 08
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'SETEMBRO' THEN 09
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'OUTUBRO' THEN 10
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'NOVEMBRO' THEN 11
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'DEZEMBRO' THEN 12
# MAGIC END AS cd_mes_referencia, 
# MAGIC right(mes_ano, 4) || lpad(
# MAGIC CASE
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JANEIRO' THEN 01
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'FEVEREIRO' THEN 02
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MARÇO' THEN 03
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'ABRIL' THEN 04
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MAIO' THEN 05
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JUNHO' THEN 06
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JULHO' THEN 07
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'AGOSTO' THEN 08
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'SETEMBRO' THEN 09
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'OUTUBRO' THEN 10
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'NOVEMBRO' THEN 11
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'DEZEMBRO' THEN 12
# MAGIC END, 2, '0') AS cd_ano_mes_apurado_atual,
# MAGIC CASE
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JANEIRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4) || 12
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'FEVEREIRO' THEN RIGHT(mes_ano, 4) || lpad(01, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MARÇO' THEN RIGHT(mes_ano, 4) || lpad(02, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'ABRIL' THEN RIGHT(mes_ano, 4) || lpad(03, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'MAIO' THEN RIGHT(mes_ano, 4) || lpad(04, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JUNHO' THEN RIGHT(mes_ano, 4) || lpad(05, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'JULHO' THEN RIGHT(mes_ano, 4) || lpad(06, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'AGOSTO' THEN RIGHT(mes_ano, 4) || lpad(07, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'SETEMBRO' THEN RIGHT(mes_ano, 4) || lpad(08, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'OUTUBRO' THEN RIGHT(mes_ano, 4) || lpad(09, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'NOVEMBRO' THEN RIGHT(mes_ano, 4) || lpad(10, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,  locate(' ', mes_ano) - 1)) = 'DEZEMBRO' THEN RIGHT(mes_ano, 4) || lpad(11, 2, '0')
# MAGIC END AS cd_ano_mes_apurado_ant,
# MAGIC CASE
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'JANEIRO' THEN substring((RIGHT(mes_ano, 4) - 2), 1, 4) || 12
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'FEVEREIRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4) || lpad(01, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'MARÇO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(02, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'ABRIL' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(03, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'MAIO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(04, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'JUNHO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(05, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'JULHO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(06, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'AGOSTO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(07, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'SETEMBRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(08, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'OUTUBRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(09, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'NOVEMBRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(10, 2, '0')
# MAGIC WHEN UPPER(substring(mes_ano, 1,locate(' ', mes_ano) - 1)) = 'DEZEMBRO' THEN substring((RIGHT(mes_ano, 4) - 1), 1, 4)  || lpad(11, 2, '0')
# MAGIC END AS cd_ano_mes_apurado_doze_meses,
# MAGIC valor_ipca
# MAGIC from ipca
# MAGIC where cd_ano_mes_apurado_atual <= #prm_ano_fechamento || #prm_mes_fechamento
# MAGIC order by cd_ano_mes_apurado_atual desc
# MAGIC ) SELECAO
# MAGIC ```

# COMMAND ----------

df_selecao = spark.read.parquet(src_ipca)\
.select("valor_ipca", "mes_ano", "dh_arq_in")\
.withColumn("mes_ano", f.trim(f.col("mes_ano")))\
.withColumn("cd_ano_referencia", f.split(f.col("mes_ano"), " ").getItem(1).cast("int"))\
.withColumn("cd_mes_referencia", 
            f.when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "JANEIRO", 1)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "FEVEREIRO", 2)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "MARÇO", 3)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "ABRIL", 4)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "MAIO", 5)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "JUNHO", 6)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "JULHO", 7)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "AGOSTO", 8)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "SETEMBRO", 9)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "OUTUBRO", 10)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "NOVEMBRO", 11)\
             .when(f.upper(f.split(f.col("mes_ano"), " ").getItem(0)) == "DEZEMBRO", 12)\
             .otherwise(f.lit(None)))\
.withColumn("cd_ano_mes_apurado_atual", f.concat(f.col("cd_ano_referencia"), f.lpad(f.col("cd_mes_referencia"), 2, "0")))\
.withColumn("cd_ano_mes_apurado_ant", last_month_udf(f.col("cd_ano_referencia"), f.col("cd_mes_referencia")))\
.withColumn("cd_ano_mes_apurado_doze_meses", f.concat(f.col("cd_ano_referencia") - 1, f.lpad(f.col("cd_mes_referencia"), 2, "0")))\
.filter(f.col("cd_ano_mes_apurado_atual") <= f.concat(f.lit(var_parameters["prm_ano_fechamento"]), f.lpad(f.lit(var_parameters["prm_mes_fechamento"]), 2, "0")))\
.drop("mes_ano")\
.coalesce(1)\
.cache()

df_selecao.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 2: Efetua o calculo do indice de inflação do mês corrente e dos últimos 12 meses
# MAGIC */ ====================================================================================================================
# MAGIC 
# MAGIC select 
# MAGIC   a.cd_ano_referencia
# MAGIC , a.cd_mes_referencia
# MAGIC , a.cd_ano_mes_apurado_atual AS cd_ano_mes_apurado_atual
# MAGIC , b.cd_ano_mes_apurado_atual AS cd_ano_mes_apurado_ant
# MAGIC , c.cd_ano_mes_apurado_atual AS cd_ano_mes_apurado_doze_meses
# MAGIC , a.valor_ipca as vl1
# MAGIC , b.valor_ipca as vl2
# MAGIC , c.valor_ipca as vl3
# MAGIC , ((a.valor_ipca / b.valor_ipca) - 1) * 100 AS qt_valor_apurado_mes
# MAGIC , ((a.valor_ipca / c.valor_ipca) - 1) * 100 AS qt_valor_apurado_doze_meses
# MAGIC FROM passo1_ipca a
# MAGIC INNER JOIN passo1_ipca b ON a.cd_ano_mes_apurado_ant = b.cd_ano_mes_apurado_atual 
# MAGIC INNER JOIN passo1_ipca c ON a.cd_ano_mes_apurado_doze_meses = c.cd_ano_mes_apurado_atual 
# MAGIC where a.cd_ano_referencia = #prm_ano_fechamento and a.cd_mes_referencia = #prm_mes_fechamento
# MAGIC ```

# COMMAND ----------

df_ipca = df_selecao.alias("a")\
.join(df_selecao.alias("b"), f.col("a.cd_ano_mes_apurado_ant") == f.col("b.cd_ano_mes_apurado_atual"), "inner")\
.join(df_selecao.alias("c"), f.col("a.cd_ano_mes_apurado_doze_meses") == f.col("c.cd_ano_mes_apurado_atual"), "inner")\
.select(f.col("a.cd_ano_referencia").alias("cd_ano_referencia"),
        f.col("a.cd_mes_referencia").alias("cd_mes_referencia"),    
        f.col("a.dh_arq_in").alias("dt_referencia").cast("date"),    
        f.col("a.valor_ipca").alias("vl_a"),
        f.col("b.valor_ipca").alias("vl_b"),
        f.col("c.valor_ipca").alias("vl_c"))\
.withColumn("pc_indice_inflacao_mes", 
           (((f.col("vl_a") / f.col("vl_b")) - 1) * 100).cast("decimal(18,6)"))\
.withColumn("pc_indice_inflacao_doze_meses", 
           (((f.col("vl_a") / f.col("vl_c")) - 1) * 100).cast("decimal(18,6)"))\
.withColumn("cd_periodicidade", f.lit("M"))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.filter((f.col("cd_ano_referencia") == f.col("cd_ano_fechamento")) &\
        (f.col("cd_mes_referencia") == f.col("cd_mes_fechamento")))\
.drop("vl_a", "vl_b", "vl_c")\
.coalesce(1)\
.cache()

df_ipca.count()

# COMMAND ----------

#display(df_ipca)

# COMMAND ----------

df_selecao = df_selecao.unpersist()

# COMMAND ----------

if df_ipca.count()==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df_ipca. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

var_pc_columns = [column for column in df_ipca.columns if column.startswith('pc_')]

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

var_keep_columns = ['cd_ano_fechamento', 'cd_mes_fechamento', 'dt_fechamento','cd_periodicidade', 'cd_ano_referencia', 'cd_mes_referencia', 'dt_referencia']

# COMMAND ----------

df = melt(df_ipca, var_keep_columns, var_pc_columns, 'cd_metrica', 'vl_metrica')

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

df_ipca = df_ipca.unpersist()