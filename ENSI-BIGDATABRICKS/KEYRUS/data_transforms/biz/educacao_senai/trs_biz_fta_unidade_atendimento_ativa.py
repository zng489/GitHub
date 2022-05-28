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
# MAGIC Processo	trs_biz_fta_unidade_atendimento_ativa
# MAGIC Tabela/Arquivo Origem	"/trs/mtd/corp/unidade_atendimento
# MAGIC /trs/mtd/corp/unidade_atendimento_caracteristica"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_unidade_atendimento_ativa
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores das unidades de atendimentos fixas e movéis ativas no período
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
var_tables = {"origins": ["/mtd/corp/unidade_atendimento",
                          "/mtd/corp/unidade_atendimento_caracteristica"],
              "destination": "/producao/fta_unidade_atendimento_ativa",
              "databricks": {
                "notebook": "/biz/educacao_senai/trs_biz_fta_unidade_atendimento_ativa"
              }
             }

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/trs",
    "business": "/biz"
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'trs_biz_fta_unidade_atendimento_ativa',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'marcela',
       'adf_trigger_time': '2020-11-10T18:25:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 10, "dt_closing": "2020-11-10"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_uni, src_unic = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_uni, src_unic)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get parameters prm_ano_fechamento, prm_mes_fechamento and prm_data_corte

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION <b>
# MAGIC ```
# MAGIC /*  
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento da Produção Unidades Fixas e Móveis
# MAGIC do SENAI do mês: #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */ 
# MAGIC ```

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
# MAGIC ### Step 1

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC Contabilizar por cd_entidade_regional, c.cd_tipo_descricao para um determinado ano de fechamento:
# MAGIC - Unidades Ativas Fixas
# MAGIC    - qt_unidade_fixa
# MAGIC - Unidades Ativas Móveis
# MAGIC    - qt_unidade_movel
# MAGIC */
# MAGIC      SELECT 
# MAGIC    #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC    #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC    #prm_data_corte AS dt_fechamento,
# MAGIC    u.cd_entidade_regional,
# MAGIC    c.cd_tipo_descricao,
# MAGIC    CASE
# MAGIC    WHEN c.cd_tipo_categoria_ativo = 1
# MAGIC    THEN count(u.cd_unidade_atendimento_dr) END AS qt_unidade_fixa,  --- alterado
# MAGIC    CASE
# MAGIC    WHEN c.cd_tipo_categoria_ativo = 2
# MAGIC    THEN count(u.cd_unidade_atendimento_dr) END AS qt_unidade_movel   --- alterado
# MAGIC    
# MAGIC    FROM unidade_atendimento u
# MAGIC    INNER JOIN 
# MAGIC    (
# MAGIC select c.cd_unidade_atendimento_dr, c.dt_inicio_vigencia, c.dt_fim_vigencia, c.fl_excluido_oltp, c.cd_tipo_categoria_ativo, c.cd_tipo_descricao, c.fl_ativo, 
# MAGIC c.cd_status, ROW_NUMBER() OVER(PARTITION BY c.cd_unidade_atendimento_dr ORDER BY c.dt_inicio_vigencia DESC) SQ   --- alterado 01/12/2020
# MAGIC from unidade_atendimento_caracteristica c
# MAGIC where CAST(s.dh_inicio_vigencia AS DATE) <= #prm_data_corte 
# MAGIC and   (CAST(c.dt_fim_vigencia AS DATE) >= #prm_data_corte OR c.dt_fim_vigencia IS NULL)) c
# MAGIC    ON u.cd_unidade_atendimento_dr = c.cd_unidade_atendimento_dr and SQ = 1
# MAGIC 
# MAGIC    -- Unidades Ativas no ano até a data de corte
# MAGIC    WHERE 
# MAGIC    c.fl_excluido_oltp = 0 
# MAGIC    AND   c.fl_ativo = 'A'
# MAGIC    AND   c.cd_status = 15  --- incluido em 01/12/2020
# MAGIC    AND   c.cd_tipo_categoria_ativo in  (1, 2)
# MAGIC    GROUP BY  u.cd_entidade_regional, c.cd_tipo_descricao, c.cd_tipo_categoria_ativo --- alterado
# MAGIC ```

# COMMAND ----------

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from unidade_atendimento_caracteristica and apply filters

# COMMAND ----------

useful_columns_unidade_atendimento_caracteristica = ["cd_tipo_descricao", "cd_unidade_atendimento_dr", "dt_inicio_vigencia", "dt_fim_vigencia", "fl_ativo", "cd_tipo_categoria_ativo", "fl_excluido_oltp", "cd_status"]

# COMMAND ----------

var_window = Window.partitionBy("cd_unidade_atendimento_dr").orderBy(f.col("dt_inicio_vigencia").desc())

df_uni_atend_car = spark.read.parquet(src_unic)\
.select(*useful_columns_unidade_atendimento_caracteristica)\
.filter((f.col("dt_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) & \
        ((f.col("dt_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
         (f.col("dt_fim_vigencia").isNull())))\
.withColumn("rank", f.row_number().over(var_window))\
.filter(f.col("rank") == 1)\
.drop("dt_inicio_vigencia", "dt_fim_vigencia", "rank")

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from unidade_atendimento and apply filters

# COMMAND ----------

useful_columns_unidade_atendimento = ["cd_entidade_regional", "cd_unidade_atendimento_dr"]

# COMMAND ----------

df_uni_atend = spark.read.parquet(src_uni)\
.select(*useful_columns_unidade_atendimento)

# COMMAND ----------

# MAGIC %md
# MAGIC Join matricula_ensino_prof_situacao with the matricula_ensino_profissional 

# COMMAND ----------

df = df_uni_atend.join(df_uni_atend_car, ["cd_unidade_atendimento_dr"], "inner")\
.withColumn("qt_unidade_fixa", 
            f.when(f.col("cd_tipo_categoria_ativo") == 1, f.lit(1).cast("int"))\
             .otherwise(f.lit(0).cast("int")))\
.withColumn("qt_unidade_movel", 
            f.when(f.col("cd_tipo_categoria_ativo") == 2, f.lit(1).cast("int"))\
             .otherwise(f.lit(0).cast("int")))\
.filter((f.col("fl_excluido_oltp") == 0) &\
        (f.lower(f.col("fl_ativo")) == "a") &\
        (f.col("cd_status") == 15) &\
        (f.col("cd_tipo_categoria_ativo").isin(1,2)))\
.groupBy("cd_entidade_regional", "cd_tipo_descricao", "cd_tipo_categoria_ativo")\
.agg(f.sum("qt_unidade_fixa").alias("qt_unidade_fixa"),
     f.sum("qt_unidade_movel").alias("qt_unidade_movel"))\
.drop("cd_tipo_categoria_ativo")

# COMMAND ----------

# MAGIC %md
# MAGIC Add columns

# COMMAND ----------

var_mapping = {"cd_ano_fechamento": f.lit(var_parameters["prm_ano_fechamento"]).cast("int"),
               "cd_mes_fechamento": f.lit(var_parameters["prm_mes_fechamento"]).cast("int"),
               "dt_fechamento": f.lit(var_parameters["prm_data_corte"]).cast("date")}

for cl in var_mapping:
  df = df.withColumn(cl, var_mapping[cl])

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write on ADLS

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

# MAGIC %md
# MAGIC <pre>
# MAGIC Dynamic overwrite will guarantee that only this view will be updated by cd_ano_fechamento and cd_mes_fechamento
# MAGIC We use coalesce equals to 4 to avoid saving multiple files on adls
# MAGIC </pre>

# COMMAND ----------

df.coalesce(1).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")