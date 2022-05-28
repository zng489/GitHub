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
# MAGIC Processo	raw_biz_fta_kpi_meta_kpi_pivot
# MAGIC Tabela/Arquivo Origem	"/usr/unigest/kpi_meta
# MAGIC '/trs/mtd/corp/entidade_regional"
# MAGIC Tabela/Arquivo Destino	/biz/orcamento/fta_kpi_meta_kpi_pivot
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção que não possuem origem sistêmica.
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Manutanção:
# MAGIC   2021/01/08 - Thomaz Moreira - Aplicação de novos filtros de condição
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
var_tables = {"origins": ["/usr/unigest/kpi_meta",                        
                          "/mtd/corp/entidade_regional"],
              "destination": "/orcamento/fta_kpi_meta_kpi_pivot",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/raw_biz_fta_kpi_meta_kpi_pivot"
              }
             }

# Will read from production data but write in dev. That's why I'm messing with the dict.
var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/trs",
    "business": "/tmp/dev/biz"
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_biz_fta_kpi_meta_kpi_pivot',
       'adf_pipeline_run_id': 'p1_dev',
       'adf_trigger_id': '3d54fd35ae9c4bfea99c5c140625c87a',
       'adf_trigger_name': 'Manual',
       'adf_trigger_time': '2021-01-12T11:50:00.834217Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {'closing': {'year': 2019, 'dt_closing': '2021-01-12', 'month': 12}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_met = "{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"], var_tables["origins"][0])
print(src_met)

# COMMAND ----------

src_reg = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],var_tables["origins"][-1])
print(src_reg)

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
from pyspark.sql.window import Window
from trs_control_field import trs_control_field as tcf
import crawler.functions as cf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DecimalType

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
# MAGIC SELECT
# MAGIC #prm_ano_fechamento  AS cd_ano_fechamento,
# MAGIC #prm_mes_fechamento  AS cd_mes_fechamento,
# MAGIC #prm_data_fechamento AS dt_fechamento,
# MAGIC CASE WHEN a.sg_entidade_regional = 'SENAI-BR' THEN -3
# MAGIC      WHEN a.sg_entidade_regional = 'SESI-BR'  THEN -2 
# MAGIC      ELSE ISNULL(c.cd_entidade_regional, -98) 
# MAGIC END AS cd_entidade_regional,
# MAGIC a.sg_entidade_regional,
# MAGIC a.cd_periodicidade,
# MAGIC a.cd_ano_referencia,
# MAGIC a.cd_mes_referencia,
# MAGIC a.dh_referencia,
# MAGIC a.cd_metrica,
# MAGIC a.vl_metrica
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT
# MAGIC     'A' AS cd_periodicidade,
# MAGIC     sg_entidade_regional,
# MAGIC     cd_campo_metrica as cd_metrica,
# MAGIC     isnull(a.vl_metrica, 0) as vl_metrica,
# MAGIC     cd_ano_fechamento  AS cd_ano_referencia,
# MAGIC     1  AS cd_mes_referencia, 
# MAGIC     dh_arq_in AS dh_referencia 
# MAGIC     FROM
# MAGIC     (    SELECT 
# MAGIC     	    sg_entidade_regional,
# MAGIC     	    cd_campo_metrica,
# MAGIC     	    vl_metrica,
# MAGIC     	    cd_ano_fechamento, 
# MAGIC     	    dh_arq_in, 
# MAGIC     	    row_number() OVER(PARTITION BY  sg_entidade_regional, cd_campo_metrica, cd_ano_fechamento
# MAGIC     	                      ORDER BY dh_arq_in DESC) AS SQ
# MAGIC          FROM kpi_meta
# MAGIC     	    WHERE cd_ano_fechamento = #prm_ano_fechamento
# MAGIC          AND   CAST(dh_arq_in AS DATE) <= #prm_data_fechamento
# MAGIC     ) WHERE SQ = 1
# MAGIC ) a
# MAGIC LEFT JOIN entidade_regional c ON a.sg_entidade_regional = c.sg_entidade_regional
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_entidade_regional (aux)

# COMMAND ----------

df_reg = spark.read.parquet(src_reg)\
.select("sg_entidade_regional", "cd_entidade_regional")\
.coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### kpi_meta

# COMMAND ----------

if  cf.directory_exists(dbutils, src_met.replace(var_adls_uri, '')):
  meta_columns = ["sg_entidade_regional", "cd_campo_metrica", "vl_metrica", "cd_ano_fechamento", "dh_arq_in"]
  
  df = spark.read.parquet(src_met)\
  .select(*meta_columns)\
  .filter((f.col("cd_ano_fechamento") == var_parameters["prm_ano_fechamento"]) &\
          (f.col("dh_arq_in").cast("date") <= var_parameters["prm_data_corte"]))\
  .withColumn("cd_campo_metrica", f.regexp_replace(f.col("cd_campo_metrica"), "\\s+", ""))\
  .withColumn("sq", f.row_number().over(Window.partitionBy("sg_entidade_regional", "cd_campo_metrica", "cd_ano_fechamento").orderBy(f.col("dh_arq_in").desc())))\
  .withColumn("cd_periodicidade", f.lit("A"))\
  .withColumnRenamed("dh_arq_in", "dh_referencia")\
  .withColumnRenamed("cd_campo_metrica", "cd_metrica")\
  .withColumnRenamed("cd_ano_fechamento", "cd_ano_referencia")\
  .withColumn("cd_mes_referencia", f.lit(1))\
  .fillna(0, subset=["vl_metrica"])\
  .coalesce(1)

else:
  meta_schema = StructType([StructField("sg_entidade_regional", StringType()),
                            StructField("cd_metrica", StringType()),
                            StructField("vl_metrica", DecimalType(18,6)),
                            StructField("cd_ano_referencia", IntegerType()),
                            StructField("dh_referencia", TimestampType()),
                            StructField("sq", IntegerType()),
                            StructField("cd_periodicidade", StringType()),
                            StructField("cd_mes_referencia", IntegerType())])
  
  df = spark.createDataFrame(spark.sparkContext.emptyRDD(),meta_schema) 
    

# COMMAND ----------

df = df.filter(f.col("sq") == 1)\
.join(df_reg, ["sg_entidade_regional"], "left")\
.withColumn("cd_entidade_regional",
            f.when(f.upper(f.col("sg_entidade_regional")) == 'SENAI-BR', f.lit(-3))\
            .when(f.upper(f.col("sg_entidade_regional")) == 'SESI-BR', f.lit(-2))\
            .otherwise(f.col("cd_entidade_regional")))\
.withColumn("cd_ano_fechamento", f.lit(var_parameters["prm_ano_fechamento"]).cast("int"))\
.withColumn("cd_mes_fechamento", f.lit(var_parameters["prm_mes_fechamento"]).cast("int"))\
.withColumn("dt_fechamento", f.lit(var_parameters["prm_data_corte"]).cast("date"))\
.fillna('-98', subset=["cd_entidade_regional"])\
.drop("sq", "sg_entidade_regional")\
.select("cd_ano_fechamento",
        "cd_mes_fechamento",
        "dt_fechamento",
        "cd_entidade_regional",
        "cd_periodicidade",
        "cd_ano_referencia",
        "cd_mes_referencia",
        "dh_referencia",
        "cd_metrica",
        "vl_metrica")\
.coalesce(1)

# COMMAND ----------

# Reusing df_count for efficiency.
if df.count() == 0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

var_column_type_map = {"cd_ano_referencia":"int",
                       "cd_mes_referencia":"int",
                       "cd_metrica":"string",
                       "vl_metrica":"decimal(18,6)",
                       "cd_entidade_regional":"int",
                       "cd_periodicidade":"string",
                       "dh_referencia":"timestamp",
                       "cd_ano_fechamento":"int",
                       "cd_mes_fechamento":"int",
                       "dt_fechamento":"date"}

for c in var_column_type_map:
  df = df.withColumn(c, df[c].cast(var_column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

