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
# MAGIC raw_biz_fta_kpi_escola_sesi_nivel_avancado
# MAGIC "/usr/unigest/kpi_escola_sesi_nivel_avancado
# MAGIC /trs/mtd/corp/entidade_regional
# MAGIC /trs/mtd/corp/produto_servico_educacao_sesi"
# MAGIC /biz/orcamento/fta_kpi_escola_sesi_nivel_avancado
# MAGIC cd_ano_fechamento / cd_mes_fechamento
# MAGIC Indicadores básicos de produção que não possuem origem sistêmica.
# MAGIC P = substituição parcial (delete/insert)
# MAGIC trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
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
var_tables = {"origins": ["/usr/unigest/kpi_escola_sesi_nivel_avancado",                        
                          "/mtd/corp/entidade_regional",
                          "/mtd/sesi/produto_servico_educacao_sesi"],
              "destination": "/orcamento/fta_kpi_escola_sesi_nivel_avancado",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/raw_biz_fta_kpi_escola_sesi_nivel_avancado"
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
       'adf_pipeline_name': 'trs_biz_fta_gestao_financeira',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': '3d54fd35ae9c4bfea99c5c140625c87a',
       'adf_trigger_name': 'Manual',
       'adf_trigger_time': '2020-06-09T17:22:07.834217Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {'closing': {'year': 2019, 'dt_closing': '2021-01-15', 'month': 6}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_esc = "{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"], var_tables["origins"][0])
print(src_esc)

# COMMAND ----------

src_reg, src_pro = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], t) for t in var_tables["origins"][1:3]]
print(src_reg, src_pro)

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
# MAGIC /*  
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento mensal metas
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */ 
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
# MAGIC a.cd_produto_servico_educacao_sesi,
# MAGIC a.sg_disciplina_avaliada,
# MAGIC a.qt_calc_escola_avaliada,
# MAGIC a.qt_calc_escola_avaliada_avancado,
# MAGIC a.dh_referencia
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT
# MAGIC     'A' AS cd_periodicidade,
# MAGIC     sg_entidade_regional,
# MAGIC     cd_ano_fechamento  AS cd_ano_referencia,
# MAGIC     12  AS cd_mes_referencia, 
# MAGIC 	cd_produto_servico_educacao_sesi,
# MAGIC     sg_disciplina_avaliada,
# MAGIC 	NVL(qt_calc_escola_avaliada, 0)         AS qt_calc_escola_avaliada,
# MAGIC     NVL(qt_calc_escola_avaliada_avancado,0) AS qt_calc_escola_avaliada_avancado,
# MAGIC     dh_arq_in AS dh_referencia,
# MAGIC     ROW_NUMBER() OVER(PARTITION BY sg_entidade_regional, cd_produto_servico_educacao_sesi, sg_disciplina_avaliada
# MAGIC     	              ORDER BY cd_ano_fechamento DESC, dh_arq_in DESC) AS SQ  
# MAGIC 	FROM kpi_escola_sesi_nivel_avancado
# MAGIC     WHERE cd_ano_fechamento <= #prm_ano_fechamento
# MAGIC     AND   CAST(dh_arq_in AS DATE) <= #prm_data_fechamento
# MAGIC ) WHERE SQ = 1
# MAGIC LEFT JOIN entidade_regional b ON a.sg_entidade_regional = b.sg_entidade_regional
# MAGIC INNER JOIN produto_servico_educacao_sesi c ON a.cd_produto_servico_educacao_sesi = c.cd_produto_servico_educacao_sesi
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
# MAGIC ##### produto_servico_educacao_sesi (aux)

# COMMAND ----------

df_pro = spark.read.parquet(src_pro)\
.select("cd_centro_responsabilidade", "cd_produto_servico_educacao_sesi")\
.coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### kpi_escola_sesi_nivel_avancado

# COMMAND ----------

if cf.directory_exists(dbutils, src_esc.replace(var_adls_uri, '')):
  esc_columns = ["sg_entidade_regional", "qt_calc_escola_avaliada", "qt_calc_escola_avaliada_avancado", "sg_disciplina_avaliada", "cd_produto_servico_educacao_sesi", "cd_ano_fechamento", "dh_arq_in"]

  df = spark.read.parquet(src_esc)\
  .select(esc_columns)\
  .filter((f.col("cd_ano_fechamento") <= var_parameters["prm_ano_fechamento"]) &\
          (f.col("dh_arq_in").cast("date") <= var_parameters["prm_data_corte"]))\
  .withColumn("sq", f.row_number().over(
    Window.partitionBy("sg_entidade_regional", "cd_produto_servico_educacao_sesi", "sg_disciplina_avaliada").orderBy(f.col("cd_ano_fechamento").desc(), f.col("dh_arq_in").desc()))
             )\
  .withColumn("cd_periodicidade", f.lit("A"))\
  .withColumnRenamed("dh_arq_in", "dh_referencia")\
  .withColumnRenamed("cd_campo_metrica", "cd_metrica")\
  .withColumnRenamed("cd_ano_fechamento", "cd_ano_referencia")\
  .withColumn("cd_mes_referencia", f.lit(12))\
  .fillna(0, subset=["qt_calc_escola_avaliada",
                    "qt_calc_escola_avaliada_avancado"])\
  .coalesce(1)

else:  
  esc_schema = StructType([StructField("sg_entidade_regional", StringType()),
                            StructField("qt_calc_escola_avaliada", IntegerType()),
                            StructField("qt_calc_escola_avaliada_avancado", IntegerType()),
                            StructField("sg_disciplina_avaliada", StringType()),
                            StructField("cd_produto_servico_educacao_sesi", IntegerType()),
                            StructField("cd_ano_referencia", IntegerType()),
                            StructField("dh_referencia", TimestampType()),
                            StructField("sq", IntegerType()),
                            StructField("cd_periodicidade", StringType()),
                            StructField("cd_mes_referencia", IntegerType())])
  
  
  df = spark.createDataFrame(spark.sparkContext.emptyRDD(),esc_schema) 
    

# COMMAND ----------

df = df.filter(f.col("sq") == 1)\
.join(df_pro, ["cd_produto_servico_educacao_sesi"], "inner")\
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
        "cd_centro_responsabilidade",
        "cd_produto_servico_educacao_sesi",
        "cd_periodicidade",
        "sg_disciplina_avaliada",
        "cd_ano_referencia",
        "cd_mes_referencia",
        "dh_referencia",
        "qt_calc_escola_avaliada",
        "qt_calc_escola_avaliada_avancado")\
.coalesce(1)

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

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

var_column_type_map = {"cd_ano_fechamento":"int",
                       "cd_mes_fechamento":"int",
                       "dt_fechamento":"date",
                       "cd_entidade_regional":"int",
                       "cd_centro_responsabilidade":"string",
                       "cd_produto_servico_educacao_sesi":"int",
                       "cd_periodicidade":"string",
                       "sg_disciplina_avaliada":"string",
                       "cd_ano_referencia":"int",
                       "cd_mes_referencia":"int",
                       "dh_referencia":"timestamp",
                       "qt_calc_escola_avaliada":"int",
                       "qt_calc_escola_avaliada_avancado":"int"}

for c in var_column_type_map:
  df = df.withColumn(c, df[c].cast(var_column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

df.coalesce(1).write.partitionBy(["cd_ano_fechamento", "cd_mes_fechamento"]).save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

