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
# MAGIC Processo	trs_biz_fte_producao_acao_educativa_sesi
# MAGIC Tabela/Arquivo Origem	"/trs/evt/participacao_acao_educativa_sesi
# MAGIC /trs/evt/participacao_acao_educativa_sesi_caracteristica
# MAGIC /trs/mtd/sesi/acao_educativa_sesi"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fte_producao_acao_educativa_sesi
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção SESI de participações anuais em ações educativas promovidas pela entidade
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
var_tables = {"origins": ["/evt/participacao_acao_educativa_sesi",
                          "/evt/participacao_acao_educativa_sesi_caracteristica"],
              "destination": "/producao/fte_producao_acao_educativa_sesi",
              "databricks": {
                "notebook": "/biz/educacao_sesi/trs_biz_fte_producao_acao_educativa_sesi"
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
       'adf_pipeline_name': 'trs_biz_fta_producao_educacao_sesi',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-27T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 6, "dt_closing": "2020-07-16"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_paes, src_paesc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_paes, src_paesc)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, to_date, from_json
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
# MAGIC <pre>
# MAGIC /*  
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento da Produção de Matrículas em Ensino do SESI: 
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */ 
# MAGIC </pre>

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
# MAGIC SELECT
# MAGIC #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC #prm_data_corte     AS dt_fechamento,
# MAGIC cd_entidade_regional,
# MAGIC cd_centro_responsabilidade,
# MAGIC cd_participacao_acao_educativa, 
# MAGIC cd_participacao_acao_educativa_dr,
# MAGIC 1 AS qt_participante_acao_educativa
# MAGIC FROM
# MAGIC ( SELECT c.cd_entidade_regional, c.cd_centro_responsabilidade, c.cd_participacao_acao_educativa, c.cd_participacao_acao_educativa_dr, pc.fl_excluido_oltp,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY pc.cd_participacao_acao_educativa ORDER BY pc.dh_inicio_vigencia DESC) SQ
# MAGIC   FROM participacao_acao_educativa_sesi_caracteristica pc
# MAGIC   INNER JOIN participacao_acao_educativa_sesi p ON pc.cd_participacao_acao_educativa_sesi = p.cd_participacao_acao_educativa_sesi	
# MAGIC   WHERE YEAR(p.dt_participacao) = #prm_ano_fechamento
# MAGIC   AND   CAST(pc.dh_inicio_vigencia AS DATE) <= #prm_data_corte 
# MAGIC   AND (CAST(pc.dh_fim_vigencia AS DATE) >= #prm_data_corte OR pc.dh_fim_vigencia IS NULL)
# MAGIC ) WHERE SQ = 1 AND fl_excluido_oltp = 0
# MAGIC ```

# COMMAND ----------

useful_columns_participacao_acao_educativa_sesi_caracteristica = ["cd_participacao_acao_educativa", "fl_excluido_oltp", "dh_inicio_vigencia", "dh_fim_vigencia"]

# COMMAND ----------

df_paesc = spark.read.parquet(src_paesc)\
.select(*useful_columns_participacao_acao_educativa_sesi_caracteristica)\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) &\
        ((col("dh_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
         (col("dh_fim_vigencia").isNull())))\
.drop("dh_fim_vigencia")

# COMMAND ----------

useful_columns_participacao_acao_educativa_sesi = ["cd_entidade_regional", "cd_centro_responsabilidade", "cd_participacao_acao_educativa", "cd_participacao_acao_educativa_dr", "dt_participacao"]

# COMMAND ----------

df_paes = spark.read.parquet(src_paes)\
.select(*useful_columns_participacao_acao_educativa_sesi)\
.filter(year(col("dt_participacao")) == var_parameters["prm_ano_fechamento"])\
.drop("dt_participacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join dataframes

# COMMAND ----------

w = Window.partitionBy("cd_participacao_acao_educativa").orderBy(col("dh_inicio_vigencia").desc())

df = df_paesc\
.join(df_paes, ["cd_participacao_acao_educativa"], "inner")\
.withColumn("sq", row_number().over(w))\
.filter((col("sq") == 1) &\
        (col("fl_excluido_oltp") == 0))\
.withColumn("qt_participante_acao_educativa", lit(1))\
.drop("fl_excluido_oltp", "dh_inicio_vigencia", "sq")

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
# MAGIC Add columns

# COMMAND ----------

var_mapping = {"cd_ano_fechamento": lit(var_parameters["prm_ano_fechamento"]).cast("int"),
               "cd_mes_fechamento": lit(var_parameters["prm_mes_fechamento"]).cast("int"),
               "dt_fechamento": lit(var_parameters ["prm_data_corte"]).cast("date")}

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
# MAGIC Dynamic overwrite will guarantee that only this view will be updated by cd_ano_fechamento and cd_mes_fechamento
# MAGIC We use coalesce equals to 1 to avoid large files
# MAGIC </pre>

# COMMAND ----------

df.coalesce(4).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

