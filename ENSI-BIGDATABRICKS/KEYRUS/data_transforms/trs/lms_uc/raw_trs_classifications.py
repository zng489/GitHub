# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import re
import json

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

src_tb = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_tb)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, desc, greatest, trim, when, col, lit
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from curso_ensino_profissional
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_atualizacao = df_sink.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First thing is to apply filtering for the maximum available var_max_dt_atualizacao.

# COMMAND ----------

classifications = spark.read.parquet(src_tb)


# COMMAND ----------

type(classifications)

# COMMAND ----------

column_name_mapping = {"classification_id": "id_iniciativa", "classification_name": "nm_iniciativa", "classification_type": "tipo_iniciativa"}
  
for key in column_name_mapping:
  classifications =  classifications.withColumnRenamed(key, column_name_mapping[key])

classifications = classifications.select("id_iniciativa", "nm_iniciativa", "tipo_iniciativa")

# COMMAND ----------

if first_load is True:
  df_sink = spark.createDataFrame([], schema=classifications.schema)

# COMMAND ----------

classifications = classifications.select(df_sink.columns)

# COMMAND ----------

df_sink = df_sink.union(classifications)

# COMMAND ----------

df_sink.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------


