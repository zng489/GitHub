# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.window import Window

import crawler.functions as cf
import datetime
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])
lnd_path

# COMMAND ----------

dict_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table_dict"])
dict_path

# COMMAND ----------

adl_sink = "{adl_path}{raw}/crw/{schema}/{table}".format(adl_path=var_adls_uri, raw=raw, schema=table["schema"], table=table["table_dest"])
adl_sink

# COMMAND ----------

lnd_uri = '{adl}/{lnd}/*/*'.format(adl=var_adls_uri, lnd=lnd_path)
df_source = spark.read.text(lnd_uri)

# COMMAND ----------

lnd_dict_uri = '{adl}/{lnd}/'.format(adl=var_adls_uri, lnd=dict_path)

df_dict = spark.read.parquet(lnd_dict_uri)
df_dict = df_dict.withColumn('POSICAO_INICIAL', f.col('POSICAO_INICIAL').cast('Int'))
df_dict = df_dict.withColumn('TAMANHO', f.col('TAMANHO').cast('Int'))
df_dict = df_dict.select('POSICAO_INICIAL', 'TAMANHO', 'CODIGO_VARIAVEL').distinct()

# COMMAND ----------

lnd_dict_uri = '{adl}/{lnd}/'.format(adl=var_adls_uri, lnd=dict_path)
lnd_dict_uri

# COMMAND ----------

df_columns = ((row.POSICAO_INICIAL, f.substring(f.col('value'), row.POSICAO_INICIAL, row.TAMANHO).alias(row.CODIGO_VARIAVEL)) 
              for row in df_dict.collect())

df_columns = sorted(df_columns, key=lambda tp: tp[0])
df_columns = map(lambda tp: tp[1], df_columns)

df = df_source.select(*df_columns)

# COMMAND ----------

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, adl_list_dir=lnd_path, recursive=True)
df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')


df = df.withColumn('ano_trim', 
                    f.concat(f.col('ANO'), f.col('Trimestre')))

# COMMAND ----------

df.repartition(10).write.partitionBy('ano_trim').parquet(adl_sink, mode='overwrite')
