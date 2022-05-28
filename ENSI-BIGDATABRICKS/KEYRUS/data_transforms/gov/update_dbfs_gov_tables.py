# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

dbutils.widgets.text("env","dev")
dbutils.widgets.text("gov_table","source")

# COMMAND ----------

env = dbutils.widgets.get("env")
gov_table = dbutils.widgets.get("gov_table")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""

# COMMAND ----------

path_table = "{adl_path}{default_dir}/gov/tables/{gov_table}".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)
path_dbfs_table = "/dbfs/gov/{env}/dbfs_tables/{gov_table}".format(env=env,gov_table=gov_table)

# COMMAND ----------

df_table = spark.read.format("delta").load(path_table)

# COMMAND ----------

df_table.write.format("delta").mode("overwrite").save(path_dbfs_table)

# COMMAND ----------

from pyspark.sql.functions import upper, col, concat_ws
from pyspark.sql.types import StructType

# COMMAND ----------

key = ""

if gov_table == "source":
  df = df_table.withColumn("key_source_schema", col("source_name"))

if gov_table == "schema":
  df = df_table \
               .withColumn("key_source_schema", upper(col("source_name"))) \
               .withColumn("key_schema_table",  concat_ws('|', col("source_name"), col("schema_name"))) 
elif gov_table == "table":
  df = df_table \
               .withColumn("key_schema_table",  concat_ws('|', col("source_name"), col("schema_name"))) \
               .withColumn("key_table_field",   concat_ws('|', col("source_name"), col("schema_name"), col("table_name"))) \
               .withColumn("key_table_curador",   col("cod_data_steward"))
elif gov_table == "field":
  df = df_table \
               .withColumn("key_table_field",   concat_ws('|', col("source_name"), col("schema_name"), col("table_name"))) \
               .withColumn("key_field_profiling",   concat_ws('|', col("source_name"), col("schema_name"), col("table_name"), col("field_name")))
elif gov_table == "profiling":
  df = df_table \
               .withColumn("key_field_profiling",   concat_ws('|', col("source_name"), col("schema_name"), col("table_name"), col("field_name")))
elif gov_table == "data_steward":
  df = df_table \
               .withColumn("key_table_curador",   col("cod_data_steward"))
else: 
  spark.createDataFrame([], StructType([]))

# COMMAND ----------

# gava os dados na pasta "_fontes" no formato parquet para fonte de dados no PBI
path_fonte = "{adl_path}{default_dir}/gov/tables/_fontes/{gov_table}".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)
df.write.format("parquet").mode("overwrite").save(path_fonte)

# COMMAND ----------

path_fonte
