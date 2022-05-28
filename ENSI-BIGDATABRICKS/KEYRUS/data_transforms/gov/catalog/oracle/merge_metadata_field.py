# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("dir","")
dbutils.widgets.text("env","dev")

# COMMAND ----------

dir = dbutils.widgets.get("dir")
env = dbutils.widgets.get("env")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_lnd = "{adl_path}{default_dir}/lnd/gov/oracle/{dir}/".format(adl_path=var_adls_uri,default_dir=default_dir,dir=dir)
path_source = "{adl_path}{default_dir}/gov/tables/source".format(adl_path=var_adls_uri,default_dir=default_dir)
path_field = "{adl_path}{default_dir}/gov/tables/field".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

df_lnd = spark.read.format("parquet").option("inferSchema","true").load(path_lnd)
df_source_target = spark.read.format("delta").load(path_source)
df_field_target = spark.read.format("delta").load(path_field)

# COMMAND ----------

#remove as tabelas de backups contidas no data lake
df_lnd = df_lnd.filter('lower(table_name) not like ("%bkp%")')
df_field_target = df_field_target.filter('lower(table_name) not like ("%bkp%")')

# COMMAND ----------

df_field_lnd = df_lnd.withColumn("source_name",regexp_extract(input_file_name(),"(?<=\d\d\d\d\d\d\d\d/)(.*?)(?=_metadata)",1))\
                      .dropDuplicates(["owner","TABLE_NAME","COLUMN_NAME"])\
                      .withColumn("is_derivative",lit(0))\
                      .withColumn("created_at",current_timestamp())\
                      .withColumn("updated_at",current_timestamp())\
                      .select("source_name",\
                             col("OWNER").alias("schema_name"),\
                             col("TABLE_NAME").alias("table_name"),\
                             col("COLUMN_NAME").alias("field_name"),\
                             col("DATA_TYPE").alias("data_type"),\
                             "is_derivative","created_at","updated_at")

# COMMAND ----------

df_field_upsert = df_field_lnd.alias("field_ac").join(df_field_target.alias("field_target")\
                                                 ,(col("field_ac.source_name") == col("field_target.source_name"))\
                                                 & (col("field_ac.schema_name") == col("field_target.schema_name"))\
                                                 & (col("field_ac.table_name") == col("field_target.table_name"))\
                                                 & (col("field_ac.field_name") == col("field_target.field_name"))\
                                                 ,'leftouter')\
                                            .select("field_ac.*")

df_field_merge = df_field_upsert

# COMMAND ----------

delta_field = DeltaTable.forPath(spark, path_field)

source_set = {
  "source_name": "upsert.source_name",
  "source_type": lit("oracle"),
  "schema_name": "upsert.schema_name",
  "table_name": "upsert.table_name",
  "field_name": "upsert.field_name",
  "data_type": "upsert.data_type",
  "created_at": "upsert.created_at",
  "is_derivative": "upsert.is_derivative",
  "updated_at": "upsert.updated_at"
}

delta_field.alias("target").merge(
           df_field_merge.alias("upsert"),
           "target.source_name = upsert.source_name and target.schema_name = upsert.schema_name and \
           target.table_name = upsert.table_name and target.field_name = upsert.field_name")\
           .whenNotMatchedInsert(values=source_set)\
           .whenMatchedUpdate(set=source_set)\
           .execute()

# COMMAND ----------

#try:
#  dbutils.notebook.run("/KEYRUS/{env}/gov/catalog/generate_desc_csv".format(env=env),0,{"gov_table": "field","source_type": "metadata","env": env})
#except:
#  print("Arquivo template CSV não gerado")