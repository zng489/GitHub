# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("dir","")
dbutils.widgets.text("env","dev")
dir = dbutils.widgets.get("dir")
env = dbutils.widgets.get("env")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_lnd = "{adl_path}{default_dir}/lnd/gov/oracle/{dir}/".format(adl_path=var_adls_uri,default_dir=default_dir,dir=dir)
path_source = "{adl_path}{default_dir}/gov/tables/source".format(adl_path=var_adls_uri,default_dir=default_dir)
path_table = "{adl_path}{default_dir}/gov/tables/table".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

df_lnd = spark.read.format("parquet").option("inferSchema","true").load(path_lnd)
df_source_target = spark.read.format("delta").load(path_source)
df_table_target = spark.read.format("delta").load(path_table)

# COMMAND ----------

#remove as tabelas de backups contidas no data lake
df_lnd = df_lnd.filter('lower(table_name) not like ("%bkp%")')
df_table_target = df_table_target.filter('lower(table_name) not like ("%bkp%")')

# COMMAND ----------

df_table_lnd = df_lnd.withColumn("source_name",regexp_extract(input_file_name(),"(?<=\d\d\d\d\d\d\d\d/)(.*?)(?=_metadata)",1))\
                      .dropDuplicates(["owner","table_name"])\
                      .withColumn("created_at",current_timestamp())\
                      .withColumn("updated_at",current_timestamp())\
                      .withColumn("path",F.lower(F.concat(F.lit('/raw/bdo/'), F.col('OWNER'), F.lit('/'), F.col('table_name'))))\
                      .select("source_name",col("OWNER").alias("schema_name"),\
                              col("TABLE_NAME").alias("table_name"),"created_at","updated_at","path")

# COMMAND ----------

df_table_upsert = df_table_lnd.alias("table_ac").join(df_table_target.alias("table_target")\
                                                 ,(col("table_ac.source_name") == col("table_target.source_name"))\
                                                 & (col("table_ac.schema_name") == col("table_target.schema_name"))\
                                                 & (col("table_ac.table_name") == col("table_target.table_name"))\
                                                 ,'leftouter')\
                                            .select("table_ac.*")

df_table_merge = df_table_upsert

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, path_table)

source_set = {
  "source_name": "upsert.source_name",
  "source_type": lit("oracle"),
  "schema_name": "upsert.schema_name",
  "table_name": "upsert.table_name",
  "created_at": "upsert.created_at",
  "updated_at": "upsert.updated_at",
  "path": "upsert.path",
}

delta_table.alias("target").merge(
           df_table_merge.alias("upsert"),
           "target.source_name = upsert.source_name and target.schema_name = upsert.schema_name and target.table_name = upsert.table_name")\
           .whenNotMatchedInsert(values=source_set)\
           .whenMatchedUpdate(set=source_set)\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/{env}/gov/catalog/generate_desc_csv".format(env=env),0,{"gov_table": "table","source_type": "metadata","env": env})
#except:
#  print("Arquivo template CSV não gerado")