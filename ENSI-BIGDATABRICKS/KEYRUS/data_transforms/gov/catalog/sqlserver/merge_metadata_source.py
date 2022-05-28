# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

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
path_lnd = "{adl_path}{default_dir}/lnd/gov/sqlserver/{dir}/".format(adl_path=var_adls_uri,default_dir=default_dir,dir=dir)
path_source = "{adl_path}{default_dir}/gov/tables/source".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

df_lnd = spark.read.format("parquet").option("inferSchema","true").load(path_lnd)
df_source_target = spark.read.format("delta").load(path_source)

# COMMAND ----------

df_source_lnd = df_lnd.withColumn("source_name",regexp_extract(input_file_name(),"(?<=\d\d\d\d\d\d\d\d/)(.*?)(?=_metadata)",1))\
                          .dropDuplicates(["source_name"])\
                          .withColumn("created_at",current_timestamp())\
                          .withColumn("updated_at",current_timestamp())\
                          .select("source_name","created_at","updated_at")

# COMMAND ----------

df_source_upsert = df_source_lnd.alias("source_ac").join(df_source_target.alias("source_target")\
                                                   ,(col("source_ac.source_name") == col("source_target.source_name")) \
                                                   ,'leftouter')\
                                            .select("source_ac.*")\
                                            .withColumn("deleted",lit(0))

df_source_merge = df_source_upsert

# COMMAND ----------

delta_source = DeltaTable.forPath(spark, path_source)

source_set = {
  "source_name": "upsert.source_name",
  "source_type": lit("sqlserver"),
  "created_at": "upsert.created_at",
  "updated_at": "upsert.updated_at"
}

delta_source.alias("target").merge(
           df_source_merge.alias("upsert"),
           "target.source_name = upsert.source_name")\
           .whenNotMatchedInsert(condition = "upsert.deleted = 0",values=source_set)\
           .whenMatchedUpdate(condition = "upsert.deleted = 0",set=source_set)\
           .whenMatchedDelete(condition = "upsert.deleted = 1")\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/{env}/gov/catalog/generate_desc_csv".format(env=env),0,{"gov_table": "source","source_type": "oracle","env": env})
#except:
#  print("Arquivo template CSV não gerado")