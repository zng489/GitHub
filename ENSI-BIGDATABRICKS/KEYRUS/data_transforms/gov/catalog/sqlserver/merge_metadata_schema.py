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
path_schema = "{adl_path}{default_dir}/gov/tables/schema".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

df_lnd = spark.read.format("parquet").option("inferSchema","true").load(path_lnd)
df_source_target = spark.read.format("delta").load(path_source)
df_schema_target = spark.read.format("delta").load(path_schema)

# COMMAND ----------

df_schema_lnd = df_lnd.withColumn("source_name",regexp_extract(input_file_name(),"(?<=\d\d\d\d\d\d\d\d/)(.*?)(?=_metadata)",1))\
                      .dropDuplicates(["owner"])\
                      .withColumn("created_at",current_timestamp())\
                      .withColumn("updated_at",current_timestamp())\
                      .select("source_name",col("OWNER").alias("schema_name"),"created_at","updated_at")                                              

# COMMAND ----------

df_schema_upsert = df_schema_lnd.alias("schema_ac").join(df_schema_target.alias("schema_target")\
                                                 ,(col("schema_ac.source_name") == col("schema_target.source_name")) \
                                                 & (col("schema_ac.schema_name") == col("schema_target.schema_name")) \
                                                 ,'leftouter')\
                                            .select("schema_ac.*")

df_schema_merge = df_schema_upsert
display(df_schema_merge)

# COMMAND ----------

delta_schema = DeltaTable.forPath(spark, path_schema)

source_set = {
  "source_name": "upsert.source_name",
  "source_type": lit("sqlserver"),
  "schema_name": "upsert.schema_name",
  "created_at": "upsert.created_at",
  "updated_at": "upsert.updated_at"
}

delta_schema.alias("target").merge(
           df_schema_merge.alias("upsert"),
           "target.source_name = upsert.source_name and target.schema_name = upsert.schema_name")\
           .whenNotMatchedInsert(values=source_set)\
           .whenMatchedUpdate(set=source_set)\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/{env}/gov/catalog/generate_desc_csv".format(env=env),0,{"gov_table": "schema","source_type": "metadata","env": env})
#except:
#  print("Arquivo template CSV não gerado")