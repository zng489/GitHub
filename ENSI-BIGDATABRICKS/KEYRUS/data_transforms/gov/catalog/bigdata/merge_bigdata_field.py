# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_lnd_gov = "{adl_path}{default_dir}/gov/usr_upload/bigdata/field/".format(adl_path=var_adls_uri,default_dir=default_dir)
path_source = "{adl_path}{default_dir}/gov/tables/field".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

try:
  source_desc_df = spark.read.format("csv")\
                           .option("sep", ";")\
                           .option("header", "true")\
                           .option("encoding", "ISO-8859-1")\
                           .load(path_lnd_gov)
except(AnalysisException) as e:
  print("Arquivo não encontrado")
  dbutils.notebook.exit("File not found")

source_df = spark.read.format("delta").load(path_source)
source_dt = DeltaTable.forPath(spark, path_source)

# COMMAND ----------

source_df = source_df.filter("source_type = 'bigdata'")

df_delete = source_df.alias("governance").join(source_desc_df.alias("delete")\
                                               , (lower(col("governance.source_name")) == lower(col("delete.source_name")))\
                                               & (lower(col("governance.schema_name")) == lower(col("delete.schema_name")))\
                                               & (lower(col("governance.table_name")) == lower(col("delete.table_name")))\
                                               & (lower(col("governance.field_name")) == lower(col("delete.field_name")))\
                                               , 'leftanti')\
                                         .select(col("governance.*"))\
                                         .withColumn("deleted", lit(1))

source_desc_df = source_desc_df.withColumn("deleted", lit(0))

df_upsert = source_desc_df.unionAll(df_delete)\
                          .withColumn("created_at",current_timestamp())\
                          .withColumn("updated_at",current_timestamp())

df_upsert.show()

# COMMAND ----------

update_set = {
  "data_steward": "upsert.data_steward",
  "login": "upsert.login",
  "description": "upsert.description",
  "personal_data": "upsert.personal_data",
  "is_derivative": lit(1),
  "updated_at": "upsert.updated_at"
}

#Adding, updating or delete to source table
source_dt.alias("governance").merge(
             df_upsert.alias("upsert"),
             "lower(governance.source_name) = lower(upsert.source_name) and lower(governance.schema_name) = lower(upsert.schema_name) and \
             lower(governance.table_name) = lower(upsert.table_name) and lower(governance.field_name) = lower(upsert.field_name)")\
             .whenMatchedDelete(condition = "upsert.deleted = 1") \
             .whenMatchedUpdate(condition = "upsert.deleted = 0"
                                ,set=update_set)\
             .whenNotMatchedInsertAll()\
             .execute()

# COMMAND ----------

dbutils.fs.rm(path_lnd_gov,True)