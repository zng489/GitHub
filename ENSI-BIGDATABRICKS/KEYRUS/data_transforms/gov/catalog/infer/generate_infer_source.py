# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("env","dev")
dbutils.widgets.text("source_name","crw")
dbutils.widgets.text("source_type","external")

# COMMAND ----------

env = dbutils.widgets.get("env")
source_name = dbutils.widgets.get("source_name")
source_type = dbutils.widgets.get("source_type")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""

# COMMAND ----------

if source_type == 'bigdata':
  path = "{adl_path}{default_dir}/{source_name}/".format(adl_path=var_adls_uri,default_dir=default_dir,source_name=source_name)
elif source_type == 'external':
  path = "{adl_path}{default_dir}/raw/{source_name}/".format(adl_path=var_adls_uri,default_dir=default_dir,source_name=source_name)
  
path_source = "{adl_path}{default_dir}/gov/tables/source".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

cols = ["source_name"]
crw = [Row(source_name)]
df_crw_source = spark.createDataFrame(data=crw,schema=cols)

# COMMAND ----------

df_source_merge = (
  df_crw_source.withColumn("created_at",current_timestamp())
               .withColumn("updated_at",current_timestamp())
)

# COMMAND ----------

delta_source = DeltaTable.forPath(spark, path_source)

source_set = {
  "source_name": "upsert.source_name",
  "source_type": lit(source_type),
  "created_at": "upsert.created_at",
  "updated_at": "upsert.updated_at"
}

delta_source.alias("target").merge(
           df_source_merge.alias("upsert"),
           "target.source_name = upsert.source_name")\
           .whenNotMatchedInsert(values=source_set)\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/dev/gov/catalog/generate_desc_csv",0,{"gov_table": "source","source_type": "external","env": env})
#except:
#  print("Arquivo template CSV não gerado")