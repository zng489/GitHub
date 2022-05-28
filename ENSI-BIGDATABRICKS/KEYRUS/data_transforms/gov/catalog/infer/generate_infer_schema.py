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
path_schema = "{adl_path}{default_dir}/gov/tables/schema".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

try:
  file_list = dbutils.fs.ls(path)
except:
  dbutils.notebook.exit("Arquivo não encontrado")

# COMMAND ----------

cols = ["source_name","schema_name"]
rows = []

for schema in file_list:
  schema_name = schema.name[:-1]
  if source_name == 'raw':
    raw_path = path+'/'+schema_name+"/"
    for subschema in dbutils.fs.ls(raw_path):
      subschema_name = subschema.name[:-1]
      if schema_name == 'bdo':
        final_schema = subschema_name
        rows.append(Row(source_name,final_schema))
  else:
    #rows.append(Row(source_name,schema_name))  
    if schema_name == 'mtd':
      mtd_path = path+schema_name+"/"
      for subschema in dbutils.fs.ls(mtd_path):
        subschema_name = subschema.name[:-1]
        final_schema = schema_name + '_' + subschema_name
        rows.append(Row(source_name,final_schema))
    else:
      rows.append(Row(source_name,schema_name))

df_schema = spark.createDataFrame(data=rows,schema=cols)

# COMMAND ----------

df_schema_merge = (
  df_schema.withColumn("created_at",current_timestamp())\
           .withColumn("updated_at",current_timestamp())
)

# COMMAND ----------

delta_source = DeltaTable.forPath(spark, path_schema)

schema_set = {
  "source_name": "upsert.source_name",
  "source_type": lit(source_type),
  "replica": lit("1"),
  "schema_name": "upsert.schema_name",
  "description": "upsert.schema_name",
  "created_at": "upsert.created_at",
  "updated_at": "upsert.updated_at"
}

delta_source.alias("target").merge(
           df_schema_merge.alias("upsert"),
           "target.source_name = upsert.source_name and target.schema_name = upsert.schema_name")\
           .whenNotMatchedInsert(values=schema_set)\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/dev/gov/catalog/generate_desc_csv",0,{"gov_table": "schema","source_type": "external","env": env})
#except:
#  print("Arquivo template CSV não gerado")
