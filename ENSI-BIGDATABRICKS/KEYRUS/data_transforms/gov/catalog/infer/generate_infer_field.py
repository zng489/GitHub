# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

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
  is_derivative = 1
  if source_name == 'raw':
    path = "{adl_path}{default_dir}/{source_name}/bdo/".format(adl_path=var_adls_uri,default_dir=default_dir,source_name=source_name)
    schema_name = 'raw/bdo'  
elif source_type == 'external':
  path = "{adl_path}{default_dir}/raw/{source_name}/".format(adl_path=var_adls_uri,default_dir=default_dir,source_name=source_name)
  is_derivative = 0

path_field = "{adl_path}{default_dir}/gov/tables/field".format(adl_path=var_adls_uri,default_dir=default_dir)

# COMMAND ----------

try:
  file_list = dbutils.fs.ls(path)
except:
  dbutils.notebook.exit("Arquivo não encontrado")

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# COMMAND ----------

cols = ["source_name","schema_name","table_name","field_name","data_type","is_derivative"]
rows = []
cod_data_steward = 1

for schema in file_list:
  schema_name = schema.name[:-1]
  if schema_name == 'mtd':
    mtd_path = path+schema_name+"/"
    for subschema in dbutils.fs.ls(mtd_path):
      subschema_name = subschema.name[:-1]
      final_schema = schema_name + '_' + subschema_name
      schema_path = path+schema_name+"/"+subschema_name+"/"
      for table in dbutils.fs.ls(schema_path):
        table_name = table.name[:-1]
        table_path = schema_path + table_name
        source_table = spark.read.format("parquet").load(table_path)
        for name, type in source_table.dtypes:
          rows.append(Row(source_name,final_schema,table_name,name,type,is_derivative))
  else:
    schema_path = path+schema_name+"/"
    for table in dbutils.fs.ls(schema_path):
      table_name = table.name[:-1]
      table_path = schema_path + table_name
      erro = 0
      try:
        source_table = spark.read.format("parquet").load(table_path)
      except Exception as e:
        erro = 1
        output = f"{e}"
        print("erro em: " + table_path)
      if erro == 0:
        for name, type in source_table.dtypes:
          rows.append(Row(source_name,schema_name,table_name,name,type,is_derivative))
  
df_field = spark.createDataFrame(data=rows,schema=cols)

# COMMAND ----------

#remove as tabelas de backups contidas no data lake
df_field = df_field.filter('lower(table_name) not like ("%bkp%")')

# COMMAND ----------

   fields_to_drop = ["dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                     "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                     "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_insercao_raw","dh_exclusao_valor_pessoa",\
                     "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                     "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                     "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp"]
   df_field = df_field.filter(~df_field.field_name.isin(fields_to_drop))

# COMMAND ----------

df_field_merge = (
  df_field.withColumn("created_at",current_timestamp())\
          .withColumn("updated_at",current_timestamp())
)

# COMMAND ----------

delta_field = DeltaTable.forPath(spark, path_field)

field_set = {
  "source_name": "upsert.source_name",
  "source_type": lit(source_type),
  "schema_name": "upsert.schema_name",
  "table_name": "upsert.table_name",
  "field_name": "upsert.field_name",
  "data_type": "upsert.data_type",
  "created_at": "upsert.created_at",
  "is_derivative": "upsert.is_derivative",
  "updated_at": "upsert.updated_at"
  ,"cod_data_steward": "1",
}

delta_field.alias("target").merge(
           df_field_merge.alias("upsert"),
           "target.source_name = upsert.source_name and target.schema_name = upsert.schema_name\
           and target.table_name = upsert.table_name and target.field_name = upsert.field_name")\
           .whenNotMatchedInsert(values=field_set)\
           .execute()

# COMMAND ----------

#try:  
#  dbutils.notebook.run("/KEYRUS/dev/gov/catalog/generate_desc_csv",0,{"gov_table": "field","source_type": source_type,"env": env})
#except:
#  print("Arquivo template CSV não gerado")
