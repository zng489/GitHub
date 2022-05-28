# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.functions import regexp_replace,col
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

dbutils.widgets.text("gov_table","source")
gov_table = dbutils.widgets.get("gov_table")

dbutils.widgets.text("gov_steward","1")
gov_steward = dbutils.widgets.get("gov_steward")

dbutils.widgets.text("gov_steward_name","1")
gov_steward_name = dbutils.widgets.get("gov_steward_name")

# COMMAND ----------

# captura os data stewards
default_dir = ""
path_gov = "{var_adls_uri}/{default_dir}/gov/tables/".format(var_adls_uri=var_adls_uri,default_dir=default_dir)
data_steward = spark.read.format("delta").load(path_gov + "data_steward")
data_steward = data_steward.withColumn('name_data_steward', regexp_replace(col('name_data_steward'), " ", "_"))
data_steward.show()

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_gov = "{adl_path}{default_dir}/gov/tables/{gov_table}".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)
path_csv_gov = "{adl_path}{default_dir}/gov/csv_desc/{gov_table}/".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)
path_gov_field = "{adl_path}{default_dir}/gov/tables/field/".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)

# COMMAND ----------

filtro = '1 = 1'
filtro_table = filtro
filtro_field = filtro

# COMMAND ----------

filtro = '1 = 1'
filtro_field = filtro
filtro_table = '(replica = 1) or (replica is null)'  

if gov_table == 'field':
   filtro = filtro_field
    
if gov_table == 'table':
   filtro = filtro_table    

# COMMAND ----------

df_csv = spark.read.format("delta")\
                   .load(path_gov)\
                   .filter(filtro)

# COMMAND ----------

if gov_table == 'field':
   
   path_table = path_gov.replace('field','table')

   df_table = spark.read.format("delta")\
                        .load(path_table)\
                        .filter(filtro_table)\
                        .select(['source_name','schema_name','table_name','path'])
   
   cond = [df_csv.source_name == df_table.source_name, df_csv.schema_name == df_table.schema_name, df_csv.table_name == df_table.table_name]
   columns_to_drop = ['df_table.source_name','df_table.schema_name','df_table.table_name']
   df_field_table = df_csv.join(df_table, cond, 'inner').drop(df_table.source_name).drop(df_table.schema_name).drop(df_table.table_name)
    
   df_csv = df_field_table.filter('cod_data_steward = ' + gov_steward)        
   
   campos = ['source_name','source_type','schema_name','table_name','field_name','path', 'description','dsc_business_subject','personal_data','ind_relevance']
   df_csv = df_csv.select(campos)    

# COMMAND ----------

if gov_table == 'table':
   df_field = spark.read.format("delta")\
                        .load(path_gov_field)\
                        .filter('cod_data_steward = ' + gov_steward)\
                        .select(['source_name','schema_name','table_name'])\
                        .distinct()
    
   cond = [df_csv.source_name == df_field.source_name, df_csv.schema_name == df_field.schema_name, df_csv.table_name == df_field.table_name] 
   df_csv = df_csv.join(df_field, cond, 'inner').drop(df_field.source_name).drop(df_field.schema_name).drop(df_field.table_name)
    
   campos = ['source_name','source_type','schema_name','table_name','path','description','dsc_business_subject']
   df_csv = df_csv.select(campos)

# COMMAND ----------

path_file = path_csv_gov
path_gov_csv_out = "{adl_path}{default_dir}/gov/csv_desc/out/{gov_table}_ds_{gov_steward}_{gov_steward_name}.csv"\
                   .format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table,gov_steward=gov_steward,gov_steward_name=gov_steward_name)

# COMMAND ----------

df_csv = df_csv.orderBy("path")

# COMMAND ----------

df_csv.select([column for column in df_csv.columns])\
      .repartition(1)\
      .write\
      .format("csv")\
      .option("delimiter",";")\
      .option("header","true")\
      .option("encoding", "ISO-8859-1")\
      .mode("overwrite")\
      .save(path_file)


# COMMAND ----------

file_list = dbutils.fs.ls(path_csv_gov)
for fileinfo in file_list:
  if ".csv" in fileinfo.name:
    file_path = fileinfo.path.split("/",1)[1].split("/",1)[1].split("/",1)[1]
    file_path = "{adl_path}{default_dir}/{file_path}".format(adl_path=var_adls_uri,default_dir=default_dir,file_path=file_path)    

# COMMAND ----------

file_path_dir = "/".join(file_path.split('/')[:-1]) + '/'
file_path_dir

# COMMAND ----------

path_gov_csv_out

# COMMAND ----------

dbutils.fs.mv(file_path, path_gov_csv_out)

# COMMAND ----------

dbutils.fs.rm(file_path_dir, recurse = True)

# COMMAND ----------

"""
for fileinfo in file_list:
  if "csv" in fileinfo.name:
    file_path = fileinfo.path.split("/",1)[1].split("/",1)[1].split("/",1)[1]
    print(file_path)
    dbutils.notebook.exit(file_path)
"""

# COMMAND ----------


