# Databricks notebook source
from pyspark.sql import Row
from delta.tables import *

# COMMAND ----------

var_adls_uri = "abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/"

var_folder = "" 
var_folder = "tmp/dev/raw/gov"

path_gov = "{var_adls_uri}/{var_folder}/gov/tables/".format(var_adls_uri=var_adls_uri,var_folder=var_folder)

source = spark.read.format("delta").load(path_gov + "source")
schema = spark.read.format("delta").load(path_gov + "schema")
table = spark.read.format("delta").load(path_gov + "table")
field = spark.read.format("delta").load(path_gov + "field")
data_steward = spark.read.format("delta").load(path_gov + "data_steward")
profiling = spark.read.format("delta").load(path_gov + "profiling")
#profiling_bkp = spark.read.format("delta").load(path_gov + "profiling_bkp")

# COMMAND ----------

display(source)

# COMMAND ----------

display(field.filter('schema_name="BD_BASI"'))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

cond = [source.source_name == schema.source_name, source.source_type == schema.source_type]
source_schema = source.join(schema, cond , 'inner')\
         .drop(schema.source_name).drop(schema.source_type).drop(schema.description).drop(schema.created_at).drop(schema.updated_at).drop(schema.cod_data_steward)\
         .drop(source.created_at).drop(source.updated_at).drop(source.description).drop(source.cod_data_steward)\
         .withColumnRenamed('replica','schema_replica')

# COMMAND ----------

cond = [source_schema.schema_name == table.schema_name]
source_schema_table = source_schema.join(table, cond , 'inner')\
         .drop(table.source_name).drop(table.source_type).drop(table.schema_name).drop(table.description)\
         .drop(table.created_at).drop(table.updated_at).drop(table.dsc_business_subject).drop(table.cod_data_steward)\
         .withColumnRenamed('replica','table_replica')

# COMMAND ----------

cond = [source_schema_table.table_name == field.table_name]
source_schema_table_field = source_schema_table.join(field, cond , 'inner')

# COMMAND ----------

source_schema_table_field.count()

# COMMAND ----------

source_schema_table_field.filter('schema_replica=1 and table_replica=1').count()

# COMMAND ----------

# ---------------------------------------------------------------------------------------------------------

# COMMAND ----------

profiling.count()

# COMMAND ----------

profiling.groupBy('created_at').count().show()

# COMMAND ----------

# agrupa o dataset por created_at

profiling.groupBy('created_at').count().show()

# COMMAND ----------

# limpa o dataset final por meio de filtros

prof = profiling.filter('created_at is not null')

prof = prof.withColumn("created_at",  date_format(col('created_at'),"yyyy MM dd HH:mm:ss.SSSS"))
prof = prof.withColumn("created_at",  concat(substring(col('created_at'),1,10) ,  lit(" 00:00:00.0000")))
prof = prof.withColumn("created_at", to_timestamp("created_at",'yyyy MM dd HH:mm:ss.SSSS'))

prof = prof.filter('created_at != to_timestamp("2021-02-10 00:00:00","yyyy-MM-dd HH:mm:ss")')
prof = prof.filter('created_at != to_timestamp("2021-06-14 00:00:00","yyyy-MM-dd HH:mm:ss")')
prof = prof.filter('created_at != to_timestamp("2021-06-15 00:00:00","yyyy-MM-dd HH:mm:ss")')
prof = prof.filter('created_at != to_timestamp("2021-06-06 00:00:00","yyyy-MM-dd HH:mm:ss")')
prof = prof.filter('created_at != to_timestamp("2021-06-07 00:00:00","yyyy-MM-dd HH:mm:ss")')


# COMMAND ----------

display(schema.filter('replica=1'))

# COMMAND ----------

# escreve o arquivo parquet no disco

#prof.write.format("delta").mode("append").save(path_gov + "profiling")

# COMMAND ----------

# profiling.write.format("delta").mode("append").save(path_gov + "profiling_bkp")
prof.write.format("delta").mode("overwrite").save(path_gov + "profiling")

# COMMAND ----------

display(table.filter("table_name like '%PESSOA%'"))

# COMMAND ----------

display(profiling.filter("lower(schema_name) like '%corporativo%'"))

# COMMAND ----------

display(schema.filter('replica=1'))