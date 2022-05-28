# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

default_dir = "/tmp/dev/raw/gov"
path_lnd_gov = "adl://cnibigdatadls.azuredatalakestore.net{default_dir}/gov/usr_upload/bigdata/table/".format(default_dir=default_dir)

# COMMAND ----------

cols = ["source_name","source_type","schema_name","table_name","description","replica","created_at","updated_at"]
rows = [Row("trs","bigdata","pessoas","membros","test description",1,"",""),
        Row("trs","bigdata","pessoas","clientes","test description",1,"","")]
  
df_crw_schema = spark.createDataFrame(data=rows,schema=cols)

# COMMAND ----------

df_crw_schema.write.format("csv")\
                   .option("sep", ";")\
                   .option("header", "true")\
                   .option("encoding", "ISO-8859-1")\
                   .mode("overwrite")\
                   .save(path_lnd_gov)