# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

default_dir = "/tmp/dev/raw/gov"
path = "adl://cnibigdatadls.azuredatalakestore.net{default_dir}/trs/trs_test_schema1/trs_test_table1/".format(default_dir=default_dir)

# COMMAND ----------

cols = ["field1","field2","field3"]
rows = [Row("fgwsd",False,3.943),
        Row("gfhfg",True,52.122)]
  
df = spark.createDataFrame(data=rows,schema=cols)

# COMMAND ----------

df.write.format("parquet")\
        .mode("overwrite")\
        .save(path)