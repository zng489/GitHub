# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp()

# COMMAND ----------

path = "{adl_path}/tmp/dev/raw/gov/gov/tables/metrics".format(adl_path=var_adls_uri)
"{adl_path}/gov/tables/profiling".format(adl_path=var_adls_uri)

# COMMAND ----------

cols = ["metric_name","type","description"]
rows = [Row("fill_rate","general","Taxa de preenchimento de uma coluna, em porcentagem (de 0 a 1)"),
        Row("null_rate","general","Taxa de nulos de uma coluna, em porcentagem (de 0 a 1), inverso a taxa de preenchimento"),
        Row("min_value","general","Valor mínimo de uma coluna numérica"),
        Row("max_value","general","Valor máximo de uma coluna numérica"),
        Row("max_str_len","general","Valor máximo de uma coluna string"),
        Row("distinct_count","general","Quantidade de valores distintos para uma determinada coluna"),
        Row("more_than_once","general","Quantidade de valores repetiros que existem na tabela"),
        Row("only_once","general","Quantidade de valores que aparecem apenas uma vez")]
  
df = spark.createDataFrame(data=rows,schema=cols)

# COMMAND ----------

df = df.withColumn("created_at", current_timestamp())\
       .withColumn("updated_at", current_timestamp())

# COMMAND ----------

df.write.format("delta").mode("append").save()