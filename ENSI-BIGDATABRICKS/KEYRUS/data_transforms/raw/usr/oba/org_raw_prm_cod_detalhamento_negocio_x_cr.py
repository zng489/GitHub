# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # IMPORTANT NOTES FOR ADF IMPLEMENTATION
# MAGIC 
# MAGIC This file after, processing, must go to /ach folder, into which we recplicate the complete structure of the landing folder.
# MAGIC </br>In the lowest level, we'll have a folder with the integer that represents dh_insercao_raw, for keeping the version for this file.
# MAGIC 
# MAGIC <b>Example:</b>
# MAGIC </br>source: /uld/oba/prm_cod_detalhamento_negocio_x_cr 
# MAGIC </br>sink: /ach/uld/oba/prm_cod_detalhamento_negocio_x_cr/20200212134522
# MAGIC </br>*20200212134522* refers to the integer representation of dh_insercao_raw
# MAGIC 
# MAGIC For now this notebook serves the purpose of mocking and testing ADF pipeline execution. 

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS data access section
# MAGIC 
# MAGIC Testing with our PyEgg cni_connectors, this will avoid all the code recplication for connection when deploying to production.

# COMMAND ----------

import json
import re
import datetime
from raw_loader import raw_loader as rl
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index

from cni_connectors import adls_gen1_connector as adls_conn

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen22", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md 
# MAGIC Common import section. Declare your imports all here

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

# MAGIC %md
# MAGIC This cell is for implementing widgets.get and json convertion
# MAGIC Provided that it is still not implemented, i'll mock it up by setting the necessary stuff for the table I'm working with.
# MAGIC 
# MAGIC Remember that when parsing any json, we must handle any possibility of strange char, escapes ans whatever comes dirt from Data Factory!

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))

# COMMAND ----------

# USE THIS FOR DEVELOPMENT PURPOSES ONLY
"""
var_file = {'namespace': 'oba', 
            'file_folder': 'prm_cod_detalhamento_negocio_x_cr', 
            'extension': 'txt', 
            'column_delimiter': ';',
            'encoding': 'UTF-8',
            'null_value': '',
            'load_type': 'full'
           } 

var_dls = {"folders":{"landing":"/uld",
                      "error":"/err",
                      "staging":"/stg",
                      "log":"/log",
                      "raw":"/raw",
                      "archive": "/ach",
                     },
           "systems": {"raw": "usr"}
          }
"""

# COMMAND ----------

var_adls_files_path = "{}/{}/{}/".format(var_dls["folders"]["landing"], 
                                         var_file["namespace"], 
                                         var_file["file_folder"]
                                        ).strip().lower()

# COMMAND ----------

var_source = "{}{}".format(var_adls_uri,var_adls_files_path ).strip().lower()
var_sink = "{}{}/{}/{}/{}".format(var_adls_uri, var_dls["folders"]["raw"], var_dls["systems"]["raw"], var_file["namespace"], var_file["file_folder"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Implementation for file list and insertion timestamp in ADLS Gen1, 

# COMMAND ----------

# DBTITLE 1,PyEgg implementation
from raw_loader import uld_file_loader

# COMMAND ----------

var_files_mod_time =  uld_file_loader.list_files_in_adls_path(dbutils, scope="adls_gen2", files_path=var_adls_files_path)

# COMMAND ----------

adl_file_time = spark.createDataFrame(var_files_mod_time, verifySchema=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC After this step, only thing needed is to join the dataframes, but it has to be done after creating the column "nm_arq_in"

# COMMAND ----------

# mode="FAILFAST" is crucial for not letting corrupeted records pass. This execution will fail when any corrupt record is found.
# inferSchema=True will allow for dynamic typing of variables

df = spark.read.csv(path=var_source, 
                          sep=var_file["column_delimiter"],
                          encoding=var_file["encoding"],
                          header=True,
                          nullValue=var_file["null_value"],
                          mode="FAILFAST",
                          ignoreLeadingWhiteSpace=True,
                          ignoreTrailingWhiteSpace=True,
                          inferSchema=True,
                         )

# COMMAND ----------

# Files are tricky considering columns. Let's lower them all.
for c in df.columns:
  df = df.withColumnRenamed(c, c.lower())

# COMMAND ----------

# TESTING IF IT WORKS AS EXPECTED
# DUE to the absolute dependency on pyspark.sql.functions, this step won't be encapsulated on an PyEgg
df = df. \
withColumn("nm_arq_in", substring_index(input_file_name(), "/", -1)). \
withColumn("nr_reg", monotonically_increasing_id()). \
withColumn("dh_insercao_raw", from_utc_timestamp(current_timestamp(), 'Brazil/East'))

# COMMAND ----------

df = df.join(adl_file_time, ["nm_arq_in"], "left")

# COMMAND ----------

del adl_file_time

# COMMAND ----------

#display(df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Time to write it to /raw in ADLS. 
# MAGIC 
# MAGIC Archiving is a task for ADF, you won't implement it here.

# COMMAND ----------

# Inserted values returns type datetime when using collect()
var_dh_insercao_raw = df. \
select("dh_insercao_raw"). \
dropDuplicates(). \
collect()[0][0]. \
strftime("%Y%m%d%H%M%S")

# COMMAND ----------

# RAW
df.coalesce(1).write.save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC File insertion time in ADLS to exit from this notebook back to ADF.

# COMMAND ----------

dbutils.notebook.exit('{"var_dh_insercao_raw": "%s"}' % str(var_dh_insercao_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load modes implementation section. 
# MAGIC Keep it simple, clean and smooth.</br>

# COMMAND ----------

# MAGIC %md
# MAGIC This one will not work, we need a new egg for files

# COMMAND ----------

"""
if file["load_type"] == "full":
  rl.load_full(source_df, var_file, var_dls, var_options)
else:
  print("These are the cases for other types of raw loading.Not implemented yet")
  pass
"""