# Databricks notebook source
import pyspark.sql.functions as functions
import pyspark.sql.types as types
import json
from cni_connectors import adls_gen1_connector as adls_conn

# COMMAND ----------

"""
This will enable dynamic overwrite. We won't lose partition definition.
"""
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

var_table_path = "<your table path here starting with />"
print(var_table_path)

# COMMAND ----------

var_tmp_sink = var_adls_uri + "/tmp/stg" + var_table_path
var_source = var_adls_uri + var_table_path
var_sink = var_source
df = spark.read.parquet(var_source)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Schema adaptation \[START]

# COMMAND ----------

if "kv_process_control" in df.columns:
  print("THIS TABLE HAS ALREADY BEEN PROCESSED")
else:
  print("KV_PROCESS_CONTROL WILL BE ADDED")
  var_kv_process_control_schema = types.StructType([
    types.StructField("adf_factory_name", types.StringType()),
    types.StructField("adf_pipeline_name", types.StringType()),
    types.StructField("adf_pipeline_run_id", types.StringType()),
    types.StructField("adf_trigger_id", types.StringType()),
    types.StructField("adf_trigger_name", types.StringType()),
    types.StructField("adf_trigger_time", types.TimestampType()),
    types.StructField("adf_trigger_type", types.StringType())
  ])

  var_kv_process_control_value = {
    "adf_factory_name": None,
    "adf_pipeline_name": None,
    "adf_pipeline_run_id": None,
    "adf_trigger_id": None,
    "adf_trigger_name": None,
    "adf_trigger_time": None,
    "adf_trigger_type": None
  }
  
  var_count_source = df.count()
  print("count before overwrite: {}".format(var_count_source))
  
  """
  Genrally, load_control_columns are the last ones in the DF. Columns after it must be partition columns
  """
  var_load_control_columns = set(["nm_arq_in", "nr_reg", "dh_arq_in", "dh_insercao_raw"])
  var_columns = df.columns
  var_partition_columns = None

  # If it is not partitioned, then "dh_insercao_raw" is the last column
  var_max_control_column_index = max([var_columns.index(c) + 1 for c in var_load_control_columns])
  if len(var_columns) == var_max_control_column_index:
    print("partitioned: NO")
  else:
    var_partition_columns = var_columns[var_max_control_column_index: ]
    print("partitioned: YES")
    print("partition_columns:{}".format(var_partition_columns))
    
  df = df.withColumn("kv_process_control", \
                     functions.from_json(functions.lit(json.dumps(var_kv_process_control_value)),schema=var_kv_process_control_schema))
  
  if var_partition_columns is None:
    print("overwrite: NOT partitioned")
    df.write.save(var_sink, format="parquet", mode="overwrite")
    df_sink = spark.read.parquet(var_sink)
    var_count_sink = df_sink.count()
    print("counts: source= {}, sink= {}".format(var_count_source, var_count_sink))
    assert var_count_source == var_count_sink, "DATA LOSS WHEN WRITING TO SINK. YOU STILL HAVE A CONSISTENT TMP"
  else:
    print("overwrite: WITH partition, writing to tmp_sink")
    df.write.partitionBy(var_partition_columns).save(var_tmp_sink, format="parquet", mode="overwrite")
    df_tmp = spark.read.parquet(var_tmp_sink)
    var_count_tmp = df_tmp.count()
    print("counts: source= {}, tmp_sink= {}".format(var_count_source, var_count_tmp))
    assert var_count_source == var_count_tmp, "DATA LOSS WHEN WRITING TO TMP. ABORTING!"
    print("deleting source folder")
    dbutils.fs.rm(var_source, True)
    df_tmp.write.partitionBy(var_partition_columns).save(var_sink, format="parquet", mode="overwrite")
    df_sink = spark.read.parquet(var_sink)
    var_count_sink = df_sink.count()
    print("counts: source= {}, sink= {}".format(var_count_source, var_count_sink))
    assert var_count_source == var_count_sink, "DATA LOSS WHEN WRITING TO SINK. YOU STILL HAVE A CONSISTENT TMP"
    print("deleting tmp folder")
    dbutils.fs.rm(var_tmp_sink, True)
print("DONE!!!")

# COMMAND ----------

display(df_sink.limit(100))