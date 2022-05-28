# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Maintenance: 
# MAGIC   Thomaz Moreira: 2020-07-13: 
# MAGIC     added "drop('kv_process_control')". 
# MAGIC     new implementation for max and min metrics that uses agg() instead of many collects()
# MAGIC     removed string fields from max and min metrics
# MAGIC </pre>  

# COMMAND ----------

from pyspark.sql.functions import sum, col, when, lit, trim, regexp_extract, udf, regexp_replace, length, max, min, unix_timestamp
from pyspark.sql.types import BooleanType, FloatType
import json

# COMMAND ----------

var_item_name = "metric"

# COMMAND ----------

source = dbutils.widgets.get('adl_table_path')

# COMMAND ----------

control_columns = ["nm_arq_in", "nr_reg", "dh_arq_in", "dh_insercao_raw", "dh_insercao_trs", "dh_insercao_biz"]

# COMMAND ----------

results = []

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

df = spark.read.parquet("{}{}".format(var_adls_uri, source))

"""
kv_process_control is not useful in profiling
"""
if 'kv_process_control' in df.columns:
  df = df.drop('kv_process_control')

# COMMAND ----------

df_count = df.count()

# COMMAND ----------

def percentage(value):
  return (value*100)/df_count

# COMMAND ----------

def get_dtype(df,colname):
    dtype = [dtype for name, dtype in df.dtypes if name == colname][0]
    if dtype.startswith('decimal'):
        dtype = 'decimal'
    return dtype

# COMMAND ----------

def is_digit(value):
    if value:
        return value.isdigit()
    else:
        return False

# COMMAND ----------

percentage_udf = udf(percentage, FloatType())

# COMMAND ----------

is_digit_udf = udf(is_digit, BooleanType())

# COMMAND ----------

var_numeric_columns = [c for c in df.columns if get_dtype(df,c) in ["decimal", "double", "float", "int", "bigint", "smallint"]]

# COMMAND ----------

#Count
duplicates_count = {var_item_name: "count"}
duplicates_count.update({column: df_count for column in df.columns if column})
results = results + [json.dumps(duplicates_count)]

# COMMAND ----------

#Mean, stdev - applies to numeric columns
results = results + df.select(*var_numeric_columns).summary(["mean", "stddev"]).withColumnRenamed("summary", var_item_name).toJSON().collect()

# COMMAND ----------

def max_min_time_columns(df, in_metric:str="max"):
  """
  params:
    df: PySpark DataFrame
    in_metric: "min" or "max", don't cheat it!
  """
  var_time_columns = [c for c in df.columns if get_dtype(df,c) in ("date", "timestamp")]
  var_pyspark_q = "df.select(*var_time_columns).agg("
  for c in var_time_columns:
    var_pyspark_q = var_pyspark_q + ("{0}('{1}').alias('{1}'),".format(in_metric, c))
  var_pyspark_q = var_pyspark_q[:-1] + ").toJSON().first()"
  results_date_dict = json.loads(eval(var_pyspark_q))
  
  #This allows to get columns when the statistics is null
  for key in var_time_columns:
    if key not in results_date_dict:
      results_date_dict[key] = None
      return results_date_dict

# COMMAND ----------

"""
Min

Removed string from this list cause it is absolut nonsense
"""

min_results_dict = json.loads(df.select(*var_numeric_columns).summary("min").withColumnRenamed("summary", var_item_name).toJSON().collect()[0])
min_results_date_dict = max_min_time_columns(df, in_metric="min")
min_results_dict.update(min_results_date_dict)
results = results +[json.dumps(min_results_dict, ensure_ascii=False)]

# COMMAND ----------

"""
Max - Old implementation. Very inneffcient as it statrs one jobs for every collect() action

Removed string from this list cause it is absolut nonsense

max_results = df.select([c for c in df.columns if get_dtype(df,c) in ["decimal", "double", "float", "int", "bigint", "smallint"]]).summary("max").withColumnRenamed("summary", var_item_name).toJSON().collect()
max_results_dict = json.loads(max_results[0])
max_results_date_dict ={c: df.select(max(c).cast("string")).collect()[0][0] for c in df.columns if get_dtype(df,c) in ["date", "timestamp"]}
max_results_dict.update(max_results_date_dict)
results = results +[json.dumps(max_results_dict, ensure_ascii=False)]
"""

max_results_dict = json.loads(df.select(*var_numeric_columns).summary("max").withColumnRenamed("summary", var_item_name).toJSON().collect()[0])
max_results_date_dict = max_min_time_columns(df, in_metric="max")
max_results_dict.update(max_results_date_dict)
results = results +[json.dumps(max_results_dict, ensure_ascii=False)]

# COMMAND ----------

#25%, 50%, 75%
results = results + df.select(*var_numeric_columns).summary(["25%", "50%", "75%"]).withColumnRenamed("summary", var_item_name).toJSON().collect()

# COMMAND ----------

#Null percentage - This applies to all columns
results = results + df.select([percentage_udf(sum(when(col(c).isNull(), 1).otherwise(0))).alias(c) for c in df.columns]).withColumn(var_item_name, lit("null_percentage")).toJSON().collect()

# COMMAND ----------

#Empty percentage - This applies to all columns
results = results + df.select([percentage_udf(sum(when((trim(col(c)) == '') & (regexp_extract(col(c),'[ \t]*', 0) == ''), 1).otherwise(0))).alias(c) for c in df.columns]).withColumn(var_item_name, lit("empty_percentage")).toJSON().collect()

# COMMAND ----------

#Blank percentage - This applies to all columns
results = results + df.select([percentage_udf(sum(when((col(c) != '') & (col(c) == regexp_extract(col(c),'[ \t]*', 0)), 1).otherwise(0))).alias(c) for c in df.columns]).withColumn(var_item_name, lit("blank_percentage")).toJSON().collect()

# COMMAND ----------

#Zero percentage - applies only to numeric columns
results = results + df.select([percentage_udf(sum(when(col(c).cast("int") == 0, 1).otherwise(0))).alias(c) for c in var_numeric_columns]).withColumn(var_item_name, lit("zero_percentage")).toJSON().collect()

# COMMAND ----------

#Not numeric percentage
results = results + df.select([percentage_udf(sum(when(is_digit_udf(regexp_replace(col(c).cast("string"), '[.]', '')), 0).otherwise(1))).alias(c) for c in df.columns]).withColumn(var_item_name, lit("not_numeric_percentage")).toJSON().collect()

# COMMAND ----------

#Not date percentage - only time columns
results = results + df.select([percentage_udf(sum(when(col(c).cast("timestamp").isNull(), 1).otherwise(0))).alias(c) if get_dtype(df,c)=="timestamp" 
                               else percentage_udf(sum(when(col(c).cast("date").isNull(), 1).otherwise(0))).alias(c) if get_dtype(df,c)=="date" 
                               else lit(100).alias(c) for c in df.columns]). \
withColumn(var_item_name, lit("not_date_percentage")).toJSON().collect()

# COMMAND ----------

#Duplicates count
group_by_cols = [column for column in df.columns if column not in control_columns]
df_duplicates_count = df.groupBy(group_by_cols).count().filter(col('count') > lit(1)).count()

duplicates_count = {var_item_name: "duplicates_count"}
duplicates_count.update({column: df_duplicates_count for column in df.columns})

results = results + [json.dumps(duplicates_count)]

# COMMAND ----------

dbutils.notebook.exit(results)