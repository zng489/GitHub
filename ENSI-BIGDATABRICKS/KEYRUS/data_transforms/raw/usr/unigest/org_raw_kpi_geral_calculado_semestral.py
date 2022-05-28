# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

import uld_unigest.functions as uf
import crawler.functions as cf
from raw_control_field import raw_control_field as rcf
import json
import pyspark.sql.types as types
import pyspark.sql.functions as f
from pyspark.sql.utils import AnalysisException
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_file_parse = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file_parse")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_file = {'namespace':'unigest','file_folder':'kpi_geral_calculado_semestral','extension':'CSV','column_delimiter':';','encoding':'UTF-8','null_value':''}

var_adf = {
  "adf_factory_name": "cnibigdatafactory",
  "adf_pipeline_name": "org_raw_kpi_meta",
  "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
  "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
  "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
  "adf_trigger_time": "2020-11-19T01:42:41.5507749Z",
  "adf_trigger_type": "PipelineActivity"
}

var_dls = {"folders":{"landing": "/tmp/dev/uld",
                      "error": "/tmp/dev/err", 
                      "staging": "/tmp/dev/stg", 
                      "log": "/tmp/dev/log", 
                      "raw": "/tmp/dev/raw", 
                      "archive": "/tmp/dev/ach", 
                      "reject": "/tmp/dev/rjt"},
           "sub_folders":{"usr":"usr", 
                          "udl":"udl"}}

var_file_parse = {'file_path':'/prm/usr/unigest/CNI_BigData_KPIs_origens_planilhas_layouts_v02.1.xlsx', 
                  'headers':{'name_header':'campo','pos_header':'A','pos_org':'A','pos_dst':'A','pos_type':'B', 'pos_file_name_mask':'A', 'index_file_name_mask':0}
                 }
"""

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']
usr = var_dls['sub_folders']['usr']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_source

# COMMAND ----------

var_sink = "{adl_path}{raw}/{usr}/{namespace}/{file_folder}/".format(adl_path=var_adls_uri, raw=raw, usr=usr,
                                                                     namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_sink

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking if exist source

# COMMAND ----------

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit({'list_files_to_reject': [], 'msg': 'Path "%s" not exist or is empty' % var_source})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading source

# COMMAND ----------

if var_file["encoding"].lower() == 'utf-8-sig':
  var_file["encoding"] = 'utf-8'

# COMMAND ----------

df = spark.read.csv(path=var_adls_uri + var_source, sep=var_file["column_delimiter"], encoding=var_file["encoding"], mode='FAILFAST', 
                    header=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)\
.dropna(how='all')

# COMMAND ----------

for column in df.columns:
  df = df.withColumnRenamed(column, uf.normalize_str(column))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding control columns

# COMMAND ----------

"""
JSON schema definition for "adf". 
If you alter these fields in ADF, alter this schema too. There's no magic, boy.
"""

var_kv_process_control_schema = types.StructType([
  types.StructField("adf_factory_name", types.StringType()),
  types.StructField("adf_pipeline_name", types.StringType()),
  types.StructField("adf_pipeline_run_id", types.StringType()),
  types.StructField("adf_trigger_id", types.StringType()),
  types.StructField("adf_trigger_name", types.StringType()),
  types.StructField("adf_trigger_time", types.TimestampType()),
  types.StructField("adf_trigger_type", types.StringType())
])

"""
Removing milisecond precision for compatibility.
Time is in UTC timezone, same as Data Factory's. This way we can always trace back with precision!
"""
var_adf["adf_trigger_time"] = var_adf["adf_trigger_time"].split(".")[0]

# COMMAND ----------

"""
dh_isercao_raw will be replaced by timestamp cast var_adf["pipeline_trigger_time"]. 
Please note that this is the OUTER PIPELINE, the one that takes the process' title.

NO NEED to have it anymore: functions.from_utc_timestamp(functions.current_timestamp(), 'Brazil/East')
"""
var_options = {"adls_uri": var_adls_uri, 
           "control_fields": {"nm_arq_in": f.substring_index(f.input_file_name(), '/', -1), 
                              "nr_reg": f.monotonically_increasing_id(),                                                            
                              "dh_insercao_raw": f.lit(var_adf["adf_trigger_time"]).cast("timestamp"),
                              "kv_process_control": f.from_json(f.lit(json.dumps(var_adf)), schema=var_kv_process_control_schema)
                             }
              }

# COMMAND ----------

df = rcf.add_control_fields(df, var_options)

# COMMAND ----------

adl_file_time = cf.list_adl_files(spark, dbutils, var_source, file_system='datalake', scope='adls_gen2')
df = df.join(adl_file_time, on='nm_arq_in', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality

# COMMAND ----------

parse_ba, file_name_mask = uf.parse_ba_doc(dbutils, var_adl_path=var_file_parse['file_path'], headers=var_file_parse['headers'], file_sheet=var_file['file_folder'], file_system='datalake', scope='adls_gen2')
parse_ba

# COMMAND ----------

def is_numeric(s):
    if s is None:
        return True
    else:
        try:
            float(s)
            return True
        except ValueError:
            return False

# COMMAND ----------

#udf for check if is a numeric field
is_numeric_udf = f.udf(is_numeric, types.BooleanType())

# COMMAND ----------

df = df.withColumn("decimal_point_error", f.lit(0))

# COMMAND ----------

#Check if there are alphanumeric characters in numeric fields and if the decimal fields contain '.'. According to the documentation, the decimal data must be separated by a comma
for org, dst, _type in parse_ba[var_file['file_folder']]:
  if _type.lower().split('(')[0] in ['double','decimal', 'float', "integer", "long", "short"]:
    df = df.withColumn("numeric_error", 
                       f.when(is_numeric_udf(f.trim(f.regexp_replace(org, ',', '.'))),                               
                              f.lit(0))\
                       .otherwise(1))
    if _type.lower().split('(')[0] in ['double','decimal']:
      df = df.withColumn("decimal_point_error", 
                         f.when(f.col(org).contains('.'),
                                f.lit(1))\
                         .otherwise(f.col("decimal_point_error")))

# COMMAND ----------

#Filtering by the year or year / month corresponding to the name of the origin file
df = df.withColumn("split_file_name", f.split(f.split(f.col("nm_arq_in"), '\.').getItem(0), '_'))\
.withColumn("file_year_month", f.element_at(f.col('split_file_name'), -1))\
.withColumn("file_year", f.substring(f.col("file_year_month"), 1, 4))\
.withColumn("file_month", f.substring(f.col("file_year_month"), 5, 2))\
.withColumn("cd_ano_fechamento_value_error", 
            f.when(f.coalesce(f.col("cd_ano_fechamento"), f.lit(0)) != f.col("file_year").cast("int"), f.lit(1))\
            .otherwise(0))\
.withColumn("cd_mes_fechamento_value_error", 
            f.when(f.coalesce(f.col("cd_mes_fechamento"), f.lit(0)) != f.col("file_month").cast("int"), f.lit(1))\
            .otherwise(0))\
.drop("split_file_name","file_year_month","file_year", "file_month")

# COMMAND ----------

#Creating dataframe with files to be rejected and their corresponding errors. 
#If an error record exists, the entire file is rejected
df_reject_files = df.filter((f.col("decimal_point_error") == 1) |\
                          (f.col("numeric_error") == 1) |\
                          (f.col("cd_ano_fechamento_value_error") == 1))\
.groupBy("nm_arq_in")\
.agg(f.sum("decimal_point_error").alias("decimal_point_error"),
     f.sum("numeric_error").alias("numeric_error"),
     f.sum("cd_ano_fechamento_value_error").alias("cd_ano_fechamento_value_error"),
     f.sum("cd_mes_fechamento_value_error").alias("cd_mes_fechamento_value_error"))\
.withColumn("decimal_point_error",
            f.when(f.col("decimal_point_error") > 1, 
                   f.lit("decimal_point_error"))\
            .otherwise(f.lit(None)))\
.withColumn("numeric_error",
            f.when(f.col("numeric_error") > 1, 
                   f.lit("numeric_error"))\
            .otherwise(f.lit(None)))\
.withColumn("cd_ano_fechamento_value_error",
            f.when(f.col("cd_ano_fechamento_value_error") > 1, 
                   f.lit("cd_ano_fechamento_value_error"))\
            .otherwise(f.lit(None)))\
.withColumn("cd_mes_fechamento_value_error",
            f.when(f.col("cd_mes_fechamento_value_error") > 1, 
                   f.lit("cd_mes_fechamento_value_error"))\
            .otherwise(f.lit(None)))\
.withColumn("errors", f.array(f.col("decimal_point_error"), f.col("numeric_error"), f.col("cd_ano_fechamento_value_error"), f.col("cd_mes_fechamento_value_error")))\
.drop("decimal_point_error", "numeric_error", "cd_ano_fechamento_value_error", "cd_mes_fechamento_value_error")

# COMMAND ----------

# Filtering only data corresponding to files accepted by the data_quality process
df = df.join(df_reject_files.select("nm_arq_in"),
             ["nm_arq_in"],
             "leftanti")\
.drop("decimal_point_error", "numeric_error", "cd_ano_fechamento_value_error", "cd_mes_fechamento_value_error")

# COMMAND ----------

control_columns = ["kv_process_control", "nm_arq_in", "dh_insercao_raw", "nr_reg", "dh_arq_in"]

# COMMAND ----------

#Selecting columns from documentation and control columns
def __select(parse_ba, key, control_columns):
  for org, dst, _type in parse_ba[key]:
    if org == 'N/A' and dst not in df.columns:
      yield f.lit(None).cast(_type).alias(dst)
    else:
      _col = f.col(org)
      if _type.lower().split('(')[0] in ['double','decimal']:
        _col = f.regexp_replace(org, ',', '.')
      yield _col.cast(_type).alias(dst)
  for cc in control_columns:
    _col = f.col(cc)
    yield _col
      
df = df.select(*__select(parse_ba=parse_ba, key=var_file['file_folder'], control_columns=control_columns))

# COMMAND ----------

#Creating the process return dictionary with the list of rejected files and the set of errors of each rejected file
list_files_reject = map(lambda row: row.asDict(), df_reject_files.collect())

# COMMAND ----------

dict_files_reject = {file['nm_arq_in']: file['errors'] for file in list_files_reject}

# COMMAND ----------

data_quality_dict = {'list_files_to_reject': [k for k, v in dict_files_reject.items() if v], 'errors': {k: list(filter(None, v)) for k, v in dict_files_reject.items() if v}}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write on ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC If there's no data and it is the first load, we must save the data frame without partitioning, because saving an empty partitioned dataframe does not save the metadata.
# MAGIC When there is no new data in the raw and you already have other data from other loads, nothing happens.
# MAGIC And when we have new data, it is normally saved with partitioning.

# COMMAND ----------

if df.count()==0:
  try:
    spark.read.parquet(var_sink)
  except AnalysisException:
    df. \
    write. \
    save(path=var_sink, format="parquet", mode="overwrite")  
  
  data_quality_dict.update({"data_count_is_zero": 1})
  dbutils.notebook.exit(data_quality_dict)

# COMMAND ----------

df.repartition(1).write.partitionBy('cd_ano_fechamento', 'cd_mes_fechamento', 'dh_arq_in').parquet(path=var_sink, mode='overwrite')

# COMMAND ----------

dbutils.notebook.exit(data_quality_dict)

# COMMAND ----------

