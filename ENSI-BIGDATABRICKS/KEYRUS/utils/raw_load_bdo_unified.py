# Databricks notebook source
# MAGIC %md
# MAGIC ADLS data access section

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

# MAGIC %md 
# MAGIC Common import section. Declare your imports all here

# COMMAND ----------

import json
import re
import pyspark.sql.functions as functions
import pyspark.sql.types as types
from raw_loader import raw_loader as rl

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### For raw load of BDO sources, DLS come from the switch clause in ADF. 
# MAGIC ### Since it is ADF responsibility to control and decide things on the pipeline.
# MAGIC 
# MAGIC "env" variable is not needed here. Pipeline will decide upon it.
# MAGIC <pre>
# MAGIC 
# MAGIC Needed variables from ADF are (and all of them are dict):
# MAGIC <ul>
# MAGIC <li>table</li>
# MAGIC <li>adf</li>
# MAGIC <li>dls</li>
# MAGIC </ul> 
# MAGIC </pre>

# COMMAND ----------

"""
'table', 'adf' and 'dls' cause abortion when failed to receive.
"""
var_has_env = False

var_table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("table")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))

# COMMAND ----------

var_table

# COMMAND ----------

var_adf

# COMMAND ----------

var_dls

# COMMAND ----------

"""
Just a note to remember. This can't be implemented here.
"""
"""
if var_has_env is True and var_env["env"] == "dev":
  var_dls = {
    "folders":{
      "landing":"/tmp/dev/lnd",  
      "error":"/tmp/dev/err",
      "staging":"/tmp/dev/stg",
      "log":"/tmp/dev/log",
      "raw":"/tmp/dev/raw"
    }
  }

elif var_has_env is False or var_env["env"] == "prod":
  var_dls = {
    "folders":{
      "landing":"/lnd",  
      "error":"/err",
      "staging":"/stg",
      "log":"/log",
      "raw":"/raw"
    }
  }
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC These will be values received by parameter from ADF.
# MAGIC 
# MAGIC There's an added section called "raw". In there, there's the key "partition_by". Now I've got yo find a good way to implement this. 
# MAGIC 
# MAGIC ### This assumes pysparl.sql.functions totally loaded and available as a variable called "functions"

# COMMAND ----------

"""
New Implementation. Use while developing. 

In ADF, escape implementation quotes with \\' !!!
"""

"""
var_table = {
  'schema': 'BD_BASI',
  'table':'TB_CURSO',
  'load_type':'incremental',
  'partition_column':'null',
  'control_column':'DT_ATUALIZACAO',
  'control_column_type_2_db':'datetime', 
  'control_column_default_value': '19000101000000', 
  'control_column_mask_value': 'DD/MM/YYHH24:MI:SSFF', 
  'columns': 'CD_CBO_6,CD_AREA_ATUACAO,DT_INICIO_OFERTA,NM_CURSO,DT_ATUALIZACAO,CD_PRODUTO_SERVICO,CD_CURSO_SCOP,CD_CURSO,FL_EXCLUIDO,NR_CARGA_HORARIA_ESTAGIO,DT_FIM_OFERTA,CD_ENTIDADE_REGIONAL,CD_CURSO_DR,NR_CARGA_HORARIA,CD_CURSO_MEC,CD_LINHA_ACAO,DS_ORIGEM,FL_PORTFOLIO',
  'raw': {'partition_by': [
    {'col_name': 'YEAR', 'implementation': 'df.withColumn(\\'YEAR\\', functions.year(functions.col(\\'DT_ATUALIZACAO\\')))'},
    {'col_name': 'MONTH', 'implementation': 'df.withColumn(\\'MONTH\\', functions.month(functions.col(\\'DT_ATUALIZACAO\\')))'}
  ]
}
}
"""

# COMMAND ----------

# MAGIC %md
# MAGIC In case of history load, we need to replace the path from raw to hst. Parameter var_table must contain key "is_history" that should be set to 'True' or 'true'.

# COMMAND ----------

if 'is_history' in var_table:
  if str(var_table['is_history']).lower() == 'true':
    var_dls["folders"]["raw"] = var_dls["folders"]["raw"].replace("raw", "hst")

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

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
           "control_fields": {"nm_arq_in": functions.lit(None).cast("integer"), 
                              "nr_reg": functions.lit(None).cast("integer"), 
                              "dh_arq_in": functions.lit(None).cast("timestamp"), 
                              "dh_insercao_raw": functions.lit(var_adf["adf_trigger_time"]).cast("timestamp"),
                              "kv_process_control": functions.from_json(functions.lit(json.dumps(var_adf)), schema=var_kv_process_control_schema)
                             }
              }

# COMMAND ----------

var_source = "{}{}/{}/{}/{}".format(var_adls_uri, 
                                var_dls["folders"]["landing"], 
                                "bdo", 
                                var_table["schema"], 
                                var_table["table"]
                               ).strip().lower()

var_source

# COMMAND ----------

df = spark.read.parquet(var_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reimplementing this:
# MAGIC 
# MAGIC <pre>
# MAGIC df = df. \
# MAGIC withColumn('YEAR', year(col(options["partition_column"]))). \
# MAGIC withColumn('MONTH', month(col(options["partition_column"])))
# MAGIC </pre>

# COMMAND ----------

var_write_partition =  False
try:
  var_partitions = var_table["raw"]["partition_by"]
  var_write_partition = True
except KeyError:
  print("No partitions defined for this table") 

# COMMAND ----------

"""
For this to work, the hierarchy of columns to partition must be in the precise order to be iplemented, and that's why we've got an array!
"""
if var_write_partition is True:
  var_options["partition_by"] = []
  for p in var_partitions:
    if "implementation" in p:
      df = eval(p["implementation"])
    """
    If there's no key "implementation", then it is an original natural column for partitioning
    """
    var_options["partition_by"].append(p["col_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC Load type switch

# COMMAND ----------

if var_table["load_type"] in ("incremental", "incremental_with_join"):
  print("Load incremental")
  rl.load_incremental(spark, df, var_table, var_dls, var_options)
elif var_table["load_type"] == "full":
  print("Load full")
  rl.load_full(df, var_table, var_dls, var_options)
elif var_table["load_type"] == "year_overwrite":
  print("Load year overwrite")
  rl.load_year_overwrite(df, var_table, var_dls, var_options)
elif var_table["load_type"] == "full_versioning":
  print("Load full versioning")
  rl.full_versioning(spark, df, var_table, var_dls, var_options)
else:
  print("Load type {} not implemented yet. Doing Nothing.".format(var_table["load_type"]))