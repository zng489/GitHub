# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
import pyspark.sql.functions as f

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# # USE THIS ONLY FOR DEVELOPMENT PURPOSES
# tables =  {
#   "path_origin": "crw/ibge/pnadc",
#   "path_destination": "ibge/pnadc"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_cadastro_cbo',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

df_int_columns = ['ANO', 'TRIMESTRE', 'UF', 'CAPITAL', 'RM_RIDE', 'UPA', 'ESTRATO', 'V1008', 'V1014', 'V1016', 'V1022', 'V1023', 'V1029', 'POSEST', 'V2001', 'V2003', 'V2005', 'V2007', 'V2008', 'V20081', 'V20082', 'V2009', 'V2010', 'V3001', 'V3002', 'V3002A', 'V3003', 'V3003A', 'V3004', 'V3005', 'V3005A', 'V3006', 'V3006A', 'V3007', 'V3008', 'V3009', 'V3009A', 'V3010', 'V3011', 'V3011A', 'V3012', 'V3013', 'V3013A', 'V3013B', 'V3014', 'V4001', 'V4002', 'V4003', 'V4004', 'V4005', 'V4006', 'V4006A', 'V4007', 'V4008', 'V40081', 'V40082', 'V40083', 'V4009', 'V4010', 'V4012', 'V40121', 'V4013', 'V40132', 'V40132A', 'V4014', 'V4015', 'V40151', 'V401511', 'V401512', 'V4016', 'V40161', 'V40162', 'V40163', 'V4017', 'V40171', 'V401711', 'V4018', 'V40181', 'V40182', 'V40183', 'V4019', 'V4020', 'V4021', 'V4022', 'V4024', 'V4025', 'V4026', 'V4027', 'V4028', 'V4029', 'V4032', 'V4033', 'V40331', 'V403311', 'V40332', 'V403321', 'V40333', 'V403331', 'V4034', 'V40341', 'V403411', 'V40342', 'V403421', 'V4039', 'V4039C', 'V4040', 'V40401', 'V40402', 'V40403', 'V4041', 'V4043', 'V40431', 'V4044', 'V4045', 'V4046', 'V4047', 'V4048', 'V4049', 'V4050', 'V40501', 'V405011', 'V40502', 'V405021', 'V40503', 'V405031', 'V4051', 'V40511', 'V405111', 'V40512', 'V405121', 'V4056', 'V4056C', 'V4057', 'V4058', 'V40581', 'V405811', 'V40582', 'V405821', 'V40583', 'V405831', 'V40584', 'V4059', 'V40591', 'V405911', 'V40592', 'V405921', 'V4062', 'V4062C', 'V4063', 'V4063A', 'V4064', 'V4064A', 'V4071', 'V4072', 'V4072A', 'V4073', 'V4074', 'V4074A', 'V4075A', 'V4075A1', 'V4076', 'V40761', 'V40762', 'V40763', 'V4077', 'V4078', 'V4078A', 'V4082', 'VD2002', 'VD2003', 'VD2004', 'VD3004', 'VD3005', 'VD3006', 'VD4001', 'VD4002', 'VD4003', 'VD4004', 'VD4004A', 'VD4005', 'VD4007', 'VD4008', 'VD4009', 'VD4010', 'VD4011', 'VD4012', 'VD4013', 'VD4014', 'VD4015', 'VD4018', 'VD4023', 'VD4030', 'VD4031', 'VD4032', 'VD4033', 'VD4034', 'VD4035', 'VD4036', 'VD4037']
df_int_columns = (f.col(col).cast('Int').alias(col) for col in df_int_columns)

df_double_columns = ['V1027', 'V1028', 'V403312', 'V403322', 'V403412', 'V403422', 'V405012', 'V405022', 'V405112', 'V405122', 'V405812', 'V405822', 'V405912', 'V405922', 'VD4016', 'VD4017', 'VD4019', 'VD4020']
df_double_columns = (f.col(col).cast('Double').alias(col) for col in df_double_columns)

df = df.select(*df_int_columns, *df_double_columns)

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('ANO').parquet(path=target, mode='overwrite')