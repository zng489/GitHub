# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw specific parameter section

# COMMAND ----------

import re
import json
from functools import reduce, partial

import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType, StringType

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

lnd_path = f'{lnd}/crw/{table["schema"]}__{table["table"]}'
raw_path = f'{raw}/crw/{table["schema"]}/{table["table"]}'

adl_path = var_adls_uri
adl_raw = f"{adl_path}{raw_path}"

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns(sheet_name):
    for org, dst, _type in var_prm_dict[sheet_name]:
        if org == 'N/A':
            yield f.lit(None).cast(_type).alias(dst)
        else:
            yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

list_files = sorted([f'{adl_path}/'+f for f in cf.list_subdirectory(dbutils, lnd_path)])

dfs = list(map(spark.read.parquet, list_files))
func_args = partial(DataFrame.unionByName, allowMissingColumns=True)
df = reduce(func_args, dfs)

# COMMAND ----------

filter_w = [cl for cl in df.columns if not cl.isnumeric()]
filter_n = sorted([cl for cl in df.columns if cl.isnumeric()])
df = df.select(filter_w + filter_n)

# COMMAND ----------

cl_dict = 'dict_ano_valor'
df = df.withColumn(cl_dict, f.to_json(f.struct(filter_n)))
df = df.withColumn(cl_dict,f.from_json(f.col(cl_dict),MapType(StringType(),StringType())))

list_columns = [f.col(cl) for cl in filter_w] + [f.explode(df.dict_ano_valor)]
df = df.select(list_columns)

df = df.withColumnRenamed('key','YEAR').withColumnRenamed('value','VALUE')

# COMMAND ----------

sheet_name = table["table"]
cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet_name)
df = df.select(*__transform_columns(sheet_name))

# COMMAND ----------

df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, lnd_path)

df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').partitionBy('YEAR').parquet(path=adl_raw)
