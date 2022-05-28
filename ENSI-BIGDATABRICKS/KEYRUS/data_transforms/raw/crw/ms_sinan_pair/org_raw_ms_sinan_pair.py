# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

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

prm = dls['folders']['prm']
prm_file = table["prm_path"].split('/')[-1]
prm_path = f'{prm}/usr/{table["schema"]}_{table["table"]}/{prm_file}'

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

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

list_files = []

for lnd_year_path in sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: int(p.split('/')[-1])):
  list_files += [adl_path+'/'+file_ for file_ in cf.list_subdirectory(dbutils, lnd_year_path) if file_.endswith('.parquet')]

dfs = list(map(spark.read.parquet, list_files))

# COMMAND ----------

sheet_name = table["table"].upper()

for i in range(len(dfs)):
  cf.check_ba_doc(dfs[i], parse_ba=var_prm_dict, sheet=sheet_name)
  dfs[i] = dfs[i].select(*__transform_columns(sheet_name))
  dfs[i] = cf.append_control_columns(dfs[i], dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])

func_args = partial(DataFrame.unionByName, allowMissingColumns=True)
df = reduce(func_args, dfs)

# COMMAND ----------

df_adl_files = cf.list_adl_files(spark, dbutils, lnd_path)

df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')

df.write.mode('overwrite').partitionBy('NU_ANO').parquet(path=adl_raw)
