# Databricks notebook source
import cni_connectors.adls_gen1_connector as connector
import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

parse_ba = cf.parse_ba_doc(dbutils, 
                           table['prm_path'], 
                           headers={'name_header': 'Campo Origem', 'pos_header': 'C', 'pos_org': 'C', 'pos_dst': 'E', 'pos_type': 'F'})

# COMMAND ----------

when_cols = {
  'TX_RESP_Q065': 'C',
  'TX_RESP_Q066': 'C',
  'TX_RESP_Q067': 'C',
  'TX_RESP_Q068': 'C',
  'TX_RESP_Q069': 'C',
  'TX_RESP_Q070': 'C',
  'TX_RESP_Q071': 'C',
  'TX_RESP_Q072': 'D',
  'TX_RESP_Q073': 'D',
  'TX_RESP_Q074': 'D'
}

# COMMAND ----------

df = spark.read.parquet(source)

last_sheet = list(parse_ba)[-1]

select_alias = (f.col(org).cast(_type).alias(dst) for org, dst, _type in parse_ba[last_sheet])
df = df.select(*select_alias)

for col_name, value in when_cols.items():
  df = df.withColumn(col_name, f.when((f.col('ID_PROVA_BRASIL') > f.lit(2011)) & (f.col(col_name) == f.lit(value)), f.lit(None)).otherwise(f.col(col_name)))

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))
df.write.partitionBy(table['partition_col']).parquet(target, mode='overwrite')
