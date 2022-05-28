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

# table = {
#   'path_origin': 'crw/inep_enem/enem_escola/',
#   'path_destination': 'inep_enem/enem_escola',
#   'prm_path': '/prm/usr/inep_enem/KC2332_Painel_ENEM_mapeamento_trusted.xlsx',
#   'partition_col': 'NR_ANO'
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_elegiveis_16_18',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

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

df = spark.read.parquet(source)

# COMMAND ----------

sheet_name = 'raw_trs_ENEM_ESCOLA'
parse_ba = cf.parse_ba_doc(dbutils, 
                           table['prm_path'], 
                           headers={'name_header': 'Campo Origem', 'pos_header': 'C', 'pos_org': 'C', 'pos_dst': 'E', 'pos_type': 'F'}, 
                           sheet_names=[sheet_name])

# COMMAND ----------

cf.check_ba_doc(df, parse_ba, sheet_name)

# COMMAND ----------

select_alias = (f.col(org).cast(_type).alias(dst) for org, dst, _type in parse_ba[sheet_name])
df = df.select(*select_alias)

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.write.partitionBy(table['partition_col']).parquet(target, mode='overwrite')