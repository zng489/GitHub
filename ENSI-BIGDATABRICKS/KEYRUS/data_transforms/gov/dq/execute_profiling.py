# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

from dq.profiling import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import datetime

# COMMAND ----------

dbutils.widgets.text("source","")
dbutils.widgets.text("source_type","")
dbutils.widgets.text("schema","")
dbutils.widgets.text("table","")
dbutils.widgets.text("marca","")
# {'source': 'scan-rac11g', 'source_type': 'oracle', 'schema': 'integradorweb', 'table': 'tb_congelado_epmat', 'env': 'dev'}

# COMMAND ----------

source = dbutils.widgets.get("source")
source_type = dbutils.widgets.get("source_type")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
marca = dbutils.widgets.get("marca")

# COMMAND ----------

dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_profiling = "{adl_path}{default_dir}/gov/tables/profiling".format(adl_path=var_adls_uri,default_dir=default_dir)
if source_type == 'external':
  path_lake = "{adl_path}{default_dir}/raw/crw/{schema}/{table}"\
              .format(adl_path=var_adls_uri,default_dir=default_dir,schema=schema,table=table)
elif source_type == 'oracle':
  path_lake = "{adl_path}{default_dir}/raw/bdo/{schema}/{table}"\
              .format(adl_path=var_adls_uri,default_dir=default_dir,schema=schema,table=table)
elif source_type == 'sqlserver':
  path_lake = "{adl_path}{default_dir}/raw/bdo/{schema}/{table}"\
              .format(adl_path=var_adls_uri,default_dir=default_dir,schema=schema,table=table)  
elif source_type == 'bigdata':
  if source == 'trs':
     path_lake = "{adl_path}{default_dir}/trs/{schema}/{table}"\
                 .format(adl_path=var_adls_uri,default_dir=default_dir,schema=schema,table=table)
  if source == 'biz':
     path_lake = "{adl_path}{default_dir}/biz/{schema}/{table}"\
                 .format(adl_path=var_adls_uri,default_dir=default_dir,schema=schema,table=table)  

# COMMAND ----------

df_lake = spark.read.format("parquet").load(path_lake)
columns_to_drop = ['kv_process_control','nm_arq_in','nr_reg','dh_arq_in','dh_insercao_raw']
columns_to_drop = ["dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                   "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                   "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_insercao_raw","dh_exclusao_valor_pessoa",\
                   "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                   "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                   "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp"]
df_lake = df_lake.drop(*columns_to_drop)
names = df_lake.schema.names

# COMMAND ----------

#try:
#  df_lake = spark.read.format("parquet").load(path_lake)
#  names = df_lake.schema.names
#  for name in names:
#    #print(name)
#    if name == "kv_process_control":
#      df_lake = df_lake.drop(name)
#except (AnalysisException) as e:
#  print("Tabela não encontrada no caminho especificado")
#  raise e

# COMMAND ----------

#fill_rate_df = fill_rate(spark,df_lake,source,schema,table)
fill_count_df = fill_count(spark,df_lake,source,schema,table)

# COMMAND ----------

min_max_df = min_max_values(spark,df_lake,source,schema,table)

# COMMAND ----------

max_string_df = max_string_len(spark,df_lake,source,schema,table)

# COMMAND ----------

distinct_count_df = distinct_count(spark,df_lake,source,schema,table)

# COMMAND ----------

repeating_values_df = repeating_values(spark,df_lake,source,schema,table)

# COMMAND ----------

df_profiling = fill_count_df.union(min_max_df)\
                            .union(max_string_df)\
                            .union(distinct_count_df)\
                            .union(repeating_values_df)

# COMMAND ----------

# substitui a coluna de data/time pelo timestamp geral único de todo o processo
df_profiling = df_profiling.withColumn("created_at", to_timestamp(lit(marca + " 00:00:00.0000"), 'yyyy-MM-dd HH:mm:ss.SSSS'))

# COMMAND ----------

df_profiling.write.format("delta").mode("append").save(path_profiling)

# COMMAND ----------

profiling_dt = DeltaTable.forPath(spark, path_profiling)

profiling_dt.alias("source").merge(
             df_profiling.alias("target"),"source.metric is not null")\
             .whenNotMatchedInsertAll()\
             .execute()