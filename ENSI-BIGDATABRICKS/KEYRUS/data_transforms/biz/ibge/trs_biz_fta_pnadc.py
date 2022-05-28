# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import json
import re
import pyspark.sql.functions as f

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

# tables =  {
#   "path_origin": "ibge/pnadc",
#   "path_dim_ocup": "ibge/ocup_industriais",
#   "path_dim_cnae": "ibge/cnae_industriais",
#   "path_destination": "ibge/fta_pnadc",
#   "destination": "/ibge/fta_pnadc",
#   "databricks": {
#     "notebook": "/biz/ibge/trs_biz_fta_pnadc"
#   }
# }

# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs","business":"/biz"}}
# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'trs_biz_dim_estrutura_territorial',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

trs = dls['folders']['trusted']
biz = dls['folders']['business']

# COMMAND ----------

source = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_origin"])
source

# COMMAND ----------

source_ocup = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_dim_ocup"])
source_ocup

# COMMAND ----------

source_cnae = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_dim_cnae"])
source_cnae

# COMMAND ----------

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_destination"])
target

# COMMAND ----------

df_source = spark.read.parquet(source)
df_source_columns = ['ANO', 'TRIMESTRE', 'UF']

df_source_columns_renamed = [('V4010', 'CBO'), ('V4013', 'CNAE'), ('V1028', 'quantidade_trabalhadores')]
df_source_columns_renamed = (f.col(org).alias(dst) for org, dst in df_source_columns_renamed)

df_source = df_source.select(*df_source_columns, *df_source_columns_renamed, 'VD4002', 'VD4009', 'VD4012', 'VD4017', 'V1028', 'V4009', 'V4019', 'V4021', 'V4025', 'V4039')

# COMMAND ----------

df_source = df_source.withColumn('CNAE', f.when(f.col('CNAE') > f.lit(9999), f.substring(f.col('CNAE'), 0, 2).cast('Int')).otherwise(f.substring(f.col('CNAE'), 0, 1).cast('Int')))

# COMMAND ----------

df_source = df_source.withColumn('MERCADO_TRABALHO1', 
                     f.when(f.col('VD4002') == f.lit(2), f.lit('Desocupado'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (((f.col('VD4009') == f.lit(1)) & (f.col('V4039') >= f.lit(40)) & (f.col('V4009') == f.lit(1)) & (f.col('V4025') == f.lit(2)) & (f.col('V4021') == f.lit(1))) | (f.col('VD4009') == f.lit(3))), f.lit('Empregado formal padrão'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009') == f.lit(1)) & ~((f.col('V4039') >= f.lit(40)) & (f.col('V4009') == f.lit(1)) & (f.col('V4025') == f.lit('2')) & (f.col('V4021') == f.lit(1)))), f.lit('Empregado formal não-padrão (+flexível)'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009').isin([2, 4, 10])) & (f.col('VD4012') == f.lit(1))), f.lit('Empregado informal contribuinte'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009').isin([2, 4, 10])) & (f.col('VD4012') == f.lit(2))), f.lit('Empregado informal não contribuinte'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009').isin([5, 6, 7])), f.lit('Servidores'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009') == f.lit(8)), f.lit('Empregadores'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009') == f.lit(9)) & (f.col('VD4012') == f.lit(1)) & (f.col('V4019') == f.lit(1))), f.lit('Trabalho independente formal contribuinte'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009') == f.lit(9)) & (f.col('VD4012') == f.lit(2)) & (f.col('V4019') == f.lit(1))), f.lit('Trabalho independente formal não contribuinte'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009') == f.lit(9)) & (f.col('VD4012') == f.lit(1)) & (f.col('V4019') == f.lit(2))), f.lit('Trabalho independente informal contribuinte'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & ((f.col('VD4009') == f.lit(9)) & (f.col('VD4012') == f.lit(2)) & (f.col('V4019') == f.lit(2))), f.lit('Trabalho independente informal não contribuinte'))
                      .otherwise(f.lit('NÃO CLASSIFICADO')))))))))))))

# COMMAND ----------

df_source = df_source.withColumn('MERCADO_TRABALHO2',
                     f.when(f.col('VD4002') == f.lit(2), f.lit('Desocupado'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009').isin([1, 3])), f.lit('Empregado formal'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009').isin([2, 4, 10])), f.lit('Empregado informal'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009').isin([5, 6, 7])), f.lit('Empregado do setor público'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009') == f.lit(8)), f.lit('Empregador'))
                      .otherwise(
                     f.when((f.col('VD4002') == f.lit(1)) & (f.col('VD4009') == f.lit(9)), f.lit('Conta própria'))
                      .otherwise('NÃO CLASSIFICADO')))))))

# COMMAND ----------

df_source = df_source.withColumn('TEMA_GERAL', 
                                 f.when(f.col('MERCADO_TRABALHO1') == f.lit('Empregado formal padrão'), f.lit('Trabalho padrão'))
                                  .otherwise('Trabalho não padrão'))

# COMMAND ----------

df_source = df_source.withColumn('massa_salarial', f.col('VD4017') * f.col('V1028'))

# COMMAND ----------

df_ocup = spark.read.parquet(source_ocup)
df_ocup = df_ocup.select(f.col('DESC_GRUPO_FAMILIA').alias('CBO_AGG'), f.col('GRUPO_DE_BASE').alias('CBO'))

# COMMAND ----------

df_cnae = spark.read.parquet(source_cnae)
df_cnae = df_cnae.select(f.col('AGRUPAMENTO').alias('CNAE_AGG'), f.col('DIVISAO').alias('CNAE'))

# COMMAND ----------

df = df_source.join(df_ocup, on='CBO', how='left')
df = df.join(df_cnae, on='CNAE', how='left')

# COMMAND ----------

ordered_columns = ['ano', 'trimestre', 'uf', 'tema_geral', 'mercado_trabalho1', 'mercado_trabalho2', 'cbo', 'cbo_agg', 'cnae', 'cnae_agg', 'quantidade_trabalhadores', 'massa_salarial']
df = df.select(*ordered_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.coalesce(1).write.partitionBy('ANO').parquet(target, mode='overwrite')