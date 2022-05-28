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
#   "path_origin": "me/caged",
#   "path_territorial_structure": "corporativo/dim_estrutura_territorial",
#   "path_cbo": "uniepro/dim_cadastro_cbo",
#   "path_cnae": "uniepro/dim_cnae_subclasses",
#   "path_age_range": "uniepro/dim_faixa_etaria",
#   "path_education_degree": "uniepro/dim_grau_instrucao",
#   "path_destination": "uniepro/fta_caged_kpi",
#   "destination": "/uniepro/fta_caged_kpi",
#   "databricks": {
#     "notebook": "/biz/me/trs_biz_fta_caged_kpi"
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

source_territorial_structure = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_territorial_structure"])
source_territorial_structure

# COMMAND ----------

source_cbo = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_cbo"])
source_cbo

# COMMAND ----------

source_cnae = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_cnae"])
source_cnae

# COMMAND ----------

source_age_range = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_age_range"])
source_age_range

# COMMAND ----------

source_edu_degree = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_education_degree"])
source_edu_degree

# COMMAND ----------

target = "{adl_path}{biz}/{origin}".format(adl_path=var_adls_uri, biz=biz, origin=tables["path_destination"])
target

# COMMAND ----------

df_source = spark.read.parquet(source)

df_source_columns = ['CD_MUNICIPIO', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'VL_IDADE', 'CD_GRAU_INSTRUCAO', 'FL_ADMITIDOS_DESLIGADOS', 'CD_CBO', 'CD_CNAE10_CLASSE', 'CD_CNAE20_CLASSE', 'CD_CNAE20_SUBCLASSE', 'CD_SEXO', 'VL_SALARIO_MENSAL', 'NR_MES_TEMPO_EMPREGO', 'QT_HORA_CONTRAT', 'CD_ANO_MOVIMENTACAO']

df_source_renamed_columns = [('CD_ANO_MES_COMPETENCIA_DECLARADA', 'DT_COMPETENCIA'), ('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO', 'DT_MOVIMENTACAO'), ('CD_SALDO_MOV', 'VL_SALDO_MOV'), ('CD_ANO_MOVIMENTACAO', 'ANO')]
df_source_renamed_columns = (f.col(org).alias(dst) for org, dst in df_source_renamed_columns)

df_source = df_source.select(*df_source_columns, *df_source_renamed_columns)

date_columns = ['DT_COMPETENCIA', 'DT_MOVIMENTACAO']
for col in date_columns:
  df_source = df_source.withColumn(col, f.from_unixtime(f.unix_timestamp(f.col(col).cast('String'), 'yyyyMM')).cast('Date'))

# COMMAND ----------

df_territorial = spark.read.parquet(source_territorial_structure)
df_territorial_columns = ['NM_MUNICIPIO', 'NM_REGIAO_GEOGRAFICA', 'NM_UF']

df_territorial = df_territorial.select(*df_territorial_columns, f.col('CD_MUNICIPIO_6POSICOES').alias('CD_MUNICIPIO'))

# COMMAND ----------

df_cbo = spark.read.parquet(source_cbo)
df_cbo_columns = ['DS_CBO6', 'DS_TIPO_FAMILIA', 'DS_GRUPO_FAMILIA']

df_cbo = df_cbo.select(*df_cbo_columns, f.col('CD_CBO6').alias('CD_CBO'))

# COMMAND ----------

df_cnae = spark.read.parquet(source_cnae)
df_cnae_columns = ['DS_DENOMINACAO']

df_cnae = df_cnae.select(*df_cnae_columns, f.regexp_replace(f.col('CD_SUBCLASSE'), r'[-\/]', '').alias('CD_CNAE20_SUBCLASSE'))

# COMMAND ----------

df_age_range = spark.read.parquet(source_age_range)
df_age_range_columns = ['CD_FAIXA_ETARIA', 'DS_FAIXA_ETARIA']

df_age_range = df_age_range.select(*df_age_range_columns, 'NR_DE', 'NR_ATE')

# COMMAND ----------

df_edu_degree = spark.read.parquet(source_edu_degree)
df_edu_degree_columns = ['DS_GRAU_INSTRUCAO', 'DS_AGG_GRAU_INSTRUCAO']

df_edu_degree = df_edu_degree.select(*df_edu_degree_columns, 'CD_GRAU_INSTRUCAO')

# COMMAND ----------

df = df_source.join(df_cbo, on='CD_CBO', how='left')
df = df.join(df_edu_degree, on='CD_GRAU_INSTRUCAO', how='left')
df = df.join(df_territorial, on='CD_MUNICIPIO', how='left')
df = df.join(df_cnae, on='CD_CNAE20_SUBCLASSE', how='left')
df = df.join(df_age_range, on=df.VL_IDADE.between(df_age_range.NR_DE, df_age_range.NR_ATE), how='left')

# COMMAND ----------

df_columns = ['DT_COMPETENCIA', 'DT_MOVIMENTACAO', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'NM_REGIAO_GEOGRAFICA', 'NM_UF', 'VL_IDADE', 'CD_FAIXA_ETARIA', 'DS_FAIXA_ETARIA', 'CD_GRAU_INSTRUCAO', 'DS_GRAU_INSTRUCAO', 'DS_AGG_GRAU_INSTRUCAO', 'FL_ADMITIDOS_DESLIGADOS', 'CD_CBO', 'DS_CBO6', 'DS_TIPO_FAMILIA', 'DS_GRUPO_FAMILIA', 'CD_CNAE10_CLASSE', 'CD_CNAE20_CLASSE', 'CD_CNAE20_SUBCLASSE', 'DS_DENOMINACAO', 'CD_SEXO', 'VL_SALARIO_MENSAL', 'VL_SALDO_MOV', 'NR_MES_TEMPO_EMPREGO', 'QT_HORA_CONTRAT', 'ANO']

df = df.select(*df_columns)

# COMMAND ----------

df = df.withColumn('dh_insercao_biz', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.repartition(15).write.partitionBy('ANO').parquet(target, mode='overwrite')