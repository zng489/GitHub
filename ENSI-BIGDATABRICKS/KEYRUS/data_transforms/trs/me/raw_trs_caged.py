# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
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

# tables =  {
#   "path_caged": "crw/me/caged",
#   "path_caged_adjust": "crw/me/caged_ajustes",
#   "path_destination": "me/caged"
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_caged',
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

source_caged = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_caged"])
source_caged

# COMMAND ----------

source_caged_adjust = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_caged_adjust"])
source_caged_adjust

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df_caged = spark.read.parquet(source_caged)
df_caged_columns = ['FL_ADMITIDOS_DESLIGADOS', 'CD_BAIRRO_FORTALEZA', 'CD_BAIRRO_RJ', 'CD_BAIRRO_SP', 'CD_CBO', 'CD_CNAE10_CLASSE', 'CD_CNAE20_CLASSE', 'CD_CNAE20_SUBCLASSE', 'CD_DISTRITO_SP', 'CD_FAIXA_EMPR_INICIO_JAN', 'CD_GRAU_INSTRUCAO', 'CD_IBGE_SUBSETOR', 'VL_IDADE', 'FL_IND_APRENDIZ', 'FL_IND_PORTADOR_DEFIC', 'FL_IND_TRAB_INTERMITENTE', 'FL_IND_TRAB_PARCIAL', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'CD_MUNICIPIO', 'QT_HORA_CONTRAT', 'COD_RACA_COR', 'CD_REGIAO_ADM_RJ', 'CD_REGIAO_ADM_SP', 'CD_REGIAO_COREDE', 'CD_REGIAO_COREDE04', 'CD_REGIAO_GOV_SP', 'CD_REGIAO_SENAC_PR', 'CD_REGIAO_SENAI_PR', 'CD_REGIAO_SENAI_SP', 'CD_REGIOES_ADM_DF', 'VL_SALARIO_MENSAL', 'CD_SALDO_MOV', 'CD_SEXO', 'CD_SUB_REGIAO_SENAI_PR', 'NR_MES_TEMPO_EMPREGO', 'CD_TIPO_DEFIC', 'CD_TIPO_ESTAB', 'CD_TIPO_MOV_DESAGREGADO', 'CD_UF']

df_caged = df_caged.select(*df_caged_columns, 
                           f.col('CD_ANO_COMPETENCIA').alias('CD_ANO_MOVIMENTACAO'),
                           f.col('CD_ANO_MES_COMPETENCIA_DECLARADA').alias('CD_ANO_MES_COMPETENCIA_DECLARADA'),
                           f.col('CD_ANO_MES_COMPETENCIA_DECLARADA').alias('CD_ANO_MES_COMPETENCIA_MOVIMENTACAO'),
                           f.lit(None).alias('CD_CBO94'),
                           f.lit(None).alias('CD_REGIAO_METRO_MTE'),
                           f.lit('CAGED').alias('DS_FONTE'))

# COMMAND ----------

df_caged_adjust = spark.read.parquet(source_caged_adjust)

df_caged_adjust_columns = ['FL_ADMITIDOS_DESLIGADOS', 'CD_ANO_MOVIMENTACAO', 'CD_BAIRRO_FORTALEZA', 'CD_BAIRRO_RJ', 'CD_BAIRRO_SP', 'CD_CBO', 'CD_CNAE10_CLASSE', 'CD_CNAE20_SUBCLASSE', 'CD_ANO_MES_COMPETENCIA_DECLARADA', 'CD_ANO_MES_COMPETENCIA_MOVIMENTACAO', 'CD_DISTRITO_SP', 'CD_FAIXA_EMPR_INICIO_JAN', 'CD_GRAU_INSTRUCAO', 'CD_IBGE_SUBSETOR', 'VL_IDADE', 'FL_IND_APRENDIZ', 'FL_IND_PORTADOR_DEFIC', 'FL_IND_TRAB_INTERMITENTE', 'FL_IND_TRAB_PARCIAL', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'CD_MUNICIPIO', 'CD_CBO94', 'QT_HORA_CONTRAT', 'COD_RACA_COR', 'CD_REGIAO_ADM_RJ', 'CD_REGIAO_ADM_SP', 'CD_REGIAO_COREDE', 'CD_REGIAO_COREDE04', 'CD_REGIAO_GOV_SP', 'CD_REGIAO_SENAC_PR', 'CD_REGIAO_SENAI_PR', 'CD_REGIAO_SENAI_SP', 'CD_REGIOES_ADM_DF', 'VL_SALARIO_MENSAL', 'CD_SALDO_MOV', 'CD_SEXO', 'CD_SUB_REGIAO_SENAI_PR', 'NR_MES_TEMPO_EMPREGO', 'CD_TIPO_DEFIC', 'CD_TIPO_ESTAB', 'CD_TIPO_MOV_DESAGREGADO', 'CD_REGIAO_METRO_MTE']

df_caged_adjust = df_caged_adjust.select(*df_caged_adjust_columns,
                                          f.col('CD_UF_LOCALIZACAO_ESTABELECIMENTO').alias('CD_UF'),
                                          f.substring(f.col('CD_CNAE20_SUBCLASSE'), 0, 5).alias('CD_CNAE20_CLASSE'),
                                          f.lit('AJUSTE').alias('DS_FONTE'))

# COMMAND ----------

df = df_caged.union(df_caged_adjust.select(*df_caged.columns))

# COMMAND ----------

ordered_columns = ['FL_ADMITIDOS_DESLIGADOS', 'CD_ANO_MOVIMENTACAO', 'CD_BAIRRO_FORTALEZA', 'CD_BAIRRO_RJ', 'CD_BAIRRO_SP', 'CD_CBO', 'CD_CNAE10_CLASSE', 'CD_CNAE20_CLASSE', 'CD_CNAE20_SUBCLASSE', 'CD_ANO_MES_COMPETENCIA_DECLARADA', 'CD_ANO_MES_COMPETENCIA_MOVIMENTACAO', 'CD_DISTRITO_SP', 'CD_FAIXA_EMPR_INICIO_JAN', 'CD_GRAU_INSTRUCAO', 'CD_IBGE_SUBSETOR', 'VL_IDADE', 'FL_IND_APRENDIZ', 'FL_IND_PORTADOR_DEFIC', 'FL_IND_TRAB_INTERMITENTE', 'FL_IND_TRAB_PARCIAL', 'CD_MESORREGIAO', 'CD_MICRORREGIAO', 'CD_MUNICIPIO', 'CD_CBO94', 'QT_HORA_CONTRAT', 'COD_RACA_COR', 'CD_REGIAO_ADM_RJ', 'CD_REGIAO_ADM_SP', 'CD_REGIAO_COREDE', 'CD_REGIAO_COREDE04', 'CD_REGIAO_GOV_SP', 'CD_REGIAO_SENAC_PR', 'CD_REGIAO_SENAI_PR', 'CD_REGIAO_SENAI_SP', 'CD_REGIOES_ADM_DF', 'VL_SALARIO_MENSAL', 'CD_SALDO_MOV', 'CD_SEXO', 'CD_SUB_REGIAO_SENAI_PR', 'NR_MES_TEMPO_EMPREGO', 'CD_TIPO_DEFIC', 'CD_TIPO_ESTAB', 'CD_TIPO_MOV_DESAGREGADO', 'CD_UF', 'CD_REGIAO_METRO_MTE', 'DS_FONTE']

df = df.select(*ordered_columns)

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

(df
 .repartition(10)
 .write
 .partitionBy('CD_ANO_MOVIMENTACAO', 'CD_ANO_MES_COMPETENCIA_MOVIMENTACAO')
 .parquet(path=target, mode='overwrite'))