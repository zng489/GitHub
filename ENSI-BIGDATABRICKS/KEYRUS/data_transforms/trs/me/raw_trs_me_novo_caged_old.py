# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize
import datetime

import crawler.functions as cf
import json
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']


# COMMAND ----------

raw_novo_caged_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])
adl_raw_novo_caged = "{adl_path}{raw_novo_caged_path}".format(adl_path=var_adls_uri, raw_novo_caged_path=raw_novo_caged_path)


trs_caged_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"].replace('novo_',''))
adl_caged_path = "{adl_path}{trs_caged_path}".format(adl_path=var_adls_uri, trs_caged_path=trs_caged_path)


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])
adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns():
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)
 

# COMMAND ----------

sheet_name='CAGED'

df_caged = spark.read.parquet(adl_caged_path)

cf.check_ba_doc(df_caged, parse_ba=var_prm_dict, sheet='CAGED')

df_caged = df_caged.select(*__transform_columns())


# COMMAND ----------

df_caged = df_caged.withColumn('CD_GRAUDEINSTRUCAO', f.when(f.col('CD_GRAUDEINSTRUCAO')==-1, '99').otherwise(f.col('CD_GRAUDEINSTRUCAO')))


# COMMAND ----------

df_caged = df_caged.withColumn('CD_RACA_COR', f.when(f.col('CD_RACA_COR')==1, 5).\
                   otherwise(f.when(f.col('CD_RACA_COR')==2, 1).\
                   otherwise(f.when(f.col('CD_RACA_COR')==4, 2).\
                   otherwise(f.when(f.col('CD_RACA_COR')==6, 4).\
                   otherwise(f.when(f.col('CD_RACA_COR')==8, 3).\
                   otherwise(f.when(f.col('CD_RACA_COR')==-1, 6).\
                   otherwise(f.col('CD_RACA_COR'))))))))

# COMMAND ----------

df_caged = df_caged.withColumn('CD_SEXO', f.when(f.col('CD_SEXO')==2, 3).\
                   otherwise(f.when(f.col('CD_SEXO')==-1, 9).\
                   otherwise(f.col('CD_SEXO'))))

# COMMAND ----------

df_caged = df_caged.withColumn('CD_TIPO_ESTABELECIMENTO', f.when(f.col('CD_TIPO_ESTABELECIMENTO')==3, 5).\
                   otherwise(f.when(f.col('CD_TIPO_ESTABELECIMENTO')==9, f.lit(None)).\
                   otherwise(f.when(f.col('CD_TIPO_ESTABELECIMENTO')==-1, f.lit(None)).\
                   otherwise(f.col('CD_TIPO_ESTABELECIMENTO')))))

# COMMAND ----------

df_caged = df_caged.withColumn('CD_TIPO_MOVIMENTACAO', f.when(f.col('CD_TIPO_MOVIMENTACAO')==1, 10).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==2, 20).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==3, 70).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==4, 31).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==5, 32).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==6, 40).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==7, 50).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==8, 60).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==9, 80).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==10, 35).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==11, 45).\
                   otherwise(f.when(f.col('CD_TIPO_MOVIMENTACAO')==-1, 99).\
                   otherwise(f.col('CD_TIPO_MOVIMENTACAO'))))))))))))))

# COMMAND ----------

df_caged = df_caged.withColumn('CD_TIPO_DEDEFICIENCIA', f.when(f.col('CD_TIPO_DEDEFICIENCIA')==-1, 9).\
                   otherwise(f.col('CD_TIPO_DEDEFICIENCIA')))

# COMMAND ----------

df_caged = df_caged.withColumn('CD_TAM_ESTAB_JAN', f.when(f.col('CD_TAM_ESTAB_JAN')==-1, 99).\
                   otherwise(f.when(f.col('CD_TAM_ESTAB_JAN')>=1, f.col('CD_TAM_ESTAB_JAN')+1).\
                   otherwise(f.col('CD_TAM_ESTAB_JAN'))))

# COMMAND ----------

sheet_name='NOVO_CAGED'

df_novo_caged = spark.read.parquet(adl_raw_novo_caged)

cf.check_ba_doc(df_novo_caged, parse_ba=var_prm_dict, sheet='NOVO_CAGED')

df_novo_caged = df_novo_caged.select(*__transform_columns())



# COMMAND ----------

df_novo_caged = df_novo_caged.withColumn('CD_ANO_COMPETENCIA', f.substring(f.col('CD_COMPETENCIA'), 0, 4).cast(IntegerType()))

# COMMAND ----------

df = df_caged.union(df_novo_caged)

# COMMAND ----------

df = df.withColumn('CD_SUBCLASSE', f.when(f.length(f.col('CD_SUBCLASSE'))<7, f.lpad('CD_SUBCLASSE', 7, "0")).otherwise(f.col('CD_SUBCLASSE')))


# COMMAND ----------

(df
 .write
 .partitionBy('CD_ANO_COMPETENCIA', 'CD_COMPETENCIA')
 .parquet(path=adl_trs, mode='overwrite'))

# COMMAND ----------


