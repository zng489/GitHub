# Databricks notebook source
import datetime

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize

import crawler.functions as cf
import json
import pyspark.sql.functions as f
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

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)

adl_raw


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)

adl_trs


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


# COMMAND ----------

sheet_name='CPC'
columns = [name[1] for name in var_prm_dict[sheet_name]]


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

df = spark.read.parquet(adl_raw)


# COMMAND ----------

df = df.select(*__transform_columns())


# COMMAND ----------

ORG_ACADEMICA = [
        {'NO_ORG_ACADEMICA': 'CENTROS UNIVERSITÁRIOS', 'NO_ORG_ACADEMICA_TRS': 'Centro Universitário'},
        {'NO_ORG_ACADEMICA': 'FACULDADES', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Instituto Superior ou Escola Superior', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Faculdade', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Centro Universitário', 'NO_ORG_ACADEMICA_TRS': 'Centro Universitário'},
        {'NO_ORG_ACADEMICA': 'Universidade Especializada', 'NO_ORG_ACADEMICA_TRS': 'Universidade'},
        {'NO_ORG_ACADEMICA': 'UNIVERSIDADES', 'NO_ORG_ACADEMICA_TRS': 'Universidade'},
        {'NO_ORG_ACADEMICA': 'Universidade', 'NO_ORG_ACADEMICA_TRS': 'Universidade'},
        {'NO_ORG_ACADEMICA': 'Ifet/Cefet', 'NO_ORG_ACADEMICA_TRS': 'IF e Cefet'},
        {'NO_ORG_ACADEMICA': 'Faculdade de Tecnologia', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Faculdade', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Centro Federal de Educação Tecnológica', 'NO_ORG_ACADEMICA_TRS': 'IF e Cefet'},
        {'NO_ORG_ACADEMICA': 'Faculdades Integradas', 'NO_ORG_ACADEMICA_TRS': 'Faculdade'},
        {'NO_ORG_ACADEMICA': 'Instituto Federal de Educação, Ciência e Tecnologia', 'NO_ORG_ACADEMICA_TRS': 'IF e Cefet'}
]

# COMMAND ----------

df_org_academica = spark.createDataFrame(ORG_ACADEMICA)


# COMMAND ----------

CAT_ADMI = [
        {'NO_CAT_ADMI': 'Federal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Sociedade', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Associação de Utilidade Pública', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Especial', 'NO_CAT_ADMI_TRS': 'Especial'},
        {'NO_CAT_ADMI': 'PÚBLICA', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Associação de Utilidade Pública', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'PRIVADA', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Civil', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Público - Municipal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Privada', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Pública Federal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Público - Estadual', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pública', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Privada com fins lucrativos', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Municipal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Estadual', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Com fins lucrativos - Sociedade Mercantil ou Comercial', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Pública Estadual', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Fundação Pública de Direito Privado Municipal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Público - Federal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pública Municipal', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Privada sem fins lucrativos', 'NO_CAT_ADMI_TRS': 'Privada'},
        {'NO_CAT_ADMI': 'Munic_autarquia', 'NO_CAT_ADMI_TRS': 'Pública'},
        {'NO_CAT_ADMI': 'Pessoa Jurídica de Direito Privado - Sem fins lucrativos - Fundação', 'NO_CAT_ADMI_TRS': 'Privada'}
]

# COMMAND ----------

df_cat_admi = spark.createDataFrame(CAT_ADMI)


# COMMAND ----------

REG_CURSO = [
    {'NO_REG_CURSO_OLD': 'Nordeste', 'NO_REG_CURSO': 'Nordeste'},
    {'NO_REG_CURSO_OLD': 'NORDESTE', 'NO_REG_CURSO': 'Nordeste'},
    {'NO_REG_CURSO_OLD': 'Norte', 'NO_REG_CURSO': 'Norte'},
    {'NO_REG_CURSO_OLD': 'NORTE', 'NO_REG_CURSO': 'Norte'},
    {'NO_REG_CURSO_OLD': 'Sul', 'NO_REG_CURSO': 'Sul'},
    {'NO_REG_CURSO_OLD': 'SUL', 'NO_REG_CURSO': 'Sul'},
    {'NO_REG_CURSO_OLD': 'Sudeste', 'NO_REG_CURSO': 'Sudeste'},
    {'NO_REG_CURSO_OLD': 'SUDESTE', 'NO_REG_CURSO': 'Sudeste'},
    {'NO_REG_CURSO_OLD': 'CENTRO-OESTE', 'NO_REG_CURSO': 'Centro-Oeste'},
    {'NO_REG_CURSO_OLD': 'Centro-Oeste', 'NO_REG_CURSO': 'Centro-Oeste'}
]

# COMMAND ----------

df_reg_curso = spark.createDataFrame(REG_CURSO)


# COMMAND ----------

df = df.withColumn("NO_ORG_ACADEMICA", f.trim(df.NO_ORG_ACADEMICA))
df = df.withColumn("NO_CAT_ADMI", f.trim(df.NO_CAT_ADMI))
df = df.withColumn("NO_REG_CURSO", f.trim(df.NO_REG_CURSO))

df = df.join(df_org_academica, f.upper(df.NO_ORG_ACADEMICA)==f.upper(df_org_academica.NO_ORG_ACADEMICA), how='left') \
       .drop(df.NO_ORG_ACADEMICA_TRS) \
       .drop(df_org_academica.NO_ORG_ACADEMICA)

df = df.join(df_cat_admi, f.upper(df.NO_CAT_ADMI)==f.upper(df_cat_admi.NO_CAT_ADMI), how='left') \
       .drop(df.NO_CAT_ADMI_TRS) \
       .drop(df_cat_admi.NO_CAT_ADMI)

df = df.join(df_reg_curso, f.upper(df.NO_REG_CURSO)==f.upper(df_reg_curso.NO_REG_CURSO_OLD), how='left') \
       .drop(df.NO_REG_CURSO) \
       .drop(df_reg_curso.NO_REG_CURSO_OLD)


df = df.withColumn('CO_MUNICIPIO', f.when(f.length(f.col('CO_MUNICIPIO'))>7, \
        f.concat(f.substring(f.col('CO_MUNICIPIO'), 0, 2), \
        f.substring(f.col('CO_MUNICIPIO'), 8, 12))) \
        .otherwise(f.col('CO_MUNICIPIO')))

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

# display(df)


# COMMAND ----------

df.write \
  .partitionBy('NU_ANO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


