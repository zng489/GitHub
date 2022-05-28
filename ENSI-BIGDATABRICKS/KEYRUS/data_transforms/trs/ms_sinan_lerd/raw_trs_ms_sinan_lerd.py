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
import pyspark
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
prm = dls['folders']['prm']


# COMMAND ----------

prm_path = "{prm}{prm_path}".format(prm=prm, prm_path=table["prm_path"])


# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)


# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)


# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)


# COMMAND ----------

sheet_name='LERD'
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

df = df.withColumn('DS_IDADE_N', f.when((f.length(f.col('CD_IDADE_N'))==4)&(f.substring(f.col('CD_IDADE_N'), 0, 1)==4), f.substring(f.col('CD_IDADE_N'), 2, 4)).otherwise(f.lit('< 1 ano')))

# COMMAND ----------

data = [
        {'DS_SEXO_OLD': 'M', 'DS_SEXO': 'Masculino'},
        {'DS_SEXO_OLD': 'F', 'DS_SEXO': 'Feminino'},
        {'DS_SEXO_OLD': 'I', 'DS_SEXO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.DS_SEXO==df_data_aux.DS_SEXO_OLD, how='left') \
       .drop(df.DS_SEXO) \
       .drop(df_data_aux.DS_SEXO_OLD)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_GESTANT': 1, 'DS_GESTANT': '1º Trimestre'},
        {'CD_GESTANT': 2, 'DS_GESTANT': '2º Trimestre'},
        {'CD_GESTANT': 3, 'DS_GESTANT': '3º Trimestre'},
        {'CD_GESTANT': 4, 'DS_GESTANT': 'Idade gestacional ignorada'},
        {'CD_GESTANT': 5, 'DS_GESTANT': 'Não'},
        {'CD_GESTANT': 6, 'DS_GESTANT': 'Não se aplica'},
        {'CD_GESTANT': 9, 'DS_GESTANT': 'Ignorado'}  
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_GESTANT==df_data_aux.CD_GESTANT, how='left') \
       .drop(df.DS_GESTANT) \
       .drop(df_data_aux.CD_GESTANT)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_TIPO_RACA': 1, 'DS_TIPO_RACA': 'Branca'},
        {'CD_TIPO_RACA': 2, 'DS_TIPO_RACA': 'Preta'},
        {'CD_TIPO_RACA': 3, 'DS_TIPO_RACA': 'Amarela'},
        {'CD_TIPO_RACA': 4, 'DS_TIPO_RACA': 'Parda'},
        {'CD_TIPO_RACA': 5, 'DS_TIPO_RACA': 'Indígena'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TIPO_RACA==df_data_aux.CD_TIPO_RACA, how='left') \
       .drop(df.DS_TIPO_RACA) \
       .drop(df_data_aux.CD_TIPO_RACA)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_ESCOL_N': 1, 'DS_ESCOL_N': '1ª a 4ª série incompleta do EF'},
        {'CD_ESCOL_N': 2, 'DS_ESCOL_N': '4ª série completa do EF (antigo 1° grau)'},
        {'CD_ESCOL_N': 3, 'DS_ESCOL_N': '5ª à 8ª série incompleta do EF (antigo ginásio ou 1° grau)'},
        {'CD_ESCOL_N': 4, 'DS_ESCOL_N': 'Ensino fundamental completo (antigo ginásio ou 1° grau)'},
        {'CD_ESCOL_N': 5, 'DS_ESCOL_N': 'Ensino médio incompleto (antigo colegial ou 2° grau)'},
        {'CD_ESCOL_N': 6, 'DS_ESCOL_N': 'Ensino médio completo (antigo colegial ou 2° grau)'},
        {'CD_ESCOL_N': 7, 'DS_ESCOL_N': 'Educação superior incompleta'},
        {'CD_ESCOL_N': 8, 'DS_ESCOL_N': 'Educação superior completa'},
        {'CD_ESCOL_N': 9, 'DS_ESCOL_N': 'Ignorado'},
        {'CD_ESCOL_N': 10, 'DS_ESCOL_N': 'Não se aplica'},
        {'CD_ESCOL_N': 43, 'DS_ESCOL_N': 'Analfabeto'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ESCOL_N==df_data_aux.CD_ESCOL_N, how='left') \
       .drop(df.DS_ESCOL_N) \
       .drop(df_data_aux.CD_ESCOL_N)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_TRAB': "01", 'DS_TRAB': 'Empregado registrado com carteira assinada'},
        {'CD_TRAB': "02", 'DS_TRAB': 'Empregado não registrado'},
        {'CD_TRAB': "03", 'DS_TRAB': 'Autônomo/conta própria'},
        {'CD_TRAB': "04", 'DS_TRAB': 'Servidor público estatuário'},
        {'CD_TRAB': "05", 'DS_TRAB': 'Servidor público celetista'},
        {'CD_TRAB': "06", 'DS_TRAB': 'Aposentado'},
        {'CD_TRAB': "07", 'DS_TRAB': 'Desempregado'},
        {'CD_TRAB': "08", 'DS_TRAB': 'Trabalho temporário'},
        {'CD_TRAB': "09", 'DS_TRAB': 'Cooperativado'},
        {'CD_TRAB': "10", 'DS_TRAB': 'Trabalhador avulso'},
        {'CD_TRAB': "11", 'DS_TRAB': 'Empregador'},
        {'CD_TRAB': "12", 'DS_TRAB': 'Outros'},
        {'CD_TRAB': "99", 'DS_TRAB': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TRAB==df_data_aux.CD_TRAB, how='left') \
       .drop(df.DS_TRAB) \
       .drop(df_data_aux.CD_TRAB)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_TPTEMPO': 1, 'DS_TPTEMPO': 'hora'},
        {'CD_TPTEMPO': 2, 'DS_TPTEMPO': 'dia'},
        {'CD_TPTEMPO': 3, 'DS_TPTEMPO': 'mês'},
        {'CD_TPTEMPO': 4, 'DS_TPTEMPO': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TPTEMPO==df_data_aux.CD_TPTEMPO, how='left') \
       .drop(df.DS_TPTEMPO) \
       .drop(df_data_aux.CD_TPTEMPO)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_TIPO_TERCEIRIZA': 1, 'DS_TERCEIRIZA': 'Sim'},
        {'CD_TIPO_TERCEIRIZA': 2, 'DS_TERCEIRIZA': 'Não'},
        {'CD_TIPO_TERCEIRIZA': 3, 'DS_TERCEIRIZA': 'Não se aplica'},
        {'CD_TIPO_TERCEIRIZA': 4, 'DS_TERCEIRIZA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TIPO_TERCEIRIZA==df_data_aux.CD_TIPO_TERCEIRIZA, how='left') \
       .drop(df.DS_TERCEIRIZA) \
       .drop(df_data_aux.CD_TIPO_TERCEIRIZA)

del df_data_aux

# COMMAND ----------

data = [
        {'CD_HIPERTEN': 1, 'DS_HIPERTEN': 'Sim'},
        {'CD_HIPERTEN': 2, 'DS_HIPERTEN': 'Não'},
        {'CD_HIPERTEN': 3, 'DS_HIPERTEN': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_HIPERTEN==df_data_aux.CD_HIPERTEN, how='left') \
       .drop(df.DS_HIPERTEN) \
       .drop(df_data_aux.CD_HIPERTEN)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_TUBE': 1, 'DS_TUBE': 'Sim'},
        {'CD_TUBE': 2, 'DS_TUBE': 'Não'},
        {'CD_TUBE': 3, 'DS_TUBE': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TUBE==df_data_aux.CD_TUBE, how='left') \
       .drop(df.DS_TUBE) \
       .drop(df_data_aux.CD_TUBE)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_DIABETES': 1, 'DS_DIABETES': 'Sim'},
        {'CD_DIABETES': 2, 'DS_DIABETES': 'Não'},
        {'CD_DIABETES': 3, 'DS_DIABETES': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_DIABETES==df_data_aux.CD_DIABETES, how='left') \
       .drop(df.DS_DIABETES) \
       .drop(df_data_aux.CD_DIABETES)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_ASMA': 1, 'DS_ASMA': 'Sim'},
        {'CD_ASMA': 2, 'DS_ASMA': 'Não'},
        {'CD_ASMA': 3, 'DS_ASMA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ASMA==df_data_aux.CD_ASMA, how='left') \
       .drop(df.DS_ASMA) \
       .drop(df_data_aux.CD_ASMA)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_HANSENIASE': 1, 'DS_HANSENIASE': 'Sim'},
        {'CD_HANSENIASE': 2, 'DS_HANSENIASE': 'Não'},
        {'CD_HANSENIASE': 3, 'DS_HANSENIASE': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_HANSENIASE==df_data_aux.CD_HANSENIASE, how='left') \
       .drop(df.DS_HANSENIASE) \
       .drop(df_data_aux.CD_HANSENIASE)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_MENTAL': 1, 'DS_MENTAL': 'Sim'},
        {'CD_MENTAL': 2, 'DS_MENTAL': 'Não'},
        {'CD_MENTAL': 3, 'DS_MENTAL': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MENTAL==df_data_aux.CD_MENTAL, how='left') \
       .drop(df.DS_MENTAL) \
       .drop(df_data_aux.CD_MENTAL)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_OUT_AGRAVO': 1, 'DS_OUT_AGRAVO': 'Sim'},
        {'CD_OUT_AGRAVO': 2, 'DS_OUT_AGRAVO': 'Não'},
        {'CD_OUT_AGRAVO': 3, 'DS_OUT_AGRAVO': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_OUT_AGRAVO==df_data_aux.CD_OUT_AGRAVO, how='left') \
       .drop(df.DS_OUT_AGRAVO) \
       .drop(df_data_aux.CD_OUT_AGRAVO)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_TPTEMPORIS': 1, 'DS_TPTEMPORIS': 'hora'},
        {'CD_TPTEMPORIS': 2, 'DS_TPTEMPORIS': 'dia'},
        {'CD_TPTEMPORIS': 3, 'DS_TPTEMPORIS': 'mês'},
        {'CD_TPTEMPORIS': 4, 'DS_TPTEMPORIS': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TPTEMPORIS==df_data_aux.CD_TPTEMPORIS, how='left') \
       .drop(df.DS_TPTEMPORIS) \
       .drop(df_data_aux.CD_TPTEMPORIS)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_REGIME': 1, 'DS_REGIME': 'Hospitalar'},
        {'CD_REGIME': 2, 'DS_REGIME': 'Ambulatorial'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_REGIME==df_data_aux.CD_REGIME, how='left') \
       .drop(df.DS_REGIME) \
       .drop(df_data_aux.CD_REGIME)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_SENSIBILI': 1, 'DS_SENSIBILI': 'Sim'},
        {'CD_SENSIBILI': 2, 'DS_SENSIBILI': 'Não'},
        {'CD_SENSIBILI': 9, 'DS_SENSIBILI': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_SENSIBILI==df_data_aux.CD_SENSIBILI, how='left') \
       .drop(df.DS_SENSIBILI) \
       .drop(df_data_aux.CD_SENSIBILI)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_LIMITA_MOV': 1, 'DS_LIMITA_MOV': 'Sim'},
        {'CD_LIMITA_MOV': 2, 'DS_LIMITA_MOV': 'Não'},
        {'CD_LIMITA_MOV': 9, 'DS_LIMITA_MOV': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_LIMITA_MOV==df_data_aux.CD_LIMITA_MOV, how='left') \
       .drop(df.DS_LIMITA_MOV) \
       .drop(df_data_aux.CD_LIMITA_MOV)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_MUSCULAR': 1, 'DS_MUSCULAR': 'Sim'},
        {'CD_MUSCULAR': 2, 'DS_MUSCULAR': 'Não'},
        {'CD_MUSCULAR': 9, 'DS_MUSCULAR': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MUSCULAR==df_data_aux.CD_MUSCULAR, how='left') \
       .drop(df.DS_MUSCULAR) \
       .drop(df_data_aux.CD_MUSCULAR)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_FLOGISTICO': 1, 'DS_FLOGISTICO': 'Sim'},
        {'CD_FLOGISTICO': 2, 'DS_FLOGISTICO': 'Não'},
        {'CD_FLOGISTICO': 9, 'DS_FLOGISTICO': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FLOGISTICO==df_data_aux.CD_FLOGISTICO, how='left') \
       .drop(df.DS_FLOGISTICO) \
       .drop(df_data_aux.CD_FLOGISTICO)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_MENOS_MOV': 1, 'DS_MENOS_MOV': 'Sim'},
        {'CD_MENOS_MOV': 2, 'DS_MENOS_MOV': 'Não'},
        {'CD_MENOS_MOV': 9, 'DS_MENOS_MOV': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MENOS_MOV==df_data_aux.CD_MENOS_MOV, how='left') \
       .drop(df.DS_MENOS_MOV) \
       .drop(df_data_aux.CD_MENOS_MOV)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_DOR': 1, 'DS_DOR': 'Sim'},
        {'CD_DOR': 2, 'DS_DOR': 'Não'},
        {'CD_DOR': 9, 'DS_DOR': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_DOR==df_data_aux.CD_DOR, how='left') \
       .drop(df.DS_DOR) \
       .drop(df_data_aux.CD_DOR)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_OUTRO_SIN': 1, 'DS_OUTRO_SIN': 'Sim'},
        {'CD_OUTRO_SIN': 2, 'DS_OUTRO_SIN': 'Não'},
        {'CD_OUTRO_SIN': 9, 'DS_OUTRO_SIN': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_OUTRO_SIN==df_data_aux.CD_OUTRO_SIN, how='left') \
       .drop(df.DS_OUTRO_SIN) \
       .drop(df_data_aux.CD_OUTRO_SIN)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_TAREFAS': 1, 'DS_TAREFAS': 'Sim'},
        {'CD_TAREFAS': 2, 'DS_TAREFAS': 'Não'},
        {'CD_TAREFAS': 9, 'DS_TAREFAS': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TAREFAS==df_data_aux.CD_TAREFAS, how='left') \
       .drop(df.DS_TAREFAS) \
       .drop(df_data_aux.CD_TAREFAS)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_PREMIOS': 1, 'DS_PREMIOS': 'Sim'},
        {'CD_PREMIOS': 2, 'DS_PREMIOS': 'Não'},
        {'CD_PREMIOS': 9, 'DS_PREMIOS': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_PREMIOS==df_data_aux.CD_PREMIOS, how='left') \
       .drop(df.DS_PREMIOS) \
       .drop(df_data_aux.CD_PREMIOS)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_HA_PAUSA': 1, 'DS_HA_PAUSA': 'Sim'},
        {'CD_HA_PAUSA': 2, 'DS_HA_PAUSA': 'Não'},
        {'CD_HA_PAUSA': 9, 'DS_HA_PAUSA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_HA_PAUSA==df_data_aux.CD_HA_PAUSA, how='left') \
       .drop(df.DS_HA_PAUSA) \
       .drop(df_data_aux.CD_HA_PAUSA)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_REPETITIVO': 1, 'DS_REPETITIVO': 'Sim'},
        {'CD_REPETITIVO': 2, 'DS_REPETITIVO': 'Não'},
        {'CD_REPETITIVO': 9, 'DS_REPETITIVO': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_REPETITIVO==df_data_aux.CD_REPETITIVO, how='left') \
       .drop(df.DS_REPETITIVO) \
       .drop(df_data_aux.CD_REPETITIVO)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_MAIS_6HS': 1, 'DS_MAIS_6HS': 'Sim'},
        {'CD_MAIS_6HS': 2, 'DS_MAIS_6HS': 'Não'},
        {'CD_MAIS_6HS': 9, 'DS_MAIS_6HS': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MAIS_6HS==df_data_aux.CD_MAIS_6HS, how='left') \
       .drop(df.DS_MAIS_6HS) \
       .drop(df_data_aux.CD_MAIS_6HS)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_STRESS': 1, 'DS_STRESS': 'Sim'},
        {'CD_STRESS': 2, 'DS_STRESS': 'Não'},
        {'CD_STRESS': 9, 'DS_STRESS': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_STRESS==df_data_aux.CD_STRESS, how='left') \
       .drop(df.DS_STRESS) \
       .drop(df_data_aux.CD_STRESS)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_AFASTAMENT': 1, 'DS_AFASTAMENT': 'Sim'},
        {'CD_AFASTAMENT': 2, 'DS_AFASTAMENT': 'Não'},
        {'CD_AFASTAMENT': 9, 'DS_AFASTAMENT': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AFASTAMENT==df_data_aux.CD_AFASTAMENT, how='left') \
       .drop(df.DS_AFASTAMENT) \
       .drop(df_data_aux.CD_AFASTAMENT)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_TP_AFAST': 1, 'DS_TP_AFAST': 'hora'},
        {'CD_TP_AFAST': 2, 'DS_TP_AFAST': 'dia'},
        {'CD_TP_AFAST': 3, 'DS_TP_AFAST': 'mês'},
        {'CD_TP_AFAST': 4, 'DS_TP_AFAST': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TP_AFAST==df_data_aux.CD_TP_AFAST, how='left') \
       .drop(df.DS_TP_AFAST) \
       .drop(df_data_aux.CD_TP_AFAST)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_EVOL_AFAST': 1, 'DS_EVOL_AFAST': 'Sim'},
        {'CD_EVOL_AFAST': 2, 'DS_EVOL_AFAST': 'Não'},
        {'CD_EVOL_AFAST': 9, 'DS_EVOL_AFAST': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EVOL_AFAST==df_data_aux.CD_EVOL_AFAST, how='left') \
       .drop(df.DS_EVOL_AFAST) \
       .drop(df_data_aux.CD_EVOL_AFAST)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_TRAB_DOE': 1, 'DS_TRAB_DOE': 'Sim'},
        {'CD_TRAB_DOE': 2, 'DS_TRAB_DOE': 'Não'},
        {'CD_TRAB_DOE': 9, 'DS_TRAB_DOE': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux= spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TRAB_DOE==df_data_aux.CD_TRAB_DOE, how='left') \
       .drop(df.DS_TRAB_DOE) \
       .drop(df_data_aux.CD_TRAB_DOE)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_AFAST_RISC': 1, 'DS_AFAST_RISC': 'Sim'},
        {'CD_AFAST_RISC': 2, 'DS_AFAST_RISC': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux= spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AFAST_RISC==df_data_aux.CD_AFAST_RISC, how='left') \
       .drop(df.DS_AFAST_RISC) \
       .drop(df_data_aux.CD_AFAST_RISC)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_INDIVIDUAL': 1, 'DS_INDIVIDUAL': 'Sim'},
        {'CD_INDIVIDUAL': 2, 'DS_INDIVIDUAL': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_INDIVIDUAL==df_data_aux.CD_INDIVIDUAL, how='left') \
       .drop(df.DS_INDIVIDUAL) \
       .drop(df_data_aux.CD_INDIVIDUAL)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_MUDA_TRAB': 1, 'DS_MUDA_TRAB': 'Sim'},
        {'CD_MUDA_TRAB': 2, 'DS_MUDA_TRAB': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MUDA_TRAB==df_data_aux.CD_MUDA_TRAB, how='left') \
       .drop(df.DS_MUDA_TRAB) \
       .drop(df_data_aux.CD_MUDA_TRAB)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_NENHUM': 1, 'DS_NENHUM': 'Sim'},
        {'CD_NENHUM': 2, 'DS_NENHUM': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_NENHUM==df_data_aux.CD_NENHUM, how='left') \
       .drop(df.DS_NENHUM) \
       .drop(df_data_aux.CD_NENHUM)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_COLETIVA': 1, 'DS_COLETIVA': 'Sim'},
        {'CD_COLETIVA': 2, 'DS_COLETIVA': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_COLETIVA==df_data_aux.CD_COLETIVA, how='left') \
       .drop(df.DS_COLETIVA) \
       .drop(df_data_aux.CD_COLETIVA)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_AFAST_TRAB': 1, 'DS_AFAST_TRAB': 'Sim'},
        {'CD_AFAST_TRAB': 2, 'DS_AFAST_TRAB': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AFAST_TRAB==df_data_aux.CD_AFAST_TRAB, how='left') \
       .drop(df.DS_AFAST_TRAB) \
       .drop(df_data_aux.CD_AFAST_TRAB)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_EVOLUCAO': 1, 'DS_EVOLUCAO': 'Cura'},
        {'CD_EVOLUCAO': 2, 'DS_EVOLUCAO': 'Cura não confirmada'},
        {'CD_EVOLUCAO': 3, 'DS_EVOLUCAO': 'Incapacidade'},
        {'CD_EVOLUCAO': 4, 'DS_EVOLUCAO': 'Incapacidade permanente parcial'},
        {'CD_EVOLUCAO': 5, 'DS_EVOLUCAO': 'Incapacidade permanente total'},
        {'CD_EVOLUCAO': 6, 'DS_EVOLUCAO': 'Óbito por doença relacionada ao trabalho'},
        {'CD_EVOLUCAO': 7, 'DS_EVOLUCAO': 'Óbito por outra causa'},
        {'CD_EVOLUCAO': 8, 'DS_EVOLUCAO': 'Outro'},
        {'CD_EVOLUCAO': 9, 'DS_EVOLUCAO': 'Ignorado'},

]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EVOLUCAO==df_data_aux.CD_EVOLUCAO, how='left') \
       .drop(df.DS_EVOLUCAO) \
       .drop(df_data_aux.CD_EVOLUCAO)

del df_data_aux


# COMMAND ----------

data = [
        {'CD_CAT': 1, 'DS_CAT': "Sim"},
        {'CD_CAT': 2, 'DS_CAT': "Não"},
        {'CD_CAT': 3, 'DS_CAT': "Não se aplica"},
        {'CD_CAT': 9, 'DS_CAT': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_CAT==df_data_aux.CD_CAT, how='left') \
       .drop(df.DS_CAT) \
       .drop(df_data_aux.CD_CAT)

del df_data_aux


# COMMAND ----------

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


