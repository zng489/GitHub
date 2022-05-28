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


# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}".format(raw=raw, schema=table["schema"], table=table["table"])

adl_raw = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)



# COMMAND ----------

trs_path = "{trs}/{schema}/{table}".format(trs=trs, schema=table["schema"], table=table["table"])

adl_trs = "{adl_path}{trs_path}".format(adl_path=var_adls_uri, trs_path=trs_path)



# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)


# COMMAND ----------

sheet_name='CANC'
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
        {'CD_SEXO': 'M', 'DS_SEXO': 'Masculino'},
        {'CD_SEXO': 'F', 'DS_SEXO': 'Feminino'},
        {'CD_SEXO': 'I', 'DS_SEXO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_ds_sexo = spark.createDataFrame(row_data)

df = df.join(df_ds_sexo, df.CD_SEXO==df_ds_sexo.CD_SEXO, how='left') \
       .drop(df.DS_SEXO) \
       .drop(df_ds_sexo.CD_SEXO)

# COMMAND ----------

data = [
        {'CD_TIPO_RACA': 1, 'DS_TIPO_RACA': 'Branca'},
        {'CD_TIPO_RACA': 2, 'DS_TIPO_RACA': 'Preta'},
        {'CD_TIPO_RACA': 3, 'DS_TIPO_RACA': 'Amarela'},
        {'CD_TIPO_RACA': 4, 'DS_TIPO_RACA': 'Parda'},
        {'CD_TIPO_RACA': 5, 'DS_TIPO_RACA': 'Indígena'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tipo_raca = spark.createDataFrame(row_data)

df = df.join(df_tipo_raca, df.CD_TIPO_RACA==df_tipo_raca.CD_TIPO_RACA, how='left') \
       .drop(df.DS_TIPO_RACA) \
       .drop(df_tipo_raca.CD_TIPO_RACA)

# COMMAND ----------

data = [
        {'CD_ESCOL_N': 43, 'DS_ESCOL_N': 'Analfabeto'},
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
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_escolaridade = spark.createDataFrame(row_data)

df = df.join(df_escolaridade, df.CD_ESCOL_N==df_escolaridade.CD_ESCOL_N, how='left') \
       .drop(df.DS_ESCOL_N) \
       .drop(df_escolaridade.CD_ESCOL_N)

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
df_sit_trab = spark.createDataFrame(row_data)

df = df.join(df_sit_trab, df.CD_TRAB==df_sit_trab.CD_TRAB, how='left') \
       .drop(df.DS_TRAB) \
       .drop(df_sit_trab.CD_TRAB)

# COMMAND ----------

data = [
        {'CD_TPTEMPO': 1, 'DS_TPTEMPO': 'hora'},
        {'CD_TPTEMPO': 2, 'DS_TPTEMPO': 'dia'},
        {'CD_TPTEMPO': 3, 'DS_TPTEMPO': 'mês'},
        {'CD_TPTEMPO': 4, 'DS_TPTEMPO': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tempo_trab = spark.createDataFrame(row_data)

df = df.join(df_tempo_trab, df.CD_TPTEMPO==df_tempo_trab.CD_TPTEMPO, how='left') \
       .drop(df.DS_TPTEMPO) \
       .drop(df_tempo_trab.CD_TPTEMPO)

# COMMAND ----------

data = [
        {'CD_TIPO_TERCEIRIZA': 1, 'DS_TERCEIRIZA': 'Sim'},
        {'CD_TIPO_TERCEIRIZA': 2, 'DS_TERCEIRIZA': 'Não'},
        {'CD_TIPO_TERCEIRIZA': 3, 'DS_TERCEIRIZA': 'Não se aplica'},
        {'CD_TIPO_TERCEIRIZA': 4, 'DS_TERCEIRIZA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_terceiriza = spark.createDataFrame(row_data)

df = df.join(df_terceiriza, df.CD_TIPO_TERCEIRIZA==df_terceiriza.CD_TIPO_TERCEIRIZA, how='left') \
       .drop(df.DS_TERCEIRIZA) \
       .drop(df_terceiriza.CD_TIPO_TERCEIRIZA)

# COMMAND ----------

data = [
        {'CD_TPTEMPORIS': 1, 'DS_TPTEMPORIS': 'hora'},
        {'CD_TPTEMPORIS': 2, 'DS_TPTEMPORIS': 'dia'},
        {'CD_TPTEMPORIS': 3, 'DS_TPTEMPORIS': 'mês'},
        {'CD_TPTEMPORIS': 4, 'DS_TPTEMPORIS': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_temporis = spark.createDataFrame(row_data)

df = df.join(df_temporis, df.CD_TPTEMPORIS==df_temporis.CD_TPTEMPORIS, how='left') \
       .drop(df.DS_TPTEMPORIS) \
       .drop(df_temporis.CD_TPTEMPORIS)

# COMMAND ----------

data = [
        {'CD_TIPO_REGIME': 1, 'DS_TIPO_REGIME': 'Hospitalar'},
        {'CD_TIPO_REGIME': 2, 'DS_TIPO_REGIME': 'Ambulatorial'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tipo_regime = spark.createDataFrame(row_data)

df = df.join(df_tipo_regime, df.CD_TIPO_REGIME==df_tipo_regime.CD_TIPO_REGIME, how='left') \
       .drop(df.DS_TIPO_REGIME) \
       .drop(df_tipo_regime.CD_TIPO_REGIME)

# COMMAND ----------

data = [
        {'CD_ASBESTO': 1, 'DS_ASBESTO': 'Sim'},
        {'CD_ASBESTO': 2, 'DS_ASBESTO': 'Não'},
        {'CD_ASBESTO': 9, 'DS_ASBESTO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_asbesto = spark.createDataFrame(row_data)

df = df.join(df_asbesto, df.CD_ASBESTO==df_asbesto.CD_ASBESTO, how='left') \
       .drop(df.DS_ASBESTO) \
       .drop(df_asbesto.CD_ASBESTO)



# COMMAND ----------

data = [
        {'CD_SILICA': 1, 'DS_SILICA': 'Sim'},
        {'CD_SILICA': 2, 'DS_SILICA': 'Não'},
        {'CD_SILICA': 9, 'DS_SILICA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_silica = spark.createDataFrame(row_data)

df = df.join(df_silica, df.CD_SILICA==df_silica.CD_SILICA, how='left') \
       .drop(df.DS_SILICA) \
       .drop(df_silica.CD_SILICA)



# COMMAND ----------

data = [
        {'CD_AMINA': 1, 'DS_AMINA': 'Sim'},
        {'CD_AMINA': 2, 'DS_AMINA': 'Não'},
        {'CD_AMINA': 9, 'DS_AMINA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_amina = spark.createDataFrame(row_data)

df = df.join(df_amina, df.CD_AMINA==df_amina.CD_AMINA, how='left') \
       .drop(df.DS_AMINA) \
       .drop(df_amina.CD_AMINA)



# COMMAND ----------

data = [
        {'CD_BENZENO': 1, 'DS_BENZENO': 'Sim'},
        {'CD_BENZENO': 2, 'DS_BENZENO': 'Não'},
        {'CD_BENZENO': 9, 'DS_BENZENO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_benzeno = spark.createDataFrame(row_data)

df = df.join(df_benzeno, df.CD_BENZENO==df_benzeno.CD_BENZENO, how='left') \
       .drop(df.DS_BENZENO) \
       .drop(df_benzeno.CD_BENZENO)



# COMMAND ----------

data = [
        {'CD_ALCATRAO': 1, 'DS_ALCATRAO': 'Sim'},
        {'CD_ALCATRAO': 2, 'DS_ALCATRAO': 'Não'},
        {'CD_ALCATRAO': 9, 'DS_ALCATRAO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_alcatrao = spark.createDataFrame(row_data)

df = df.join(df_alcatrao, df.CD_ALCATRAO==df_alcatrao.CD_ALCATRAO, how='left') \
       .drop(df.DS_ALCATRAO) \
       .drop(df_alcatrao.CD_ALCATRAO)



# COMMAND ----------

data = [
        {'CD_HIDROCARBO': 1, 'DS_HIDROCARBO': 'Sim'},
        {'CD_HIDROCARBO': 2, 'DS_HIDROCARBO': 'Não'},
        {'CD_HIDROCARBO': 9, 'DS_HIDROCARBO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_hidrocarboneto = spark.createDataFrame(row_data)

df = df.join(df_hidrocarboneto, df.CD_HIDROCARBO==df_hidrocarboneto.CD_HIDROCARBO, how='left') \
       .drop(df.DS_HIDROCARBO) \
       .drop(df_hidrocarboneto.CD_HIDROCARBO)



# COMMAND ----------

data = [
        {'CD_OLEOS': 1, 'DS_OLEOS': 'Sim'},
        {'CD_OLEOS': 2, 'DS_OLEOS': 'Não'},
        {'CD_OLEOS': 9, 'DS_OLEOS': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_oleos = spark.createDataFrame(row_data)

df = df.join(df_oleos, df.CD_OLEOS==df_oleos.CD_OLEOS, how='left') \
       .drop(df.DS_OLEOS) \
       .drop(df_oleos.CD_OLEOS)



# COMMAND ----------

data = [
        {'CD_BERILIO': 1, 'DS_BERILIO': 'Sim'},
        {'CD_BERILIO': 2, 'DS_BERILIO': 'Não'},
        {'CD_BERILIO': 9, 'DS_BERILIO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_berilio = spark.createDataFrame(row_data)

df = df.join(df_berilio, df.CD_BERILIO==df_berilio.CD_BERILIO, how='left') \
       .drop(df.DS_BERILIO) \
       .drop(df_berilio.CD_BERILIO)



# COMMAND ----------

data = [
        {'CD_CADMIO': 1, 'DS_CADMIO': 'Sim'},
        {'CD_CADMIO': 2, 'DS_CADMIO': 'Não'},
        {'CD_CADMIO': 9, 'DS_CADMIO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_cadmio = spark.createDataFrame(row_data)

df = df.join(df_cadmio, df.CD_CADMIO==df_cadmio.CD_CADMIO, how='left') \
       .drop(df.DS_CADMIO) \
       .drop(df_cadmio.CD_CADMIO)



# COMMAND ----------

data = [
        {'CD_CROMO': 1, 'DS_CROMO': 'Sim'},
        {'CD_CROMO': 2, 'DS_CROMO': 'Não'},
        {'CD_CROMO': 9, 'DS_CROMO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_cromo = spark.createDataFrame(row_data)

df = df.join(df_cromo, df.CD_CROMO==df_cromo.CD_CROMO, how='left') \
       .drop(df.DS_CROMO) \
       .drop(df_cromo.CD_CROMO)



# COMMAND ----------

data = [
        {'CD_NIQUEL': 1, 'DS_NIQUEL': 'Sim'},
        {'CD_NIQUEL': 2, 'DS_NIQUEL': 'Não'},
        {'CD_NIQUEL': 9, 'DS_NIQUEL': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_niquel = spark.createDataFrame(row_data)

df = df.join(df_niquel, df.CD_NIQUEL==df_niquel.CD_NIQUEL, how='left') \
       .drop(df.DS_NIQUEL) \
       .drop(df_niquel.CD_NIQUEL)



# COMMAND ----------

data = [
        {'CD_IONIZANTES': 1, 'DS_IONIZANTES': 'Sim'},
        {'CD_IONIZANTES': 2, 'DS_IONIZANTES': 'Não'},
        {'CD_IONIZANTES': 9, 'DS_IONIZANTES': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_ionizante = spark.createDataFrame(row_data)

df = df.join(df_ionizante, df.CD_IONIZANTES==df_ionizante.CD_IONIZANTES, how='left') \
       .drop(df.DS_IONIZANTES) \
       .drop(df_ionizante.CD_IONIZANTES)



# COMMAND ----------

data = [
        {'CD_NAO_IONIZA': 1, 'DS_NAO_IONIZA': 'Sim'},
        {'CD_NAO_IONIZA': 2, 'DS_NAO_IONIZA': 'Não'},
        {'CD_NAO_IONIZA': 9, 'DS_NAO_IONIZA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_nao_ionizante = spark.createDataFrame(row_data)

df = df.join(df_nao_ionizante, df.CD_NAO_IONIZA==df_nao_ionizante.CD_NAO_IONIZA, how='left') \
       .drop(df.DS_NAO_IONIZA) \
       .drop(df_nao_ionizante.CD_NAO_IONIZA)



# COMMAND ----------

data = [
        {'CD_HORMONIO': 1, 'DS_HORMONIO': 'Sim'},
        {'CD_HORMONIO': 2, 'DS_HORMONIO': 'Não'},
        {'CD_HORMONIO': 9, 'DS_HORMONIO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_nao_ionizante = spark.createDataFrame(row_data)

df = df.join(df_nao_ionizante, df.CD_HORMONIO==df_nao_ionizante.CD_HORMONIO, how='left') \
       .drop(df.DS_HORMONIO) \
       .drop(df_nao_ionizante.CD_HORMONIO)



# COMMAND ----------

data = [
        {'CD_OUTRO_EXP': 1, 'DS_OUTRO_EXP': 'Sim'},
        {'CD_OUTRO_EXP': 2, 'DS_OUTRO_EXP': 'Não'},
        {'CD_OUTRO_EXP': 9, 'DS_OUTRO_EXP': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_outro_exp = spark.createDataFrame(row_data)

df = df.join(df_outro_exp, df.CD_OUTRO_EXP==df_outro_exp.CD_OUTRO_EXP, how='left') \
       .drop(df.DS_OUTRO_EXP) \
       .drop(df_outro_exp.CD_OUTRO_EXP)



# COMMAND ----------

data = [
        {'CD_FUMA': 1, 'DS_FUMA': 'Sim'},
        {'CD_FUMA': 2, 'DS_FUMA': 'Não'},
        {'CD_FUMA': 3, 'DS_FUMA': 'Ex-Fumante'},
        {'CD_FUMA': 9, 'DS_FUMA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_fuma = spark.createDataFrame(row_data)

df = df.join(df_fuma, df.CD_FUMA==df_fuma.CD_FUMA, how='left') \
       .drop(df.DS_FUMA) \
       .drop(df_fuma.CD_FUMA)



# COMMAND ----------

data = [
        {'CD_TEMP_FU': 1, 'DS_TEMP_FU': 'hora'},
        {'CD_TEMP_FU': 2, 'DS_TEMP_FU': 'dia'},
        {'CD_TEMP_FU': 3, 'DS_TEMP_FU': 'mês'},
        {'CD_TEMP_FU': 4, 'DS_TEMP_FU': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_fuma = spark.createDataFrame(row_data)

df = df.join(df_fuma, df.CD_TEMP_FU==df_fuma.CD_TEMP_FU, how='left') \
       .drop(df.DS_TEMP_FU) \
       .drop(df_fuma.CD_TEMP_FU)



# COMMAND ----------

data = [
        {'CD_TRAB_DOE': 1, 'DS_TRAB_DOE': 'Sim'},
        {'CD_TRAB_DOE': 2, 'DS_TRAB_DOE': 'Não'},
        {'CD_TRAB_DOE': 9, 'DS_TRAB_DOE': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_trab_doe = spark.createDataFrame(row_data)

df = df.join(df_trab_doe, df.CD_TRAB_DOE==df_trab_doe.CD_TRAB_DOE, how='left') \
       .drop(df.DS_TRAB_DOE) \
       .drop(df_trab_doe.CD_TRAB_DOE)



# COMMAND ----------

data = [
        {'CD_TIPO_EVOLUCAO': 1, 'DS_EVOLUCAO': 'Sem evidência da doença (remissão completa)'},
        {'CD_TIPO_EVOLUCAO': 2, 'DS_EVOLUCAO': 'Remissão parcial'},
        {'CD_TIPO_EVOLUCAO': 3, 'DS_EVOLUCAO': 'Doença estável'},
        {'CD_TIPO_EVOLUCAO': 4, 'DS_EVOLUCAO': 'Doença em Progressão'},
        {'CD_TIPO_EVOLUCAO': 5, 'DS_EVOLUCAO': 'Fora de possibilidade terapêutica'},
        {'CD_TIPO_EVOLUCAO': 6, 'DS_EVOLUCAO': 'Óbito por câncer relacionado ao trabalho'},
        {'CD_TIPO_EVOLUCAO': 7, 'DS_EVOLUCAO': 'Óbito por outras causas'},
        {'CD_TIPO_EVOLUCAO': 8, 'DS_EVOLUCAO': 'Não se aplica'},
        {'CD_TIPO_EVOLUCAO': 9, 'DS_EVOLUCAO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tipo_evolucao = spark.createDataFrame(row_data)

df = df.join(df_tipo_evolucao, df.CD_TIPO_EVOLUCAO==df_tipo_evolucao.CD_TIPO_EVOLUCAO, how='left') \
       .drop(df.DS_EVOLUCAO) \
       .drop(df_tipo_evolucao.CD_TIPO_EVOLUCAO)



# COMMAND ----------

data = [
        {'CD_CAT': 1, 'DS_CAT': 'Sim'},
        {'CD_CAT': 2, 'DS_CAT': 'Não'},
        {'CD_CAT': 3, 'DS_CAT': 'Não se aplica'},
        {'CD_CAT': 9, 'DS_CAT': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_cat = spark.createDataFrame(row_data)

df = df.join(df_cat, df.CD_CAT==df_cat.CD_CAT, how='left') \
       .drop(df.DS_CAT) \
       .drop(df_cat.CD_CAT)

# COMMAND ----------

df = df.withColumn('CD_DIAG_ESP', f.translate(f.col('CD_DIAG_ESP'), '.' ,''))

# COMMAND ----------

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))



# COMMAND ----------

df.write \
  .partitionBy('NR_ANO') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


