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

sheet_name='ACGR'
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

data = [
        {'CD_TIPO_NOT': 1, 'DS_TIPO_NOT': 'Negativa'},
        {'CD_TIPO_NOT': 2, 'DS_TIPO_NOT': 'Individual'},
        {'CD_TIPO_NOT': 3, 'DS_TIPO_NOT': 'Surto'},
        {'CD_TIPO_NOT': 4, 'DS_TIPO_NOT': 'Agregado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tipo_not = spark.createDataFrame(row_data)

df = df.join(df_tipo_not, df.CD_TIPO_NOT==df_tipo_not.CD_TIPO_NOT, how='left') \
       .drop(df.DS_TIPO_NOT) \
       .drop(df_tipo_not.CD_TIPO_NOT)


# COMMAND ----------

df = df.withColumn('DS_IDADE_N', f.when((f.length(f.col('CD_IDADE_N'))==4)&(f.substring(f.col('CD_IDADE_N'), 0, 1)==4), f.substring(f.col('CD_IDADE_N'), 2, 4)).otherwise(f.lit('< 1 ano')))

# COMMAND ----------

data = [
        {'DS_SEXO_OLD': 'M', 'DS_SEXO': 'Masculino'},
        {'DS_SEXO_OLD': 'F', 'DS_SEXO': 'Feminino'},
        {'DS_SEXO_OLD': 'I', 'DS_SEXO': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_ds_sexo = spark.createDataFrame(row_data)

df = df.join(df_ds_sexo, df.DS_SEXO==df_ds_sexo.DS_SEXO_OLD, how='left') \
       .drop(df.DS_SEXO) \
       .drop(df_ds_sexo.DS_SEXO_OLD)

# COMMAND ----------

data = [
        {'CD_IDADE_GESTA': 1, 'DS_IDADE_GESTA': '1º Trimestre'},
        {'CD_IDADE_GESTA': 2, 'DS_IDADE_GESTA': '2º Trimestre'},
        {'CD_IDADE_GESTA': 3, 'DS_IDADE_GESTA': '3º Trimestre'},
        {'CD_IDADE_GESTA': 4, 'DS_IDADE_GESTA': 'Idade gestacional ignorada'},
        {'CD_IDADE_GESTA': 5, 'DS_IDADE_GESTA': 'Não'},
        {'CD_IDADE_GESTA': 6, 'DS_IDADE_GESTA': 'Não se aplica'},
        {'CD_IDADE_GESTA': 9, 'DS_IDADE_GESTA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_idade_gesta = spark.createDataFrame(row_data)

df = df.join(df_idade_gesta, df.CD_IDADE_GESTA==df_idade_gesta.CD_IDADE_GESTA, how='left') \
       .drop(df.DS_IDADE_GESTA) \
       .drop(df_idade_gesta.CD_IDADE_GESTA)

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
        {'CD_TIPO_ESCOLARIDADE': 1, 'DS_ESCOLARIDADE': '1ª a 4ª série incompleta do EF'},
        {'CD_TIPO_ESCOLARIDADE': 2, 'DS_ESCOLARIDADE': '4ª série completa do EF (antigo 1° grau)'},
        {'CD_TIPO_ESCOLARIDADE': 3, 'DS_ESCOLARIDADE': '5ª à 8ª série incompleta do EF (antigo ginásio ou 1° grau)'},
        {'CD_TIPO_ESCOLARIDADE': 4, 'DS_ESCOLARIDADE': 'Ensino fundamental completo (antigo ginásio ou 1° grau)'},
        {'CD_TIPO_ESCOLARIDADE': 5, 'DS_ESCOLARIDADE': 'Ensino médio incompleto (antigo colegial ou 2° grau)'},
        {'CD_TIPO_ESCOLARIDADE': 6, 'DS_ESCOLARIDADE': 'Ensino médio completo (antigo colegial ou 2° grau)'},
        {'CD_TIPO_ESCOLARIDADE': 7, 'DS_ESCOLARIDADE': 'Educação superior incompleta'},
        {'CD_TIPO_ESCOLARIDADE': 8, 'DS_ESCOLARIDADE': 'Educação superior completa'},
        {'CD_TIPO_ESCOLARIDADE': 9, 'DS_ESCOLARIDADE': 'Ignorado'},
        {'CD_TIPO_ESCOLARIDADE': 10, 'DS_ESCOLARIDADE': 'Não se aplica'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_escolaridade = spark.createDataFrame(row_data)

df = df.join(df_escolaridade, df.CD_TIPO_ESCOLARIDADE==df_escolaridade.CD_TIPO_ESCOLARIDADE, how='left') \
       .drop(df.DS_ESCOLARIDADE) \
       .drop(df_escolaridade.CD_TIPO_ESCOLARIDADE)

# COMMAND ----------

data = [
        {'CD_SIT_TRAB': "01", 'DS_SIT_TRAB': 'Empregado registrado com carteira assinada'},
        {'CD_SIT_TRAB': "02", 'DS_SIT_TRAB': 'Empregado não registrado'},
        {'CD_SIT_TRAB': "03", 'DS_SIT_TRAB': 'Autônomo/conta própria'},
        {'CD_SIT_TRAB': "04", 'DS_SIT_TRAB': 'Servidor público estatuário'},
        {'CD_SIT_TRAB': "05", 'DS_SIT_TRAB': 'Servidor público celetista'},
        {'CD_SIT_TRAB': "06", 'DS_SIT_TRAB': 'Aposentado'},
        {'CD_SIT_TRAB': "07", 'DS_SIT_TRAB': 'Desempregado'},
        {'CD_SIT_TRAB': "08", 'DS_SIT_TRAB': 'Trabalho temporário'},
        {'CD_SIT_TRAB': "09", 'DS_SIT_TRAB': 'Cooperativado'},
        {'CD_SIT_TRAB': "10", 'DS_SIT_TRAB': 'Trabalhador avulso'},
        {'CD_SIT_TRAB': "11", 'DS_SIT_TRAB': 'Empregador'},
        {'CD_SIT_TRAB': "12", 'DS_SIT_TRAB': 'Outros'},
        {'CD_SIT_TRAB': "99", 'DS_SIT_TRAB': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_sit_trab = spark.createDataFrame(row_data)

df = df.join(df_sit_trab, df.CD_SIT_TRAB==df_sit_trab.CD_SIT_TRAB, how='left') \
       .drop(df.DS_SIT_TRAB) \
       .drop(df_sit_trab.CD_SIT_TRAB)

# COMMAND ----------

data = [
        {'CD_TIPO_TEMPO_TRAB': 1, 'DS_TIPO_TEMPO_TRAB': 'hora'},
        {'CD_TIPO_TEMPO_TRAB': 2, 'DS_TIPO_TEMPO_TRAB': 'dia'},
        {'CD_TIPO_TEMPO_TRAB': 3, 'DS_TIPO_TEMPO_TRAB': 'mês'},
        {'CD_TIPO_TEMPO_TRAB': 4, 'DS_TIPO_TEMPO_TRAB': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tempo_trab = spark.createDataFrame(row_data)

df = df.join(df_tempo_trab, df.CD_TIPO_TEMPO_TRAB==df_tempo_trab.CD_TIPO_TEMPO_TRAB, how='left') \
       .drop(df.DS_TIPO_TEMPO_TRAB) \
       .drop(df_tempo_trab.CD_TIPO_TEMPO_TRAB)

# COMMAND ----------

data = [
        {'CD_LOCAL_ACID': 1, 'DS_LOCAL_ACID': 'Instalações do Contratante'},
        {'CD_LOCAL_ACID': 2, 'DS_LOCAL_ACID': 'Via Pública'},
        {'CD_LOCAL_ACID': 3, 'DS_LOCAL_ACID': 'Instalações de terceiros'},
        {'CD_LOCAL_ACID': 4, 'DS_LOCAL_ACID': 'Domicílio próprio'},
        {'CD_LOCAL_ACID': 5, 'DS_LOCAL_ACID': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_local_acid = spark.createDataFrame(row_data)

df = df.join(df_local_acid, df.CD_LOCAL_ACID==df_local_acid.CD_LOCAL_ACID, how='left') \
       .drop(df.DS_LOCAL_ACID) \
       .drop(df_local_acid.CD_LOCAL_ACID)

# COMMAND ----------

data = [
        {'CD_TERCEIRIZA': 1, 'DS_TERCEIRIZA': 'Sim'},
        {'CD_TERCEIRIZA': 2, 'DS_TERCEIRIZA': 'Não'},
        {'CD_TERCEIRIZA': 3, 'DS_TERCEIRIZA': 'Não se aplica'},
        {'CD_TERCEIRIZA': 4, 'DS_TERCEIRIZA': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_terceiriza = spark.createDataFrame(row_data)

df = df.join(df_terceiriza, df.CD_TERCEIRIZA==df_terceiriza.CD_TERCEIRIZA, how='left') \
       .drop(df.DS_TERCEIRIZA) \
       .drop(df_terceiriza.CD_TERCEIRIZA)

# COMMAND ----------

data = [
        {'CD_TIPO_ACID': 1, 'DS_TIPO_ACID': 'Típico'},
        {'CD_TIPO_ACID': 2, 'DS_TIPO_ACID': 'Trajeto'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tipo_acid = spark.createDataFrame(row_data)

df = df.join(df_tipo_acid, df.CD_TIPO_ACID==df_tipo_acid.CD_TIPO_ACID, how='left') \
       .drop(df.DS_TIPO_ACID) \
       .drop(df_tipo_acid.CD_TIPO_ACID)

# COMMAND ----------

data = [
        {'CD_MAIS_TRAB': 1, 'DS_MAIS_TRAB': 'Sim'},
        {'CD_MAIS_TRAB': 2, 'DS_MAIS_TRAB': 'Não'},
        {'CD_MAIS_TRAB': 9, 'DS_MAIS_TRAB': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_mais_trab = spark.createDataFrame(row_data)

df = df.join(df_mais_trab, df.CD_MAIS_TRAB==df_mais_trab.CD_MAIS_TRAB, how='left') \
       .drop(df.DS_MAIS_TRAB) \
       .drop(df_mais_trab.CD_MAIS_TRAB)

# COMMAND ----------

data = [
        {'CD_PART_CORP1': 1, 'DS_PART_CORP1': 'Olho'},
        {'CD_PART_CORP1': 2, 'DS_PART_CORP1': 'Cabeça'},
        {'CD_PART_CORP1': 3, 'DS_PART_CORP1': 'Pescoço'},
        {'CD_PART_CORP1': 4, 'DS_PART_CORP1': 'Tórax'},
        {'CD_PART_CORP1': 5, 'DS_PART_CORP1': 'Abdome'},
        {'CD_PART_CORP1': 6, 'DS_PART_CORP1': 'Mão'},
        {'CD_PART_CORP1': 7, 'DS_PART_CORP1': 'Membro superior'},
        {'CD_PART_CORP1': 8, 'DS_PART_CORP1': 'Membro Inferior'},
        {'CD_PART_CORP1': 9, 'DS_PART_CORP1': 'Pé'},
        {'CD_PART_CORP1': 10, 'DS_PART_CORP1': 'Todo o corpo'},
        {'CD_PART_CORP1': 11, 'DS_PART_CORP1': 'Outro'},
        {'CD_PART_CORP1': 99, 'DS_PART_CORP1': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_part_corp1 = spark.createDataFrame(row_data)

df = df.join(df_part_corp1, df.CD_PART_CORP1==df_part_corp1.CD_PART_CORP1, how='left') \
       .drop(df.DS_PART_CORP1) \
       .drop(df_part_corp1.CD_PART_CORP1)

# COMMAND ----------

data = [
        {'CD_PART_CORP2': 1, 'DS_PART_CORP2': 'Olho'},
        {'CD_PART_CORP2': 2, 'DS_PART_CORP2': 'Cabeça'},
        {'CD_PART_CORP2': 3, 'DS_PART_CORP2': 'Pescoço'},
        {'CD_PART_CORP2': 4, 'DS_PART_CORP2': 'Tórax'},
        {'CD_PART_CORP2': 5, 'DS_PART_CORP2': 'Abdome'},
        {'CD_PART_CORP2': 6, 'DS_PART_CORP2': 'Mão'},
        {'CD_PART_CORP2': 7, 'DS_PART_CORP2': 'Membro superior'},
        {'CD_PART_CORP2': 8, 'DS_PART_CORP2': 'Membro Inferior'},
        {'CD_PART_CORP2': 9, 'DS_PART_CORP2': 'Pé'},
        {'CD_PART_CORP2': 10, 'DS_PART_CORP2': 'Todo o corpo'},
        {'CD_PART_CORP2': 11, 'DS_PART_CORP2': 'Outro'},
        {'CD_PART_CORP2': 99, 'DS_PART_CORP2': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_part_corp2 = spark.createDataFrame(row_data)

df = df.join(df_part_corp2, df.CD_PART_CORP2==df_part_corp2.CD_PART_CORP2, how='left') \
       .drop(df.DS_PART_CORP2) \
       .drop(df_part_corp2.CD_PART_CORP2)

# COMMAND ----------

data = [
        {'CD_PART_CORP3': 1, 'DS_PART_CORP3': 'Olho'},
        {'CD_PART_CORP3': 2, 'DS_PART_CORP3': 'Cabeça'},
        {'CD_PART_CORP3': 3, 'DS_PART_CORP3': 'Pescoço'},
        {'CD_PART_CORP3': 4, 'DS_PART_CORP3': 'Tórax'},
        {'CD_PART_CORP3': 5, 'DS_PART_CORP3': 'Abdome'},
        {'CD_PART_CORP3': 6, 'DS_PART_CORP3': 'Mão'},
        {'CD_PART_CORP3': 7, 'DS_PART_CORP3': 'Membro superior'},
        {'CD_PART_CORP3': 8, 'DS_PART_CORP3': 'Membro Inferior'},
        {'CD_PART_CORP3': 9, 'DS_PART_CORP3': 'Pé'},
        {'CD_PART_CORP3': 10, 'DS_PART_CORP3': 'Todo o corpo'},
        {'CD_PART_CORP3': 11, 'DS_PART_CORP3': 'Outro'},
        {'CD_PART_CORP3': 99, 'DS_PART_CORP3': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_part_corp3 = spark.createDataFrame(row_data)

df = df.join(df_part_corp3, df.CD_PART_CORP3==df_part_corp3.CD_PART_CORP3, how='left') \
       .drop(df.DS_PART_CORP3) \
       .drop(df_part_corp3.CD_PART_CORP3)

# COMMAND ----------

data = [
        {'CD_REGIME': 1, 'DS_REGIME': 'Hospitalar'},
        {'CD_REGIME': 2, 'DS_REGIME': 'Ambulatório'},
        {'CD_REGIME': 3, 'DS_REGIME': 'Ambos'},
        {'CD_REGIME': 4, 'DS_REGIME': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_regime = spark.createDataFrame(row_data)

df = df.join(df_regime, df.CD_REGIME==df_regime.CD_REGIME, how='left') \
       .drop(df.DS_REGIME) \
       .drop(df_regime.CD_REGIME)

# COMMAND ----------

data = [
        {'CD_EVOLUCAO': 1, 'DS_EVOLUCAO': 'Cura'},
        {'CD_EVOLUCAO': 2, 'DS_EVOLUCAO': 'Incapacidade temporária'},
        {'CD_EVOLUCAO': 3, 'DS_EVOLUCAO': 'Incapacidade parcial permanente'},
        {'CD_EVOLUCAO': 4, 'DS_EVOLUCAO': 'Incapacidade total permanente'},
        {'CD_EVOLUCAO': 5, 'DS_EVOLUCAO': 'Óbito por acidente de trabalho grave'},
        {'CD_EVOLUCAO': 6, 'DS_EVOLUCAO': 'Óbito por outras causas'},
        {'CD_EVOLUCAO': 7, 'DS_EVOLUCAO': 'Outro'},
        {'CD_EVOLUCAO': 9, 'DS_EVOLUCAO': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_evolucao = spark.createDataFrame(row_data)

df = df.join(df_evolucao, df.CD_EVOLUCAO==df_evolucao.CD_EVOLUCAO, how='left') \
       .drop(df.DS_EVOLUCAO) \
       .drop(df_evolucao.CD_EVOLUCAO)

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

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))



# COMMAND ----------

df.write \
  .partitionBy('NR_ANO_NOT') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


