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

raw_path = f'{raw}/crw/{table["schema"]}/{table["table"]}'
trs_path = f'{trs}/{table["schema"]}/{table["table"]}'

adl_path = var_adls_uri
adl_raw = f"{adl_path}{raw_path}"
adl_trs = f"{adl_path}{trs_path}"

# COMMAND ----------

prm = dls['folders']['prm']
prm_file = table["prm_path"].split('/')[-1]
prm_path = f'{prm}/usr/{table["schema"]}_{table["table"]}/{prm_file}'

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

# COMMAND ----------

sheet_name='MENT'
columns = [name[1] for name in var_prm_dict[sheet_name]]

# COMMAND ----------

headers

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
df_ds_sexo = spark.createDataFrame(row_data)

df = df.join(df_ds_sexo, df.DS_SEXO==df_ds_sexo.DS_SEXO_OLD, how='left') \
       .drop(df.DS_SEXO) \
       .drop(df_ds_sexo.DS_SEXO_OLD)

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
        {'CD_ALCOOL': 1, 'DS_ALCOOL': 'Sim'},
        {'CD_ALCOOL': 2, 'DS_ALCOOL': 'Não'},
        {'CD_ALCOOL': 9, 'DS_ALCOOL': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_alcool = spark.createDataFrame(row_data)

df = df.join(df_alcool, df.CD_ALCOOL==df_alcool.CD_ALCOOL, how='left') \
       .drop(df.DS_ALCOOL) \
       .drop(df_alcool.CD_ALCOOL)

# COMMAND ----------

data = [
        {'CD_PSICO_FARM': 1, 'DS_PSICO_FARM': 'Sim'},
        {'CD_PSICO_FARM': 2, 'DS_PSICO_FARM': 'Não'},
        {'CD_PSICO_FARM': 9, 'DS_PSICO_FARM': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_psico_farm = spark.createDataFrame(row_data)

df = df.join(df_psico_farm, df.CD_PSICO_FARM==df_psico_farm.CD_PSICO_FARM, how='left') \
       .drop(df.DS_PSICO_FARM) \
       .drop(df_psico_farm.CD_PSICO_FARM)

# COMMAND ----------

data = [
        {'CD_DROGAS': 1, 'DS_DROGAS': 'Sim'},
        {'CD_DROGAS': 2, 'DS_DROGAS': 'Não'},
        {'CD_DROGAS': 9, 'DS_DROGAS': 'Ignorado'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_drogas = spark.createDataFrame(row_data)

df = df.join(df_drogas, df.CD_DROGAS==df_drogas.CD_DROGAS, how='left') \
       .drop(df.DS_DROGAS) \
       .drop(df_drogas.CD_DROGAS)

# COMMAND ----------

data = [
        {'CD_FUMA': 1, 'DS_FUMA': 'Sim'},
        {'CD_FUMA': 2, 'DS_FUMA': 'Não'},
        {'CD_FUMA': 3, 'DS_FUMA': 'Ex-fumante'},
        {'CD_FUMA': 9, 'DS_FUMA': 'Ignorado'}        
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_fuma = spark.createDataFrame(row_data)

df = df.join(df_fuma, df.CD_FUMA==df_fuma.CD_FUMA, how='left') \
       .drop(df.DS_FUMA) \
       .drop(df_fuma.CD_FUMA)

# COMMAND ----------

data = [
        {'CD_TEMPO_FUMA': 1, 'DS_TEMPO_FUMA': 'Hora'},
        {'CD_TEMPO_FUMA': 2, 'DS_TEMPO_FUMA': 'Dia'},
        {'CD_TEMPO_FUMA': 3, 'DS_TEMPO_FUMA': 'Mês'},
        {'CD_TEMPO_FUMA': 4, 'DS_TEMPO_FUMA': 'Ano'}        
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tempo_fuma = spark.createDataFrame(row_data)

df = df.join(df_tempo_fuma, df.CD_TEMPO_FUMA==df_tempo_fuma.CD_TEMPO_FUMA, how='left') \
       .drop(df.DS_TEMPO_FUMA) \
       .drop(df_tempo_fuma.CD_TEMPO_FUMA)

# COMMAND ----------

data = [
        {'CD_AFAST_DESG': 1, 'DS_AFAST_DESG': 'Sim'},
        {'CD_AFAST_DESG': 2, 'DS_AFAST_DESG': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_afast_desg = spark.createDataFrame(row_data)

df = df.join(df_afast_desg, df.CD_AFAST_DESG==df_afast_desg.CD_AFAST_DESG, how='left') \
       .drop(df.DS_AFAST_DESG) \
       .drop(df_afast_desg.CD_AFAST_DESG)

# COMMAND ----------

data = [
        {'CD_INDIVIDUAL': 1, 'DS_INDIVIDUAL': 'Sim'},
        {'CD_INDIVIDUAL': 2, 'DS_INDIVIDUAL': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_individual = spark.createDataFrame(row_data)

df = df.join(df_individual, df.CD_INDIVIDUAL==df_individual.CD_INDIVIDUAL, how='left') \
       .drop(df.DS_INDIVIDUAL) \
       .drop(df_individual.CD_INDIVIDUAL)

# COMMAND ----------

data = [
        {'CD_MUDA_TRAB': 1, 'DS_MUDA_TRAB': 'Sim'},
        {'CD_MUDA_TRAB': 2, 'DS_MUDA_TRAB': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_muda_trab = spark.createDataFrame(row_data)

df = df.join(df_muda_trab, df.CD_MUDA_TRAB==df_muda_trab.CD_MUDA_TRAB, how='left') \
       .drop(df.DS_MUDA_TRAB) \
       .drop(df_muda_trab.CD_MUDA_TRAB)

# COMMAND ----------

data = [
        {'CD_NENHUM': 1, 'DS_NENHUM': 'Sim'},
        {'CD_NENHUM': 2, 'DS_NENHUM': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_nenhum = spark.createDataFrame(row_data)

df = df.join(df_nenhum, df.CD_NENHUM==df_nenhum.CD_NENHUM, how='left') \
       .drop(df.DS_NENHUM) \
       .drop(df_nenhum.CD_NENHUM)

# COMMAND ----------

data = [
        {'CD_COLETIVA': 1, 'DS_COLETIVA': 'Sim'},
        {'CD_COLETIVA': 2, 'DS_COLETIVA': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_coletiva = spark.createDataFrame(row_data)

df = df.join(df_coletiva, df.CD_COLETIVA==df_coletiva.CD_COLETIVA, how='left') \
       .drop(df.DS_COLETIVA) \
       .drop(df_coletiva.CD_COLETIVA)

# COMMAND ----------

data = [
        {'CD_AFAST_TRAB': 1, 'DS_AFAST_TRAB': 'Sim'},
        {'CD_AFAST_TRAB': 2, 'DS_AFAST_TRAB': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_afast_trab = spark.createDataFrame(row_data)

df = df.join(df_afast_trab, df.CD_AFAST_TRAB==df_afast_trab.CD_AFAST_TRAB, how='left') \
       .drop(df.DS_AFAST_TRAB) \
       .drop(df_afast_trab.CD_AFAST_TRAB)

# COMMAND ----------

data = [
        {'CD_CONDUTA': 1, 'DS_CONDUTA': 'Sim'},
        {'CD_CONDUTA': 2, 'DS_CONDUTA': 'Não'}     
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_conduta = spark.createDataFrame(row_data)

df = df.join(df_conduta, df.CD_CONDUTA==df_conduta.CD_CONDUTA, how='left') \
       .drop(df.DS_CONDUTA) \
       .drop(df_conduta.CD_CONDUTA)

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
df_tempo_trab = spark.createDataFrame(row_data)

df = df.join(df_tempo_trab, df.CD_TPTEMPORIS==df_tempo_trab.CD_TPTEMPORIS, how='left') \
       .drop(df.DS_TPTEMPORIS) \
       .drop(df_tempo_trab.CD_TPTEMPORIS)


# COMMAND ----------

data = [
        {'CD_REGIME': 1, 'DS_REGIME': 'Hospitalar'},
        {'CD_REGIME': 2, 'DS_REGIME': 'Abulatorial'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tempo_trab = spark.createDataFrame(row_data)

df = df.join(df_tempo_trab, df.CD_REGIME==df_tempo_trab.CD_REGIME, how='left') \
       .drop(df.DS_REGIME) \
       .drop(df_tempo_trab.CD_REGIME)


# COMMAND ----------

data = [
        {'CD_TRAB_DOE': 1, 'DS_TRAB_DOE': 'Sim'},
        {'CD_TRAB_DOE': 2, 'DS_TRAB_DOE': 'Não'},
        {'CD_TRAB_DOE': 9, 'DS_TRAB_DOE': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TRAB_DOE==df_data_aux.CD_TRAB_DOE, how='left') \
       .drop(df.DS_TRAB_DOE) \
       .drop(df_data_aux.CD_TRAB_DOE)


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


# COMMAND ----------

data = [
        {'CD_CONDUTA': 1, 'DS_CONDUTA': 'Sim'},
        {'CD_CONDUTA': 2, 'DS_CONDUTA': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_CONDUTA==df_data_aux.CD_CONDUTA, how='left') \
       .drop(df.DS_CONDUTA) \
       .drop(df_data_aux.CD_CONDUTA)


# COMMAND ----------

data = [
        {'CD_EVOLUCAO': "1", 'DS_EVOLUCAO': "Cura"},
        {'CD_EVOLUCAO': "2", 'DS_EVOLUCAO': "Cura não confirmada"},
        {'CD_EVOLUCAO': "3", 'DS_EVOLUCAO': "Incapacidade temporária"},
        {'CD_EVOLUCAO': "4", 'DS_EVOLUCAO': "Incapacidade permanente parcial"},
        {'CD_EVOLUCAO': "5", 'DS_EVOLUCAO': "Incapacidade permanente total"},
        {'CD_EVOLUCAO': "6", 'DS_EVOLUCAO': "Óbito por doença relacionada ao trabalho"},
        {'CD_EVOLUCAO': "7", 'DS_EVOLUCAO': "Óbito por outra causa"},
        {'CD_EVOLUCAO': "7", 'DS_EVOLUCAO': "Outro"},
        {'CD_EVOLUCAO': "9", 'DS_EVOLUCAO': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EVOLUCAO==df_data_aux.CD_EVOLUCAO, how='left') \
       .drop(df.DS_EVOLUCAO) \
       .drop(df_data_aux.CD_EVOLUCAO)


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


# COMMAND ----------

data = [
        {'CD_CAPES': 1, 'DS_CAPES': "Sim"},
        {'CD_CAPES': 2, 'DS_CAPES': "Não"},
        {'CD_CAPES': 3, 'DS_CAPES': "Não se aplica"},
        {'CD_CAPES': 9, 'DS_CAPES': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_capes = spark.createDataFrame(row_data)

df = df.join(df_capes, df.CD_CAPES==df_capes.CD_CAPES, how='left') \
       .drop(df.DS_CAPES) \
       .drop(df_capes.CD_CAPES)


# COMMAND ----------

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO') \
  .parquet(path=adl_trs, mode='overwrite')
