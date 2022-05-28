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

sheet_name='DERM'
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
        {'CD_HIPERTEN': 1, 'DS_HIPERTEN': 'Sim'},
        {'CD_HIPERTEN': 2, 'DS_HIPERTEN': 'Não'},
        {'CD_HIPERTEN': 3, 'DS_HIPERTEN': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_HIPERTEN==df_data_aux.CD_HIPERTEN, how='left') \
       .drop(df.DS_HIPERTEN) \
       .drop(df_data_aux.CD_HIPERTEN)



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
        {'CD_AGENTE': "01", 'DS_AGENTE': "Cimento"}, 
        {'CD_AGENTE': "02", 'DS_AGENTE': "Borracha"},
        {'CD_AGENTE': "03", 'DS_AGENTE': "Plástico"},
        {'CD_AGENTE': "04", 'DS_AGENTE': "Solventes Orgânicos"},
        {'CD_AGENTE': "05", 'DS_AGENTE': "Graxas"},
        {'CD_AGENTE': "06", 'DS_AGENTE': "Óleo de Corte"},
        {'CD_AGENTE': "07", 'DS_AGENTE': "Resinas"},
        {'CD_AGENTE': "08", 'DS_AGENTE': "Níquel"},
        {'CD_AGENTE': "09", 'DS_AGENTE': "Cosméticos"},
        {'CD_AGENTE': "10", 'DS_AGENTE': "Madeiras"},
        {'CD_AGENTE': "11", 'DS_AGENTE': "Cromo"},
        {'CD_AGENTE': "12", 'DS_AGENTE': "Outros"},
        {'CD_AGENTE': "99", 'DS_AGENTE': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AGENTE==df_data_aux.CD_AGENTE, how='left') \
       .drop(df.DS_AGENTE) \
       .drop(df_data_aux.CD_AGENTE)


# COMMAND ----------

data = [
        {'CD_LESAO': "01", 'DS_LESAO': "Mão"}, 
        {'CD_LESAO': "02", 'DS_LESAO': "Membro Superior"},
        {'CD_LESAO': "03", 'DS_LESAO': "Cabeça"},
        {'CD_LESAO': "04", 'DS_LESAO': "Pescoço"},
        {'CD_LESAO': "05", 'DS_LESAO': "Tórax"},
        {'CD_LESAO': "06", 'DS_LESAO': "Abdome"},
        {'CD_LESAO': "07", 'DS_LESAO': "Membro Inferior"},
        {'CD_LESAO': "08", 'DS_LESAO': "Pé"},
        {'CD_LESAO': "09", 'DS_LESAO': "Todo o Corpo"},
        {'CD_LESAO': "10", 'DS_LESAO': "Outro"},
        {'CD_LESAO': "99", 'DS_LESAO': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_LESAO==df_data_aux.CD_LESAO, how='left') \
       .drop(df.DS_LESAO) \
       .drop(df_data_aux.CD_LESAO)


# COMMAND ----------

data = [
        {'CD_EPICUTA': 1, 'DS_EPICUTA': 'Sim'},
        {'CD_EPICUTA': 2, 'DS_EPICUTA': 'Não'},
        {'CD_EPICUTA': 3, 'DS_EPICUTA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EPICUTA==df_data_aux.CD_EPICUTA, how='left') \
       .drop(df.DS_EPICUTA) \
       .drop(df_data_aux.CD_EPICUTA)


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


# COMMAND ----------

data = [
        {'CD_AFAST': 1, 'DS_AFAST': 'hora'},
        {'CD_AFAST': 2, 'DS_AFAST': 'dia'},
        {'CD_AFAST': 3, 'DS_AFAST': 'mês'},
        {'CD_AFAST': 4, 'DS_AFAST': 'ano'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_tempo_trab = spark.createDataFrame(row_data)

df = df.join(df_tempo_trab, df.CD_AFAST==df_tempo_trab.CD_AFAST, how='left') \
       .drop(df.DS_AFAST) \
       .drop(df_tempo_trab.CD_AFAST)


# COMMAND ----------

data = [
        {'CD_EVOL_AFAST': 1, 'DS_EVOL_AFAST': 'Melhora'},
        {'CD_EVOL_AFAST': 2, 'DS_EVOL_AFAST': 'Piora'},
        {'CD_EVOL_AFAST': 9, 'DS_EVOL_AFAST': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EVOL_AFAST==df_data_aux.CD_EVOL_AFAST, how='left') \
       .drop(df.DS_EVOL_AFAST) \
       .drop(df_data_aux.CD_EVOL_AFAST)


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
        {'CD_AFAST_RISC': 1, 'DS_AFAST_RISC': 'Sim'},
        {'CD_AFAST_RISC': 2, 'DS_AFAST_RISC': 'Não'}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AFAST_RISC==df_data_aux.CD_AFAST_RISC, how='left') \
       .drop(df.DS_AFAST_RISC) \
       .drop(df_data_aux.CD_AFAST_RISC)


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

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO') \
  .parquet(path=adl_trs, mode='overwrite')

