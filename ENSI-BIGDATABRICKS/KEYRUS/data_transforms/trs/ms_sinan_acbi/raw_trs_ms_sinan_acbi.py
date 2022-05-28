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

sheet_name='ACBI'
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
        {'CD_PERCUTANEA': 1, 'DS_PERCUTANEA': 'Sim'},
        {'CD_PERCUTANEA': 2, 'DS_PERCUTANEA': 'Não'},
        {'CD_PERCUTANEA': 3, 'DS_PERCUTANEA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_PERCUTANEA==df_data_aux.CD_PERCUTANEA, how='left') \
       .drop(df.DS_PERCUTANEA) \
       .drop(df_data_aux.CD_PERCUTANEA)



# COMMAND ----------

data = [
        {'CD_PELE_INTEG': 1, 'DS_PELE_INTEG': 'Sim'},
        {'CD_PELE_INTEG': 2, 'DS_PELE_INTEG': 'Não'},
        {'CD_PELE_INTEG': 3, 'DS_PELE_INTEG': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_PELE_INTEG==df_data_aux.CD_PELE_INTEG, how='left') \
       .drop(df.DS_PELE_INTEG) \
       .drop(df_data_aux.CD_PELE_INTEG)



# COMMAND ----------

data = [
        {'CD_PELE_NAO_I': 1, 'DS_PELE_NAO_I': 'Sim'},
        {'CD_PELE_NAO_I': 2, 'DS_PELE_NAO_I': 'Não'},
        {'CD_PELE_NAO_I': 3, 'DS_PELE_NAO_I': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_PELE_NAO_I==df_data_aux.CD_PELE_NAO_I, how='left') \
       .drop(df.DS_PELE_NAO_I) \
       .drop(df_data_aux.CD_PELE_NAO_I)



# COMMAND ----------

data = [
        {'CD_OUTRO_EXP': 1, 'DS_OUTRO_EXP': 'Sim'},
        {'CD_OUTRO_EXP': 2, 'DS_OUTRO_EXP': 'Não'},
        {'CD_OUTRO_EXP': 3, 'DS_OUTRO_EXP': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_OUTRO_EXP==df_data_aux.CD_OUTRO_EXP, how='left') \
       .drop(df.DS_OUTRO_EXP) \
       .drop(df_data_aux.CD_OUTRO_EXP)



# COMMAND ----------


data = [
        {'CD_MAT_ORG': 1, 'DS_MAT_ORG': 'Sangue'},
        {'CD_MAT_ORG': 2, 'DS_MAT_ORG': 'Líquor'},
        {'CD_MAT_ORG': 3, 'DS_MAT_ORG': 'Líquido pleura'},
        {'CD_MAT_ORG': 4, 'DS_MAT_ORG': 'Líquido ascítico'},
        {'CD_MAT_ORG': 5, 'DS_MAT_ORG': 'Líquido amniótico'},
        {'CD_MAT_ORG': 6, 'DS_MAT_ORG': 'Fluído com sangue'},
        {'CD_MAT_ORG': 7, 'DS_MAT_ORG': 'Soro/plasma'},
        {'CD_MAT_ORG': 8, 'DS_MAT_ORG': 'Outros'},
        {'CD_MAT_ORG': 9, 'DS_MAT_ORG': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MAT_ORG==df_data_aux.CD_MAT_ORG, how='left') \
       .drop(df.DS_MAT_ORG) \
       .drop(df_data_aux.CD_MAT_ORG)



# COMMAND ----------

data = [
         {'CD_TIPO_ACID': "01" , 'DS_TIPO_ACID': "Administ. de medicação endovenosa"},
         {'CD_TIPO_ACID': "02" , 'DS_TIPO_ACID': "Administ. de medicação intramuscular"},
         {'CD_TIPO_ACID': "03" , 'DS_TIPO_ACID': "Administ. de medicação subcutânea"},
         {'CD_TIPO_ACID': "04" , 'DS_TIPO_ACID': "Administ. de medicação intradérmica"},
         {'CD_TIPO_ACID': "05" , 'DS_TIPO_ACID': "Punção venosa/arterial para coleta de sangue"},
         {'CD_TIPO_ACID': "06" , 'DS_TIPO_ACID': "Punção venosa/arterial não especificada"},
         {'CD_TIPO_ACID': "07" , 'DS_TIPO_ACID': "Descarte inadequado de material perfuro/cortante em saco de lixo "}, 
         {'CD_TIPO_ACID': "08" , 'DS_TIPO_ACID': "Descarte inadequado de material perfuro/cortante em bancada, cama, chão, etc..."},
         {'CD_TIPO_ACID': "09" , 'DS_TIPO_ACID': "Lavanderia"},
         {'CD_TIPO_ACID': "10" , 'DS_TIPO_ACID': "Lavagem de material"},
         {'CD_TIPO_ACID': "11" , 'DS_TIPO_ACID': "Manipulação de caixa com material perfuro/cortante"},
         {'CD_TIPO_ACID': "12" , 'DS_TIPO_ACID': "Procedimento cirúrgico"},
         {'CD_TIPO_ACID': "13" , 'DS_TIPO_ACID': "Procedimento odontológico"},
         {'CD_TIPO_ACID': "14" , 'DS_TIPO_ACID': "Procedimento laboratorial"},
         {'CD_TIPO_ACID': "15" , 'DS_TIPO_ACID': "Dextro"},
         {'CD_TIPO_ACID': "16" , 'DS_TIPO_ACID': "Reencape"}, 
         {'CD_TIPO_ACID': "98" , 'DS_TIPO_ACID': "Outros"},
         {'CD_TIPO_ACID': "99" , 'DS_TIPO_ACID': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_TIPO_ACID==df_data_aux.CD_TIPO_ACID, how='left') \
       .drop(df.DS_TIPO_ACID) \
       .drop(df_data_aux.CD_TIPO_ACID)



# COMMAND ----------

data = [
        {'CD_AGENTE': "1", 'DS_AGENTE': "Agulha com lúmen (luz)"}, 
        {'CD_AGENTE': "2", 'DS_AGENTE': "Agulha sem lúmen/maciça"},
        {'CD_AGENTE': "3", 'DS_AGENTE': "Intracath"},
        {'CD_AGENTE': "4", 'DS_AGENTE': "Vidros"},
        {'CD_AGENTE': "5", 'DS_AGENTE': "Lâmina/lanceta (qualquer tipo)"},
        {'CD_AGENTE': "6", 'DS_AGENTE': "Outros"},
        {'CD_AGENTE': "9", 'DS_AGENTE': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AGENTE==df_data_aux.CD_AGENTE, how='left') \
       .drop(df.DS_AGENTE) \
       .drop(df_data_aux.CD_AGENTE)



# COMMAND ----------

data = [
        {'CD_LUVA': 1, 'DS_LUVA': 'Sim'},
        {'CD_LUVA': 2, 'DS_LUVA': 'Não'},
        {'CD_LUVA': 3, 'DS_LUVA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_LUVA==df_data_aux.CD_LUVA, how='left') \
       .drop(df.DS_LUVA) \
       .drop(df_data_aux.CD_LUVA)



# COMMAND ----------

data = [
        {'CD_LUVA': 1, 'DS_LUVA': 'Sim'},
        {'CD_LUVA': 2, 'DS_LUVA': 'Não'},
        {'CD_LUVA': 9, 'DS_LUVA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_LUVA==df_data_aux.CD_LUVA, how='left') \
       .drop(df.DS_LUVA) \
       .drop(df_data_aux.CD_LUVA)



# COMMAND ----------

data = [
        {'CD_AVENTAL': 1, 'DS_AVENTAL': 'Sim'},
        {'CD_AVENTAL': 2, 'DS_AVENTAL': 'Não'},
        {'CD_AVENTAL': 9, 'DS_AVENTAL': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AVENTAL==df_data_aux.CD_AVENTAL, how='left') \
       .drop(df.DS_AVENTAL) \
       .drop(df_data_aux.CD_AVENTAL)



# COMMAND ----------

data = [
        {'CD_OCULOS': 1, 'DS_OCULOS': 'Sim'},
        {'CD_OCULOS': 2, 'DS_OCULOS': 'Não'},
        {'CD_OCULOS': 9, 'DS_OCULOS': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_OCULOS==df_data_aux.CD_OCULOS, how='left') \
       .drop(df.DS_OCULOS) \
       .drop(df_data_aux.CD_OCULOS)



# COMMAND ----------

data = [
        {'CD_MASCARA': 1, 'DS_MASCARA': 'Sim'},
        {'CD_MASCARA': 2, 'DS_MASCARA': 'Não'},
        {'CD_MASCARA': 9, 'DS_MASCARA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MASCARA==df_data_aux.CD_MASCARA, how='left') \
       .drop(df.DS_MASCARA) \
       .drop(df_data_aux.CD_MASCARA)



# COMMAND ----------

data = [
        {'CD_FACIAL': 1, 'DS_FACIAL': 'Sim'},
        {'CD_FACIAL': 2, 'DS_FACIAL': 'Não'},
        {'CD_FACIAL': 9, 'DS_FACIAL': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FACIAL==df_data_aux.CD_FACIAL, how='left') \
       .drop(df.DS_FACIAL) \
       .drop(df_data_aux.CD_FACIAL)



# COMMAND ----------

data = [
        {'CD_BOTA': 1, 'DS_BOTA': 'Sim'},
        {'CD_BOTA': 2, 'DS_BOTA': 'Não'},
        {'CD_BOTA': 9, 'DS_BOTA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_BOTA==df_data_aux.CD_BOTA, how='left') \
       .drop(df.DS_BOTA) \
       .drop(df_data_aux.CD_BOTA)



# COMMAND ----------

data = [
        {'CD_VACINA': 1, 'DS_VACINA': 'Vacinado'},
        {'CD_VACINA': 2, 'DS_VACINA': 'Não Vacinado'},
        {'CD_VACINA': 9, 'DS_VACINA': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_VACINA==df_data_aux.CD_VACINA, how='left') \
       .drop(df.DS_VACINA) \
       .drop(df_data_aux.CD_VACINA)



# COMMAND ----------

data = [
        {'CD_ANTI_HIV': "1", 'DS_ANTI_HIV': "Positivo"},
        {'CD_ANTI_HIV': "2", 'DS_ANTI_HIV': "Negativo"},
        {'CD_ANTI_HIV': "3", 'DS_ANTI_HIV': "Inconclusivo"},
        {'CD_ANTI_HIV': "4", 'DS_ANTI_HIV': "Não realizado"},
        {'CD_ANTI_HIV': "9", 'DS_ANTI_HIV': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ANTI_HIV==df_data_aux.CD_ANTI_HIV, how='left') \
       .drop(df.DS_ANTI_HIV) \
       .drop(df_data_aux.CD_ANTI_HIV)



# COMMAND ----------

data = [
        {'CD_HBSAG': "1", 'DS_HBSAG': "Positivo"},
        {'CD_HBSAG': "2", 'DS_HBSAG': "Negativo"},
        {'CD_HBSAG': "3", 'DS_HBSAG': "Inconclusivo"},
        {'CD_HBSAG': "4", 'DS_HBSAG': "Não realizado"},
        {'CD_HBSAG': "9", 'DS_HBSAG': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_HBSAG==df_data_aux.CD_HBSAG, how='left') \
       .drop(df.DS_HBSAG) \
       .drop(df_data_aux.CD_HBSAG)



# COMMAND ----------

data = [
        {'CD_ANTI_HBS': "1", 'DS_ANTI_HBS': "Positivo"},
        {'CD_ANTI_HBS': "2", 'DS_ANTI_HBS': "Negativo"},
        {'CD_ANTI_HBS': "3", 'DS_ANTI_HBS': "Inconclusivo"},
        {'CD_ANTI_HBS': "4", 'DS_ANTI_HBS': "Não realizado"},
        {'CD_ANTI_HBS': "9", 'DS_ANTI_HBS': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ANTI_HBS==df_data_aux.CD_ANTI_HBS, how='left') \
       .drop(df.DS_ANTI_HBS) \
       .drop(df_data_aux.CD_ANTI_HBS)



# COMMAND ----------

data = [
        {'CD_ANTI_HCV': "1", 'DS_ANTI_HCV': "Positivo"},
        {'CD_ANTI_HCV': "2", 'DS_ANTI_HCV': "Negativo"},
        {'CD_ANTI_HCV': "3", 'DS_ANTI_HCV': "Inconclusivo"},
        {'CD_ANTI_HCV': "4", 'DS_ANTI_HCV': "Não realizado"},
        {'CD_ANTI_HCV': "9", 'DS_ANTI_HCV': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ANTI_HCV==df_data_aux.CD_ANTI_HCV, how='left') \
       .drop(df.DS_ANTI_HCV) \
       .drop(df_data_aux.CD_ANTI_HCV)



# COMMAND ----------

data = [
        {'CD_ANTI_HCV': "1", 'DS_ANTI_HCV': "Positivo"},
        {'CD_ANTI_HCV': "2", 'DS_ANTI_HCV': "Negativo"},
        {'CD_ANTI_HCV': "3", 'DS_ANTI_HCV': "Inconclusivo"},
        {'CD_ANTI_HCV': "4", 'DS_ANTI_HCV': "Não realizado"},
        {'CD_ANTI_HCV': "9", 'DS_ANTI_HCV': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_ANTI_HCV==df_data_aux.CD_ANTI_HCV, how='left') \
       .drop(df.DS_ANTI_HCV) \
       .drop(df_data_aux.CD_ANTI_HCV)



# COMMAND ----------

data = [
        {'CD_FONTE': 1, 'DS_FONTE': 'Sim'},
        {'CD_FONTE': 2, 'DS_FONTE': 'Não'},
        {'CD_FONTE': 9, 'DS_FONTE': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FONTE==df_data_aux.CD_FONTE, how='left') \
       .drop(df.DS_FONTE) \
       .drop(df_data_aux.CD_FONTE)



# COMMAND ----------

data = [
        {'CD_FO_HBSAG': "1", 'DS_FO_HBSAG': "Positivo"},
        {'CD_FO_HBSAG': "2", 'DS_FO_HBSAG': "Negativo"},
        {'CD_FO_HBSAG': "3", 'DS_FO_HBSAG': "Inconclusivo"},
        {'CD_FO_HBSAG': "4", 'DS_FO_HBSAG': "Não realizado"},
        {'CD_FO_HBSAG': "9", 'DS_FO_HBSAG': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FO_HBSAG==df_data_aux.CD_FO_HBSAG, how='left') \
       .drop(df.DS_FO_HBSAG) \
       .drop(df_data_aux.CD_FO_HBSAG)



# COMMAND ----------

data = [
        {'CD_FO_ANT_HIV': "1", 'DS_FO_ANT_HIV': "Positivo"},
        {'CD_FO_ANT_HIV': "2", 'DS_FO_ANT_HIV': "Negativo"},
        {'CD_FO_ANT_HIV': "3", 'DS_FO_ANT_HIV': "Inconclusivo"},
        {'CD_FO_ANT_HIV': "4", 'DS_FO_ANT_HIV': "Não realizado"},
        {'CD_FO_ANT_HIV': "9", 'DS_FO_ANT_HIV': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FO_ANT_HIV==df_data_aux.CD_FO_ANT_HIV, how='left') \
       .drop(df.DS_FO_ANT_HIV) \
       .drop(df_data_aux.CD_FO_ANT_HIV)



# COMMAND ----------

data = [
        {'CD_FO_ANT_HBC': "1", 'DS_FO_ANT_HBC': "Positivo"},
        {'CD_FO_ANT_HBC': "2", 'DS_FO_ANT_HBC': "Negativo"},
        {'CD_FO_ANT_HBC': "3", 'DS_FO_ANT_HBC': "Inconclusivo"},
        {'CD_FO_ANT_HBC': "4", 'DS_FO_ANT_HBC': "Não realizado"},
        {'CD_FO_ANT_HBC': "9", 'DS_FO_ANT_HBC': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FO_ANT_HBC==df_data_aux.CD_FO_ANT_HBC, how='left') \
       .drop(df.DS_FO_ANT_HBC) \
       .drop(df_data_aux.CD_FO_ANT_HBC)



# COMMAND ----------

data = [
        {'CD_FO_ANT_HCV': "1", 'DS_FO_ANT_HCV': "Positivo"},
        {'CD_FO_ANT_HCV': "2", 'DS_FO_ANT_HCV': "Negativo"},
        {'CD_FO_ANT_HCV': "3", 'DS_FO_ANT_HCV': "Inconclusivo"},
        {'CD_FO_ANT_HCV': "4", 'DS_FO_ANT_HCV': "Não realizado"},
        {'CD_FO_ANT_HCV': "9", 'DS_FO_ANT_HCV': "Ignorado"},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_FO_ANT_HCV==df_data_aux.CD_FO_ANT_HCV, how='left') \
       .drop(df.DS_FO_ANT_HCV) \
       .drop(df_data_aux.CD_FO_ANT_HCV)



# COMMAND ----------

data = [
        {'CD_SEM_QUIMIO': 1, 'DS_SEM_QUIMIO': 'Sim'},
        {'CD_SEM_QUIMIO': 2, 'DS_SEM_QUIMIO': 'Não'},
        {'CD_SEM_QUIMIO': 9, 'DS_SEM_QUIMIO': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_SEM_QUIMIO==df_data_aux.CD_SEM_QUIMIO, how='left') \
       .drop(df.DS_SEM_QUIMIO) \
       .drop(df_data_aux.CD_SEM_QUIMIO)



# COMMAND ----------

data = [
        {'CD_RECUSA_QUI': 1, 'DS_RECUSA_QUI': 'Sim'},
        {'CD_RECUSA_QUI': 2, 'DS_RECUSA_QUI': 'Não'},
        {'CD_RECUSA_QUI': 9, 'DS_RECUSA_QUI': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_RECUSA_QUI==df_data_aux.CD_RECUSA_QUI, how='left') \
       .drop(df.DS_RECUSA_QUI) \
       .drop(df_data_aux.CD_RECUSA_QUI)



# COMMAND ----------

data = [
        {'CD_AZT3TC': 1, 'DS_AZT3TC': 'Sim'},
        {'CD_AZT3TC': 2, 'DS_AZT3TC': 'Não'},
        {'CD_AZT3TC': 9, 'DS_AZT3TC': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AZT3TC==df_data_aux.CD_AZT3TC, how='left') \
       .drop(df.DS_AZT3TC) \
       .drop(df_data_aux.CD_AZT3TC)



# COMMAND ----------

data = [
        {'CD_AZT3TC_IND': 1, 'DS_AZT3TC_IND': 'Sim'},
        {'CD_AZT3TC_IND': 2, 'DS_AZT3TC_IND': 'Não'},
        {'CD_AZT3TC_IND': 9, 'DS_AZT3TC_IND': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AZT3TC_IND==df_data_aux.CD_AZT3TC_IND, how='left') \
       .drop(df.DS_AZT3TC_IND) \
       .drop(df_data_aux.CD_AZT3TC_IND)



# COMMAND ----------

data = [
        {'CD_AZT3TC_NFV': 1, 'DS_AZT3TC_NFV': 'Sim'},
        {'CD_AZT3TC_NFV': 2, 'DS_AZT3TC_NFV': 'Não'},
        {'CD_AZT3TC_NFV': 9, 'DS_AZT3TC_NFV': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_AZT3TC_NFV==df_data_aux.CD_AZT3TC_NFV, how='left') \
       .drop(df.DS_AZT3TC_NFV) \
       .drop(df_data_aux.CD_AZT3TC_NFV)



# COMMAND ----------

data = [
        {'CD_IMU_HEP_B': 1, 'DS_IMU_HEP_B': 'Sim'},
        {'CD_IMU_HEP_B': 2, 'DS_IMU_HEP_B': 'Não'},
        {'CD_IMU_HEP_B': 9, 'DS_IMU_HEP_B': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_IMU_HEP_B==df_data_aux.CD_IMU_HEP_B, how='left') \
       .drop(df.DS_IMU_HEP_B) \
       .drop(df_data_aux.CD_IMU_HEP_B)



# COMMAND ----------

data = [
        {'CD_VAC_HEP_B': 1, 'DS_VAC_HEP_B': 'Sim'},
        {'CD_VAC_HEP_B': 2, 'DS_VAC_HEP_B': 'Não'},
        {'CD_VAC_HEP_B': 9, 'DS_VAC_HEP_B': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_VAC_HEP_B==df_data_aux.CD_VAC_HEP_B, how='left') \
       .drop(df.DS_VAC_HEP_B) \
       .drop(df_data_aux.CD_VAC_HEP_B)



# COMMAND ----------

data = [
        {'CD_OUTRO_ARV': 1, 'DS_OUTRO_ARV': 'Sim'},
        {'CD_OUTRO_ARV': 2, 'DS_OUTRO_ARV': 'Não'},
        {'CD_OUTRO_ARV': 9, 'DS_OUTRO_ARV': 'Ignorado'},
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_OUTRO_ARV==df_data_aux.CD_OUTRO_ARV, how='left') \
       .drop(df.DS_OUTRO_ARV) \
       .drop(df_data_aux.CD_OUTRO_ARV)



# COMMAND ----------

data = [
        {'CD_EVOLUCAO': "1", 'DS_EVOLUCAO': "Alta com conversão sorológica Especificar vírus"},
        {'CD_EVOLUCAO': "2", 'DS_EVOLUCAO': "Alta sem conversão sorológica"},
        {'CD_EVOLUCAO': "3", 'DS_EVOLUCAO': "Alta paciente fonte negativo"},
        {'CD_EVOLUCAO': "4", 'DS_EVOLUCAO': "Abandono"},
        {'CD_EVOLUCAO': "5", 'DS_EVOLUCAO': "Óbito por acidente com exposição a material biológico"},
        {'CD_EVOLUCAO': "6", 'DS_EVOLUCAO': "Óbito por outra causa"},
        {'CD_EVOLUCAO': "9", 'DS_EVOLUCAO': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_EVOLUCAO==df_data_aux.CD_EVOLUCAO, how='left') \
       .drop(df.DS_EVOLUCAO) \
       .drop(df_data_aux.CD_EVOLUCAO)



# COMMAND ----------

data = [
        {'DS_EVO_OUTR_OLD': "1", 'DS_EVO_OUTR': "Alta com conversão sorológica Especificar vírus"},
        {'DS_EVO_OUTR_OLD': "2", 'DS_EVO_OUTR': "Alta sem conversão sorológica"},
        {'DS_EVO_OUTR_OLD': "3", 'DS_EVO_OUTR': "Alta paciente fonte negativo"},
        {'DS_EVO_OUTR_OLD': "4", 'DS_EVO_OUTR': "Abandono"},
        {'DS_EVO_OUTR_OLD': "5", 'DS_EVO_OUTR': "Óbito por acidente com exposição a material biológico"},
        {'DS_EVO_OUTR_OLD': "6", 'DS_EVO_OUTR': "Óbito por outra causa"},
        {'DS_EVO_OUTR_OLD': "9", 'DS_EVO_OUTR': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.DS_EVO_OUTR==df_data_aux.DS_EVO_OUTR_OLD, how='left') \
       .drop(df.DS_EVO_OUTR) \
       .drop(df_data_aux.DS_EVO_OUTR_OLD)



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
        {'CD_MUCOSA': 1, 'DS_MUCOSA': "Sim"},
        {'CD_MUCOSA': 2, 'DS_MUCOSA': "Não"},
        {'CD_MUCOSA': 9, 'DS_MUCOSA': "Ignorado"}
]

row_data = [pyspark.sql.Row(**d) for d in data]
df_data_aux = spark.createDataFrame(row_data)

df = df.join(df_data_aux, df.CD_MUCOSA==df_data_aux.CD_MUCOSA, how='left') \
       .drop(df.DS_MUCOSA) \
       .drop(df_data_aux.CD_MUCOSA)



# COMMAND ----------

df = df[columns]

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))


# COMMAND ----------

df.write \
  .partitionBy('NR_ANO_NOT') \
  .parquet(path=adl_trs, mode='overwrite')


# COMMAND ----------


