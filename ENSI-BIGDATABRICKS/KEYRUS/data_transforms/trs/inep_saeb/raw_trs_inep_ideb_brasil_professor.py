# Databricks notebook source
import cni_connectors.adls_gen1_connector as connector
import json
import re
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
##   'path_origin': 'crw/inep_saeb/saeb_professor_unificada/',
#   'path_territorio': 'mtd/corp/estrutura_territorial/',
#   'path_destination': 'inep_saeb/saeb_professor_unificada',
# }

#dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}
#
#adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_inep_ideb_brasil_professor',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

source_territorio = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_territorio"])
source_territorio

# COMMAND ----------

df_territorio = spark.read.parquet(source_territorio).select('CD_MUNICIPIO','CD_REGIAO_GEOGRAFICA','CD_UF')

# COMMAND ----------

df_source = spark.read.parquet(source)

# COMMAND ----------

transformation = [('ID_PROVA_BRASIL', 'CD_PROVA_BRASIL', 'Int'), ('ID_REGIAO', 'CD_REGIAO', 'Int'), ('ID_UF', 'CD_UF', 'Int'), ('ID_MUNICIPIO', 'CD_MUNICIPIO', 'Int'), ('ID_ESCOLA', 'CD_ENTIDADE', 'Int'), ('ID_DEPENDENCIA_ADM', 'CD_DEPENDENCIA_ADM', 'Int'), ('ID_LOCALIZACAO', 'CD_LOCALIZACAO', 'Int'), ('ID_CAPITAL', 'CD_CAPITAL', 'Int'), ('ID_TURMA', 'CD_TURMA', 'Int'), ('CO_PROFESSOR', 'CD_PROFESSOR', 'Int'), ('ID_SERIE', 'CD_SERIE', 'Int'), ('IN_PREENCHIMENTO_QUESTIONARIO', 'FL_PREENCHIMENTO_QUESTIONARIO', 'Int')]


# COMMAND ----------

for column, renamed, _type in transformation:
  df_source = df_source.withColumn(column, col(column).cast(_type))
  df_source = df_source.withColumnRenamed(column, renamed)

# COMMAND ----------

df_source = df_source.drop('CD_REGIAO','CD_UF','dh_insercao_raw','dh_arq_in')

# COMMAND ----------

df_source = df_source.join(df_territorio, on='CD_MUNICIPIO', how='left')

# COMMAND ----------

df_source_2011 = df_source.filter(col('CD_PROVA_BRASIL') == lit(2011))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q003',\
                  when(df_source_2011.TX_RESP_Q003 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q003 == 'C', 'B').\
                   when(df_source_2011.TX_RESP_Q003 == 'G', lit(None)).otherwise(df_source_2011.TX_RESP_Q003))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q006',\
                  when(df_source_2011.TX_RESP_Q006 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q006 == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q006 == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q006 == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q006 == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q007',\
                  when(df_source_2011.TX_RESP_Q008 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q008 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q008 == 'C', 'D').\
                  when(df_source_2011.TX_RESP_Q008 == 'D', 'A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q008',\
                  when(df_source_2011.TX_RESP_Q009 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q009 == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q009 == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q009 == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q009 == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q017',\
                  when(df_source_2011.TX_RESP_Q020 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q020 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'C', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'D', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'E', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'F', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'G', 'C').\
                  when(df_source_2011.TX_RESP_Q020 == 'H', 'B').\
                  when(df_source_2011.TX_RESP_Q020 == 'I','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q019',\
                  when(df_source_2011.TX_RESP_Q022 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q022 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'C', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'D', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'E', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'F', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'G', 'C').\
                  when(df_source_2011.TX_RESP_Q022 == 'H', 'B').\
                  when(df_source_2011.TX_RESP_Q022 == 'I','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q020',\
                  when(df_source_2011.TX_RESP_Q036 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q036))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q039',\
                  when(df_source_2011.TX_RESP_Q034 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q034 == 'C', 'A').otherwise(df_source_2011.TX_RESP_Q034))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q040',\
                  when(df_source_2011.TX_RESP_Q035 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q035 == 'C', 'A').otherwise(df_source_2011.TX_RESP_Q035))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q041',\
                  when(df_source_2011.TX_RESP_Q024 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q024 == 'C', 'A').\
                  when(df_source_2011.TX_RESP_Q024 == 'D', 'A').otherwise(df_source_2011.TX_RESP_Q024))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q042',\
                  when(df_source_2011.TX_RESP_Q025 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q025 == 'C', 'A').\
                  when(df_source_2011.TX_RESP_Q025 == 'D', 'A').otherwise(df_source_2011.TX_RESP_Q025))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q052',\
                  when(df_source_2011.TX_RESP_Q043 == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q043 == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q043 == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q043 == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q043 == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q093',\
                  when(df_source_2011.TX_RESP_Q122 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q122 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q122 == 'C', 'B').\
                  when(df_source_2011.TX_RESP_Q122 == 'D', 'A').\
                  when(df_source_2011.TX_RESP_Q122 == 'E',df_source_2011.TX_RESP_Q093).otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q094',\
                  when(df_source_2011.TX_RESP_Q123 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q123 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q123 == 'C', 'B').\
                  when(df_source_2011.TX_RESP_Q123 == 'D', 'A').\
                  when(df_source_2011.TX_RESP_Q123 == 'E',df_source_2011.TX_RESP_Q094).otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q095',\
                  when(df_source_2011.TX_RESP_Q124 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q124 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q124 == 'C', 'B').\
                  when(df_source_2011.TX_RESP_Q124 == 'D', 'A').\
                  when(df_source_2011.TX_RESP_Q124 == 'E',df_source_2011.TX_RESP_Q095).otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q096',\
                  when(df_source_2011.TX_RESP_Q125 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q125 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q125 == 'C', 'B').\
                  when(df_source_2011.TX_RESP_Q125 == 'D', 'A').\
                  when(df_source_2011.TX_RESP_Q125 == 'E',df_source_2011.TX_RESP_Q096).otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q099',\
                  when(df_source_2011.TX_RESP_Q126_A == 'A', 'E').\
                  when(df_source_2011.TX_RESP_Q126_A == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q126_A == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q126_A == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q101',\
                  when(df_source_2011.TX_RESP_Q129_A == 'A', 'E').\
                  when(df_source_2011.TX_RESP_Q129_A == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q129_A == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q129_A == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q106',\
                  when(df_source_2011.TX_RESP_Q121 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q121 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q121 == 'C', 'D').\
                  when(df_source_2011.TX_RESP_Q121 == 'D','E').otherwise(lit(None)))

# COMMAND ----------

df_source_2013_2015 = df_source.filter(df_source.CD_PROVA_BRASIL.isin([2013,2015]))

# COMMAND ----------

df_source_2013_2015 = df_source_2013_2015.withColumn('TX_RESP_Q003',\
                  when(df_source_2013_2015.TX_RESP_Q003 == 'B', 'C').\
                  when(df_source_2013_2015.TX_RESP_Q003 == 'C', 'B').\
                   when(df_source_2013_2015.TX_RESP_Q003 == 'G', lit(None)).otherwise(df_source_2013_2015.TX_RESP_Q003))

# COMMAND ----------

df_source_2017 = df_source.filter(col('CD_PROVA_BRASIL') >= lit(2017))

# COMMAND ----------

df = df_source_2017.union(df_source_2013_2015).union(df_source_2011)

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.write.parquet(target, mode='overwrite')