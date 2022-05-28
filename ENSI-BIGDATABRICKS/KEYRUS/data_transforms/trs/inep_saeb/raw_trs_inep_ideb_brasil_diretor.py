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
#   'path_origin': 'crw/inep_saeb/saeb_diretor_unificada',
#   'path_territorio': 'mtd/corp/estrutura_territorial/',
#   'path_destination': 'inep_saeb/saeb_diretor_unificada',
# }

#dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

#adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_inep_ideb_brasil_diretor',
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

transformation = [('ID_CAPITAL', 'CD_CAPITAL', 'Int'), ('ID_DEPENDENCIA_ADM', 'CD_DEPENDENCIA_ADM', 'Int'), ('ID_ESCOLA', 'CD_ENTIDADE', 'Int'), ('ID_LOCALIZACAO', 'CD_LOCALIZACAO', 'Int'), ('ID_MUNICIPIO', 'CD_MUNICIPIO', 'Int'), ('ID_PROVA_BRASIL', 'CD_PROVA_BRASIL', 'Int'), ('ID_REGIAO', 'CD_REGIAO_GEOGRAFICA', 'Int'), ('ID_UF', 'CD_UF', 'Int'), ('IN_PREENCHIMENTO_QUESTIONARIO', 'FL_PREENCHIMENTO_QUESTIONARIO', 'Int')]


# COMMAND ----------

for column, renamed, _type in transformation:
  df_source = df_source.withColumn(column, col(column).cast(_type))
  df_source = df_source.withColumnRenamed(column, renamed)

# COMMAND ----------

df_source = df_source.drop('CD_REGIAO_GEOGRAFICA','CD_UF','dh_insercao_raw','dh_arq_in')

# COMMAND ----------

df_source = df_source.join(df_territorio, on='CD_MUNICIPIO', how='left')

# COMMAND ----------

df_source_2011 = df_source.filter(col('CD_PROVA_BRASIL') == lit(2011))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q003',\
                  when(df_source_2011.TX_RESP_Q003 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q003 == 'C', 'B').otherwise(df_source_2011.TX_RESP_Q003))

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
                  when(df_source_2011.TX_RESP_Q009 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q009 == 'B', 'C').\
                  when(df_source_2011.TX_RESP_Q009 == 'C', 'D').\
                  when(df_source_2011.TX_RESP_Q009 == 'D', 'E').\
                  when(df_source_2011.TX_RESP_Q009 == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q014',\
                  when(df_source_2011.TX_RESP_Q021 == 'A', 'D').\
                  when(df_source_2011.TX_RESP_Q021 == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q021 == 'D', 'C').\
                  when(df_source_2011.TX_RESP_Q021 == 'E', 'C').\
                  when(df_source_2011.TX_RESP_Q021 == 'F', 'C').otherwise(df_source_2011.TX_RESP_Q021))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q026',\
                  when(df_source_2011.TX_RESP_Q022 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q022 == 'B', 'A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q031',\
                  when(df_source_2011.TX_RESP_Q029  == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q029  == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q029  == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q029  == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q029  == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q029',\
                  when(df_source_2011.TX_RESP_Q024  == 'A', 'C').\
                  when(df_source_2011.TX_RESP_Q024  == 'B', 'D').\
                  when(df_source_2011.TX_RESP_Q024  == 'C', 'E').\
                  when(df_source_2011.TX_RESP_Q024  == 'D', 'B').\
                  when(df_source_2011.TX_RESP_Q024  == 'E','A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q026',\
                  when(df_source_2011.TX_RESP_Q022 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q022 == 'B', 'A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q039',\
                  when(df_source_2011.TX_RESP_Q033 == 'E', 'F').otherwise(df_source_2011.TX_RESP_Q033))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q040',\
                  when(df_source_2011.TX_RESP_Q034  == 'A', lit(None)).\
                  when(df_source_2011.TX_RESP_Q034  == 'B', 'A').\
                  when(df_source_2011.TX_RESP_Q034  == 'H', 'I').\
                  when(df_source_2011.TX_RESP_Q034  == 'I', 'J').otherwise(df_source_2011.TX_RESP_Q034))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q043',\
                  when(df_source_2011.TX_RESP_Q043 == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q043 == 'B', 'A').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q067',\
                  when(df_source_2011.TX_RESP_Q055 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q055))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q068',\
                  when(df_source_2011.TX_RESP_Q056 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q056))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q069',\
                  when(df_source_2011.TX_RESP_Q057 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q057))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q070',\
                  when(df_source_2011.TX_RESP_Q058 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q058))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q071',\
                  when(df_source_2011.TX_RESP_Q059 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q059))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q072',\
                  when(df_source_2011.TX_RESP_Q060 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q060))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q073',\
                  when(df_source_2011.TX_RESP_Q061 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q061))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q074',\
                  when(df_source_2011.TX_RESP_Q062 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q062))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q075',\
                  when(df_source_2011.TX_RESP_Q063 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q063))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q076',\
                  when(df_source_2011.TX_RESP_Q064 == 'C', 'D').otherwise(df_source_2011.TX_RESP_Q064))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q109',\
                  when(df_source_2011.TX_RESP_Q208  == 'B', 'A').\
                  when(df_source_2011.TX_RESP_Q109  == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q109  == 'B', 'C').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q110',\
                  when(df_source_2011.TX_RESP_Q208  == 'B', 'A').\
                  when(df_source_2011.TX_RESP_Q211  == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q211  == 'B', 'C').otherwise(lit(None)))

# COMMAND ----------

df_source_2011 = df_source_2011.withColumn('TX_RESP_Q111',\
                  when(df_source_2011.TX_RESP_Q208  == 'B', 'A').\
                  when(df_source_2011.TX_RESP_Q111  == 'A', 'B').\
                  when(df_source_2011.TX_RESP_Q111  == 'B', 'C').otherwise(lit(None)))

# COMMAND ----------

df_source_2013_2015 = df_source.filter(df_source.CD_PROVA_BRASIL.isin([2013,2015]))

# COMMAND ----------

df_source_2013_2015 = df_source_2013_2015.withColumn('TX_RESP_Q003',\
                  when(df_source_2013_2015.TX_RESP_Q003 == 'B', 'C').\
                  when(df_source_2013_2015.TX_RESP_Q003 == 'C', 'B').otherwise(df_source_2013_2015.TX_RESP_Q003))

# COMMAND ----------

df_source_2017 = df_source.filter(col('CD_PROVA_BRASIL') >= lit(2017))

# COMMAND ----------

df = df_source_2017.union(df_source_2013_2015).union(df_source_2011)

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.write.parquet(target, mode='overwrite')