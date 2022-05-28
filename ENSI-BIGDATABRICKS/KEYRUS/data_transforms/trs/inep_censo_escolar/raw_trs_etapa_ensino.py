# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type truncate/full insert
# MAGIC 
# MAGIC Processo raw_trs_etapa_ensino
# MAGIC Tabela/Arquivo Origem /raw/crw/inep_etapa_ensino/
# MAGIC Tabela/Arquivo Destino /trs/uniepro/etapa_ensino/
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino etapa de ensino forma uma estrutura de códigos que visa orientar, organizar e consolidar 
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga raw 

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window

import re
import json
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

try:
  tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
except:
  # USE THIS ONLY FOR DEVELOPMENT PURPOSES
  from datetime import datetime
  
  tables =  {
    "path_origin": "crw/inep_censo_escolar/etapa_ensino/", 
    "path_destination": "inep_censo_escolar/etapa_ensino/"
  }

  dls = {"folders":{"landing":"/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/raw","trusted":"/trs"}}

  adf = {
    'adf_factory_name': 'cnibigdatafactory', 
    'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
    'adf_pipeline_run_id': 'p1',
    'adf_trigger_id': 't1',
    'adf_trigger_name': 'author_dev',
    'adf_trigger_time': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    'adf_trigger_type': 'Manual'
  }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

df = df.select(f.col('codigo').cast('int').alias('cd_etapa_ensino'),
               f.col('nome_etapa').cast('varchar(250)').alias('nm_etapa_ensino'),
               'ano')

# COMMAND ----------

records_to_add = spark.createDataFrame([[43, 'EJA - Presencial - 1ª a 4ª Série', 1900], 
                                        [44,'NÃO EJA - Presencial - 5ª a 8ª Série APLICA', 1900],
                                        [45, ' EJA - Presencial - Ensino Médio', 1900], 
                                        [46, 'EJA - Semipresencial - 1ª a 4ª Série', 1900], 
                                        [47, 'EJA - Semipresencial - 5ª a 8ª Série', 1900], 
                                        [48, 'EJA - Semipresencial - Ensino Médio', 1900],
                                        [60, 'EJA - Presencial - Integrada à Ed. Profissional de Nível Fundamental - FIC', 1900],
                                        [61, 'EJA – Semipresencial - Integrada à Ed. Profissional de Nível Fundamental - FIC', 1900],
                                        [62, 'EJA - Presencial - Integrada à Ed. Profissional de Nível Médio', 1900],
                                        [63, 'EJA - Semipresencial - Integrada à Ed. Profissional de Nível Médio', 1900]], schema=df.schema)

# COMMAND ----------

df = df.union(records_to_add)

# COMMAND ----------

w = Window.partitionBy('cd_etapa_ensino')
df = (df
      .withColumn('tmp', f.max('ano').over(w))
      .where(f.col('ano') == f.col('tmp'))
      .drop('ano', 'tmp'))

# COMMAND ----------

# Command to insert a field for data control.

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.save(path=target, format="parquet", mode='overwrite')