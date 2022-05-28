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
# MAGIC <pre>
# MAGIC Processo	raw_trs_entidade_regional
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_entidade_regional
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/entidade_regional
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 60 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Entidade Regional (RJ, SP, CT) atualmente contemplando SESI e SENAI
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atuaização	Não se aplica
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw tb_entidade_regional
# MAGIC </pre>
# MAGIC 
# MAGIC Dev: Marcela

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

# COMMAND ----------

import json
import re

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_entidade_regional"], "destination": "/mtd/corp/entidade_regional"}

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/tmp/dev/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_trs_entidade_regional',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_tb = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_tb)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import trim, col, current_timestamp, from_utc_timestamp
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

useful_columns = ["CD_ENTIDADE_REGIONAL", "CD_ENTIDADE_NACIONAL", "NM_ENTIDADE_REGIONAL", "SG_ENTIDADE_REGIONAL", "SG_UF", "DT_ATUALIZACAO", "CD_ENTIDADE_REGIONAL_ERP"]

# COMMAND ----------

df = spark.read.parquet(src_tb)\
.select(*useful_columns)\
.filter(col("CD_ENTIDADE_NACIONAL").isin(2,3))

# COMMAND ----------

df = df.withColumnRenamed('CD_ENTIDADE_REGIONAL', 'cd_entidade_regional')\
.withColumnRenamed('CD_ENTIDADE_NACIONAL', 'cd_entidade_nacional')\
.withColumnRenamed('SG_UF', 'sg_uf_regional')\
.withColumnRenamed('DT_ATUALIZACAO', 'dt_ultima_atualizacao_oltp')\
.withColumnRenamed('CD_ENTIDADE_REGIONAL_ERP', 'cd_entidade_regional_erp_oltp')\
.withColumn('nm_entidade_regional', trim(col("NM_ENTIDADE_REGIONAL")))\
.withColumn('sg_entidade_regional', trim(col("SG_ENTIDADE_REGIONAL")))

# COMMAND ----------

#add control fields from trusted_control_field egg
df = tcf.add_control_fields(df, var_adf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Salvando tabela no destino

# COMMAND ----------

df.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")