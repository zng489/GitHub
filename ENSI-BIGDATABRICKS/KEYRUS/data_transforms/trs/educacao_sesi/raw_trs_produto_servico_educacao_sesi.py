# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type update
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_produto_servico_educacao_sesi
# MAGIC Tabela/Arquivo Origem	/raw/bdo/scae/produto_servico
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/produto_servico_educacao_sesi
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp) as cd_ano_atualizacao
# MAGIC Descrição Tabela/Arquivo Destino	Corresponde a relação de produtos e serviços oferecidos pelo SESI.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou sobrescreve o registro caso a chave cd_produto_servico_sesi seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC </pre>

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
var_tables =  {"origins": ["/bdo/scae/produto_servico"], "destination": "/mtd/sesi/produto_servico_educacao_sesi"}

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
       'adf_pipeline_name': 'raw_trs_produto_servico_educacao_sesi',
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

src_ps = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_ps)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import lit, col, trim, substring, count, concat, from_utc_timestamp, current_timestamp
from pyspark.sql import Window
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC cod_produto_servico, 
# MAGIC nome_produto, 
# MAGIC TRIM(SUBSTRING(cod_centro_responsabilidade, 3, 15)) AS cd_centro_responsabilidade, 
# MAGIC cod_produto_smd,
# MAGIC ind_ativo,
# MAGIC '20'||trim(substring(cod_centro_responsabilidade, 1, 2)) AS cd_ano_referencia
# MAGIC FROM produto_servico
# MAGIC </pre>

# COMMAND ----------

produto_servico_useful_columns = ["cod_produto_servico", "nome_produto", "cod_centro_responsabilidade", "cod_produto_smd", "ind_ativo"]
df = spark.read.parquet(src_ps)\
.select(*produto_servico_useful_columns)\
.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying transformations

# COMMAND ----------

df = df.withColumn("cd_centro_responsabilidade", trim(substring(col("cod_centro_responsabilidade"), 3, 15)))\
.withColumn("cd_ano_referencia", concat(lit("20"),trim(substring(col("cod_centro_responsabilidade"), 1, 2))))\
.withColumn("nome_produto", trim(col("nome_produto")))\
.drop("cod_centro_responsabilidade")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming columns and adjusting types

# COMMAND ----------

column_name_mapping = {'cod_produto_servico': 'cd_produto_servico_educacao_sesi',
                       'nome_produto': 'nm_produto_servico_educacao_sesi',                       
                       'ind_ativo': 'fl_ind_ativo_oltp'
                       }
  
for key in column_name_mapping:
  df =  df.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

column_type_map = {'cd_produto_servico_educacao_sesi': 'int', 
                   'nm_produto_servico_educacao_sesi': 'string',
                   'cod_produto_smd': 'string',
                   'fl_ind_ativo_oltp': 'int',
                   'cd_centro_responsabilidade': 'string',
                   'cd_ano_referencia': 'int',
                   }

for c in column_type_map:
  df = df.withColumn(c, df[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC Add timestamp of the process and partition column

# COMMAND ----------

#add control fields from trusted_control_field egg
df = tcf.add_control_fields(df, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink

# COMMAND ----------

df.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

