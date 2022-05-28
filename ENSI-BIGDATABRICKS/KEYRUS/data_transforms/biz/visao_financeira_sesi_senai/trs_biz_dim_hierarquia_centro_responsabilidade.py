# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	trs_biz_dim_hierarquia_centro_responsabilidade
# MAGIC Tabela/Arquivo Origem	/trs/mtd/corp/centro_responsabilidade
# MAGIC Tabela/Arquivo Destino	/biz/organizacao/dim_hierarquia_centro_responsabilidade
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 1.455 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Os Centros de Responsabilidade (PCR) forma uma estrutura de códigos que visa orientar, organizar e consolidar nacionalmente a construção do orçamento das Receitas e Despesas pelas linhas de negócios desenvolvidas pelo Sistema Indústria
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atuaização	Não se aplica
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs centro_responsabilidade, que ocorre às 20:00
# MAGIC 
# MAGIC Dev:
# MAGIC 
# MAGIC Adequação ao novo processo biz:
# MAGIC   2020-05-28 10:00 - Thomaz Moreira
# MAGIC   2020-07-31 14:00 - Thomaz Moreira - Adição das linhas de registros não encontrados
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1")

# COMMAND ----------

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# MOCK FOR USING THIS IN DEV
"""
var_tables = {"origins":["/mtd/corp/centro_responsabilidade"],
  "destination":"/corporativo/dim_hierarquia_centro_responsabilidade", 
  "databricks": {"notebook": "/biz/visao_financeira_sesi_senai/trs_biz_dim_hierarquia_centro_responsabilidade"}
  }

var_dls =  {"folders":
  { "landing":"/tmp/dev/lnd", 
  "error":"/tmp/dev/err", 
  "staging":"/tmp/dev/stg", 
  "log":"/tmp/dev/log", 
  "raw":"/tmp/dev/raw", 
  "trusted": "/tmp/dev/trs", 
  "business": "/tmp/dev/biz"}
  }

var_adf =  {"adf_factory_name":"cnibigdatafactory",
"adf_pipeline_name":"trs_biz_dim_hierarquia_centro_responsabilidade",
"adf_pipeline_run_id":"development",
"adf_trigger_id":"development",
"adf_trigger_name":"thomaz",
"adf_trigger_time":"2020-07-31T14:00:13.8532201Z",
"adf_trigger_type":"Manual"
}
"""

# COMMAND ----------

var_source = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["origins"][0]) 
var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"]) 
print(var_source)
print(var_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as types
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading table

# COMMAND ----------

df = spark.read.parquet(var_source)\
.withColumnRenamed("cd_centro_responsabilidade_cr_n5", "cd_centro_responsabilidade")\
.withColumnRenamed("ds_centro_responsabilidade_cr_n5", "ds_centro_responsabilidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Choosing columns

# COMMAND ----------

var_list_of_columns = ["cd_natureza_cr_n1", 
                       "ds_natureza_cr_n1", 
                       "cd_linha_acao_cr_n2", 
                       "ds_linha_acao_cr_n2", 
                       "cd_centro_responsabilidade_cr_n3",
                       "ds_centro_responsabilidade_cr_n3", 
                       "cd_centro_responsabilidade_cr_n4", 
                       "ds_centro_responsabilidade_cr_n4", 
                       "cd_centro_responsabilidade", 
                       "ds_centro_responsabilidade"
                      ]

df = df.select(*var_list_of_columns)

# COMMAND ----------

df_null_values = spark.createDataFrame([
  ["-98", "NÃO INFORMADA", "-98", "NÃO INFORMADA", "-98", "NÃO INFORMADA", "-98", "NÃO INFORMADA", "-98", "NÃO INFORMADA"],
  ["-99", "NÃO SE APLICA", "-99", "NÃO SE APLICA", "-99", "NÃO SE APLICA", "-99", "NÃO SE APLICA", "-99", "NÃO SE APLICA"]
], schema=df.schema).coalesce(1)

# COMMAND ----------

df = df.union(df_null_values).coalesce(1)

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing in adls

# COMMAND ----------

df.write.save(path=var_sink, format="parquet", mode="overwrite")