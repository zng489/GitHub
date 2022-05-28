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
# MAGIC Processo	trs_biz_dim_hierarquia_entidade_regional
# MAGIC Tabela/Arquivo Origem	/trs/mtd/corp/entidade_regional
# MAGIC Tabela/Arquivo Destino	/biz/organizacao/dim_hierarquia_entidade_regional
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 60 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Entidade Regional (RJ, SP, CT) atualmente contemplando SESI e SENAI, e seus níveis hierárquicos superiores
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atualização	Não se aplica
# MAGIC Periodicidade/Horario Execução	Diária, após carga trs entidade_regional, que ocorre às 22:00
# MAGIC 
# MAGIC Manutenção:
# MAGIC   2021/01/07 - Thomaz Moreira - Adição dos dois registros para nulos ao final.
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import re
import json

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables = {"origins": ["/mtd/corp/entidade_regional",
                         "/mtd/corp/entidade_nacional",
                         "/mtd/corp/unidade_atendimento"],
              "destination": "/corporativo/dim_hierarquia_entidade_regional",
              "databricks": {
                "notebook": "/biz/visao_financeira_sesi_senai/trs_biz_dim_hierarquia_entidade_regional"
              }
             }

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/tmp/dev/raw", 
    "trusted": "/tmp/dev/trs",
    "business": "/tmp/dev/biz"
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'trs_biz_dim_hierarquia_entidade_regional',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'thomaz',
       'adf_trigger_time': '2021-01-07T15:00:00.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_er, src_en, src_ua = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_er, src_en, src_ua)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT
# MAGIC reg.cd_entidade_regional,
# MAGIC reg.nm_entidade_regional,
# MAGIC reg.sg_entidade_regional,
# MAGIC reg.sg_uf_regional,
# MAGIC reg.cd_entidade_nacional,
# MAGIC NVL(nac.nm_entidade_nacional, 'NÃO INFORMADA') AS nm_entidade_nacional,
# MAGIC NVL(nac.sg_entidade_nacional, 'N/I') AS sg_entidade_nacional,
# MAGIC SUM(CASE WHEN NVL(ua.cd_tipo_categoria_ativo, -99) = 1 THEN 1 ELSE 0 END) AS qt_unidade_fixa,
# MAGIC SUM(CASE WHEN NVL(ua.cd_tipo_categoria_ativo, -99) = 2 THEN 1 ELSE 0 END) AS qt_unidade_movel
# MAGIC FROM entidade_regional reg 
# MAGIC LEFT JOIN entidade_nacional nac ON reg.cd_entidade_nacional = nac.cd_entidade_nacional
# MAGIC LEFT JOIN unidade_atendimento ua ON reg.cd_entidade_regional = ua.cd_entidade_regional
# MAGIC GROUP BY 
# MAGIC reg.cd_entidade_regional,
# MAGIC reg.nm_entidade_regional,
# MAGIC reg.sg_entidade_regional,
# MAGIC reg.sg_uf_regional,
# MAGIC reg.cd_entidade_nacional,
# MAGIC NVL(nac.nm_entidade_nacional, 'NÃO INFORMADA'),
# MAGIC NVL(nac.sg_entidade_nacional, 'N/I')
# MAGIC </pre>

# COMMAND ----------

from pyspark.sql.functions import when, from_utc_timestamp, current_timestamp, col, coalesce, lit, sum
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

regional_columns = ["cd_entidade_regional", "nm_entidade_regional", "sg_entidade_regional", "sg_uf_regional", "cd_entidade_nacional"]

regional  = spark.read.parquet(src_er)\
.select(*regional_columns)\
.filter(~col("cd_entidade_regional").isin([-98, -99]))

# COMMAND ----------

nacional_columns = ["nm_entidade_nacional", "sg_entidade_nacional", "cd_entidade_nacional"]

nacional = spark.read.parquet(src_en).select(*nacional_columns)\
.withColumn("nm_entidade_nacional", coalesce("nm_entidade_nacional", lit("NÃO INFORMADA").cast("string")))\
.withColumn("sg_entidade_nacional", coalesce("sg_entidade_nacional", lit("N/I").cast("string")))

# COMMAND ----------

unidade_atendimento_columns = ["cd_entidade_regional", "cd_tipo_categoria_ativo"]

unidade_atendimento = spark.read.parquet(src_ua).select(*unidade_atendimento_columns)\
.withColumn("qt_unidade_fixa", when(coalesce(col("cd_tipo_categoria_ativo"), lit(-99).cast("int")) == 1, lit(1).cast("int"))\
                              .otherwise(lit(0).cast("int")))\
.withColumn("qt_unidade_movel", when(coalesce(col("cd_tipo_categoria_ativo"), lit(-99).cast("int")) == 2, lit(1).cast("int"))\
                              .otherwise(lit(0).cast("int")))\
.drop("cd_tipo_categoria_ativo")

# COMMAND ----------

# MAGIC %md 
# MAGIC Join

# COMMAND ----------

df = regional.join(nacional, ["cd_entidade_nacional"], "left")\
.join(unidade_atendimento, ["cd_entidade_regional"], "left")\
.groupBy("cd_entidade_regional", "nm_entidade_regional", "sg_entidade_regional", "sg_uf_regional", "cd_entidade_nacional", "nm_entidade_nacional", "sg_entidade_nacional")\
.agg(sum("qt_unidade_fixa").alias("qt_unidade_fixa"), sum("qt_unidade_movel").alias("qt_unidade_movel"))\
.coalesce(1)

# COMMAND ----------

df_null = spark.createDataFrame(
data=[
  [-2, "Serviço Social da Indústria - SESI-BR", "SESI-BR", "BR", 2, "Serviço Social da Indústria", "SESI", 0, 0],
  [-3, "Serviço Nacional de Aprendizagem Industrial - SENAI-BR", "SENAI-BR", "BR", 3, "Serviço Nacional de Aprendizagem Industrial", "SENAI", 0, 0],
],
schema=df.schema)\
.coalesce(1)

# COMMAND ----------

df = df.union(df_null).coalesce(1)

# COMMAND ----------

#Create dictionary with column transformations required
var_type_map = {"cd_entidade_nacional": "int",
                "cd_entidade_regional": "int",
                "nm_entidade_regional": "string",
                "sg_entidade_regional": "string",
                "sg_uf_regional": "string",
                "nm_entidade_nacional": "string",
                "sg_entidade_nacional": "string"}

#Apply transformations defined in column_map
for c in var_type_map:
  df = df.withColumn(c, col("{}".format(c)).cast(var_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC Add control fields

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC Now just write it!
# MAGIC 
# MAGIC As this table is small, we can coalesce in one file!
# MAGIC 
# MAGIC This one doesn't need partitioning.

# COMMAND ----------

df.write.save(path=sink, format="parquet", mode="overwrite")