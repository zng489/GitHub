# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_modalidade_ensino_profissional
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/bd_basi/tb_produto_servico
# MAGIC /raw/bdo/bd_basi/tb_curso"
# MAGIC Tabela/Arquivo Destino	/trs/mtd/senai/modalidade_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São menos de 30 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Cadastro dos produtos oferecidos pelos departamentos nacional e regional, que correspondem às estrutura hierarquica de cursos do SENAI
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atualização	Não se aplica
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trusted specific parameter section

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
var_tables =  {"origins": ["/bdo/bd_basi/tb_produto_servico", "/bdo/bd_basi/tb_curso"], "destination": "/mtd/senai/modalidade_ensino_profissional"}

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
       'adf_pipeline_name': 'raw_trs_modalidade_ensino_profissional',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-27T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_tbp, src_tbc = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tbp, src_tbc)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation Section

# COMMAND ----------

# Columns for tb_produto_servico
ps_n4_columns = ["cd_produto_servico", "ds_codigo_produto", "ds_produto", "cd_produto_superior", "dt_atualizacao"]
ps_n4 = spark.read.parquet(src_tbp).select(*ps_n4_columns).cache()

# COMMAND ----------

# Dubug
#ps_n4.count(), ps_n4.distinct().count()

# COMMAND ----------

curso_columns =  ["cd_produto_servico"]
curso = spark.read.parquet(src_tbc).select(*curso_columns).cache()

# COMMAND ----------

# Dubug
# There's not that many courses, so, in the case of joins, this will lead to many, many duplicates. 
#curso.count(), curso.distinct().count()

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, to_date, year, current_date, dense_rank, desc, trim
from pyspark.sql.window import Window
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC FROM DOCUMENTATION:
# MAGIC <pre>
# MAGIC ( SELECT *, ROW_NUMBER() OVER(PARTITION BY CD_PRODUTO_SERVICO ORDER BY DT_ATUALIZACAO DESC) SEQ
# MAGIC   FROM TB_PRODUTO_SERVICO )  N4
# MAGIC INNER JOIN TB_CURSO C ON C.CD_PRODUTO_SERVICO = N4.CD_PRODUTO_SERVICO
# MAGIC ...
# MAGIC WHERE N4.SEQ = 1
# MAGIC </pre>

# COMMAND ----------

#Preparing for future steps
n2_n3_columns = ["cd_produto_servico", "ds_codigo_produto", "ds_produto", "cd_produto_superior"]
ps_n3 = ps_n2 = ps_n4.select(*n2_n3_columns)

# COMMAND ----------

# All records for n3 and n4 are distinct, and all products ares also different, as supposed
#ps_n3.count(), ps_n3.distinct().count(), ps_n3.select("cd_produto_servico").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC First, remove the many duplicates from tb_curso

# COMMAND ----------

curso = curso.distinct()

# COMMAND ----------

ps_n4 = ps_n4.withColumn("rank", dense_rank().over(Window.partitionBy("cd_produto_servico").orderBy(desc("dt_atualizacao"))))

# COMMAND ----------

ps_n4 = ps_n4.filter(ps_n4["rank"] == 1).drop(*["rank", "dt_atualizacao"]).distinct()

# COMMAND ----------

#ps_n4.count(), ps_n4.select("cd_produto_servico").distinct().count()

# COMMAND ----------

# Documentation really sates INNER. However, in a conversation with Keyrus' BAs, we changed this to LEFT so we can investigate the case cause when things are restricted to INNER, hierarchy is inconsistent. After showing these results to customer and getting to a conclusion, we can come back to INNER.
#ps_n4 = ps_n4.join(curso, ["cd_produto_servico"], "inner")
ps_n4 = ps_n4.join(curso, ["cd_produto_servico"], "left")

# COMMAND ----------

#ps_n4.count(), ps_n3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC FROM DOCUMENTATION:
# MAGIC <pre>
# MAGIC LEFT JOIN BASI_TB_PRODUTO_SERVICO N3 ON N4.CD_PRODUTO_SUPERIOR = N3.CD_PRODUTO_SERVICO
# MAGIC LEFT JOIN BASI_TB_PRODUTO_SERVICO N2 ON N3.CD_PRODUTO_SUPERIOR = N2.CD_PRODUTO_SERVICO
# MAGIC </pre>

# COMMAND ----------

n4_col_names = {"cd_produto_servico": "cd_produto_servico_modal_ensino_prof", 
                "ds_produto": "ds_produto_servico_modal_ensino_prof", 
                "ds_codigo_produto": "cd_estrut_produto_servico_n4"}
for c in n4_col_names:
  ps_n4 = ps_n4.withColumnRenamed(c , n4_col_names[c])

# COMMAND ----------

n3_col_names = {"cd_produto_servico": "cd_produto_servico_programa_ensino_prof", 
                "ds_produto": "ds_produto_servico_programa_ensino_prof",
                "ds_codigo_produto": "cd_estrut_produto_servico_n3",
                "cd_produto_superior": "cd_produto_superior_n3"}
for c in n3_col_names:
  ps_n3 = ps_n3.withColumnRenamed(c , n3_col_names[c]) 

# COMMAND ----------

n2_col_names = {"cd_produto_servico": "cd_produto_servico_nivel_ensino_prof", 
                "ds_produto": "ds_produto_servico_nivel_ensino_prof",
                "ds_codigo_produto": "cd_estrut_produto_servico_n2",
                "cd_produto_superior": "cd_produto_superior_n2"}
for c in n2_col_names:
  ps_n2 = ps_n2.withColumnRenamed(c , n2_col_names[c]) 

# COMMAND ----------

ps_join = ps_n4.join(ps_n3, ps_n4["cd_produto_superior"] == ps_n3["cd_produto_servico_programa_ensino_prof"], "left")

# COMMAND ----------

#ps_join.count(), ps_join.distinct().count()

# COMMAND ----------

ps_join = ps_join.join(ps_n2, ps_join["cd_produto_superior_n3"] == ps_n2["cd_produto_servico_nivel_ensino_prof"], "left")

# COMMAND ----------

#ps_join.count(), ps_join.distinct().count()

# COMMAND ----------

ps_join = ps_join.drop(*[ x for x in ps_join.columns if x.startswith("cd_produto_superior")])

# COMMAND ----------

# MAGIC %md
# MAGIC Data Quality Section

# COMMAND ----------

fields_to_trim = [x for x in ps_join.columns if x.startswith("ds_") or x.startswith("cd_estrut_")]

# COMMAND ----------

for f in fields_to_trim:
  ps_join = ps_join.withColumn(f, trim(ps_join[f]))

# COMMAND ----------

#add control fields from trusted_control_field egg
ps_join = tcf.add_control_fields(ps_join, var_adf)

# COMMAND ----------

ps_join.coalesce(4).write.save(path=sink, format="parquet", mode="overwrite")