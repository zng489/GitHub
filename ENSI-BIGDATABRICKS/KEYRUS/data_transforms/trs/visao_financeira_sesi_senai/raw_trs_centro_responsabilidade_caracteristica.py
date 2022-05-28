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
# MAGIC Processo	raw_trs_centro_responsabilidade_caracteristica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/protheus11/ctd010
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/centro_responsabilidade_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	Não há. São 1.455 registros atualmente
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre as características dos Centros de Responsabilidade (PCR). Essa tabela possui a validade do Centro de Responsabilidade e a vigência.
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental que insere ou versiona o registro caso a chave subtring(ctd_item, 3, 9)  seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw ctd010, que ocorre às 20:00
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
var_tables =  {"origins": ["/bdo/protheus11/ctd010"], "destination": "/mtd/corp/centro_responsabilidade_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_centro_responsabilidade',
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

src_ctd = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_ctd)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import substring, trim, lit, asc, desc, row_number, col, when, concat, length, upper, lower, dense_rank, count, from_utc_timestamp, current_timestamp, to_date, unix_timestamp, concat_ws, lag, lead
from pyspark.sql import Window
from pyspark.sql.types import LongType
import datetime
from trs_control_field import trs_control_field as tcf
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

first_load = False

# COMMAND ----------

try:
  df_trs = spark.read.parquet(sink)
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC   TRIM(SUBSTRING(CTD_ITEM, 3, 50)) as cd_centro_responsabilidade,
# MAGIC   SUBSTRING(CTD_ITEM, 1, 2) as cd_ano_referencia,
# MAGIC   CTD_DTEXIS,
# MAGIC   CTD_DTEXSF
# MAGIC FROM PROTHEUS11_CTD010 
# MAGIC WHERE R_E_C_N_O_ >= 1802
# MAGIC   AND   D_E_L_E_T_ <> '*'
# MAGIC   AND LEN(TRIM(SUBSTRING(CTD_ITEM, 3, 50))) = 9 
# MAGIC </pre>

# COMMAND ----------

useful_columns = ["CTD_ITEM", "CTD_DTEXIS", "CTD_DTEXSF", "R_E_C_N_O_", "D_E_L_E_T_"]

# COMMAND ----------

new_data = spark.read.parquet(src_ctd).select(*useful_columns)\
.filter((col("R_E_C_N_O_") >= 1802) &\
        (col("D_E_L_E_T_") != "*") &\
        (length(trim(substring(col("CTD_ITEM"), 3, 50))) == 9))\
.withColumn("cd_centro_responsabilidade", trim(substring(col("CTD_ITEM"), 3, 50)))\
.withColumn("cd_ano_referencia", substring(col("CTD_ITEM"), 1, 2))\
.withColumn("dt_inicio_vigencia", to_date(unix_timestamp(col("CTD_DTEXIS"), "yyyyMMdd").cast("timestamp")))\
.withColumn("dt_fim_vigencia", to_date(unix_timestamp(col("CTD_DTEXSF"), "yyyyMMdd").cast("timestamp")))\
.withColumn("dh_ultima_atualizacao_oltp", lit(var_adf["adf_trigger_time"]).cast("timestamp"))\
.drop("D_E_L_E_T_", "R_E_C_N_O_", "CTD_ITEM", "CTD_DTEXIS", "CTD_DTEXSF")\
.dropDuplicates()\
.coalesce(1)\
.cache()


new_data.count()

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  # Partitioning as specified by the documentation. repartition(5) is a good parameter since we avoid to write many small files in ADLS
  new_data.write.save(path=sink, format="parquet", mode="overwrite")
  new_data = new_data.unpersist()
  dbutils.notebook.exit('{"first_load": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC Versioning - This is the else section: *if first_load is False:*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Magic words:
# MAGIC 
# MAGIC <pre>
# MAGIC /* /* Verifica se existe na trs centro_responsabilidade_caracteristica o substring(raw.ctd_item, 3, 9)= trs.cd_centro_relacionamento
# MAGIC /* e o campo substring(raw.ctd_item,1,2) = trs.cd_ano_referencia
# MAGIC /* Quando da inclusão do registro 
# MAGIC         Se existe, 
# MAGIC             Se houve alteração em algum dos campos envolvidos no select
# MAGIC                UPDATE trs centro_responsabilidade_caracteristica (existente)
# MAGIC                     trs.dt_inicio_vigencia = raw.ctd_dtexis
# MAGIC                     trs.dt_fim_vigencia = raw.ctd_dtexsf
# MAGIC                     trs.dh_ultima_atualizacao_oltp = sysdate
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC             
# MAGIC         Senão, NÃO existe: 
# MAGIC                INSERT trs centro_responsabilidade_caracteristica (nova)
# MAGIC 	              trs.cd_centro_responsabilidade  = substring(raw.ctd_item, 3, 9)
# MAGIC 	              trs.cd_ano_referencia  = substring(raw.ctd_item, 1, 2)
# MAGIC 	              trs.dt_inicio_vigencia  = raw.ctd_dtexis
# MAGIC 	              trs.dt_fim_vigencia  = raw.ctd_dtexsf
# MAGIC                       trs.dh_ultima_atualizacao_oltp = sysdate
# MAGIC                       trs.dh_insercao_trs  = sysdate	                    
# MAGIC </pre>

# COMMAND ----------

var_key = list(["cd_centro_responsabilidade", "cd_ano_referencia"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

df_trs = df_trs.withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

new_data = new_data.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

new_data = new_data.union(df_trs.select(new_data.columns))

# COMMAND ----------

hash_columns = var_key + ["dt_inicio_vigencia", "dt_fim_vigencia"]

new_data = new_data.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp")))).dropDuplicates()

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which only date has changed.

new_data = new_data\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

new_data = new_data.withColumn("lead_dt_inicio_vigencia", lead("dt_inicio_vigencia", 1).over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp"))))\
.withColumn("lead_dt_fim_vigencia", lead("dt_fim_vigencia", 1).over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp"))))\
.withColumn("lead_dh_ultima_atualizacao_oltp", lead("dh_ultima_atualizacao_oltp", 1).over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

# When lead is not NULL, then we must change the field with the lead (UPDATE)
# Only these updated records must stay in trusted

new_data = new_data. \
withColumn("dt_inicio_vigencia", when(col("lead_dt_inicio_vigencia").isNotNull(), col("lead_dt_inicio_vigencia")).otherwise(col("dt_inicio_vigencia"))). \
withColumn("dt_fim_vigencia", when(col("lead_dt_fim_vigencia").isNotNull(), col("lead_dt_fim_vigencia")).otherwise(col("dt_fim_vigencia"))). \
withColumn("dh_ultima_atualizacao_oltp", when(col("lead_dh_ultima_atualizacao_oltp").isNotNull(), col("lead_dh_ultima_atualizacao_oltp")).otherwise(col("dh_ultima_atualizacao_oltp"))). \
withColumn("rank", row_number().over(var_window.orderBy(asc("dh_ultima_atualizacao_oltp")))). \
filter(col("rank") == 1). \
drop("lead_dt_inicio_vigencia", "lead_dt_fim_vigencia", "lead_dh_ultima_atualizacao_oltp", "rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in sink

# COMMAND ----------

new_data.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

new_data.unpersist()

# COMMAND ----------

