# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_atendimento_tecnologia_inovacao_situacao
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_atendimento
# MAGIC Tabela/Arquivo Destino	/trs/evt/atendimento_tecnologia_inovacao_situacao
# MAGIC Particionamento Tabela/Arquivo Destino	fl_corrente/YEAR(dt_inicio)
# MAGIC Descrição Tabela/Arquivo Destino    Registra as informações evolutivas no tempo sobre a situação do atendimento dos serviços de tecnologia e inovação, que geram produção e que são contabilizados em termos de consolidação dos dados de produção SENAI. O período de vigência de cada versão do registro deve ser obtida como GTE data de inicio e LTE que data de fim e a posição atual com versão corrente informada = 1
# MAGIC OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), atendimento_tecnologia_inovacao.
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou versiona o registro caso a chave cd_atendimento_sti seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira, maintenance by Tiago Shin
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
var_tables =  {"origins": ["/bdo/bd_basi/tb_atendimento"], "destination": "/evt/atendimento_tecnologia_inovacao_situacao"}

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
       'adf_pipeline_name': 'raw_trs_atendimento_tecnologia_inovacao_situacao',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-10-09T11:30:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_atd = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_atd)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, asc, desc, when, lit, col, concat_ws, lead, lag, date_add, count, array, lower, trim
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dh_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX("dh_inicio_vigencia") from atendimento_tecnologia_inovacao_situacao
# MAGIC 
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink)
  var_max_dh_atualizacao = trusted_data.select(max(col("dh_inicio_vigencia"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

var_max_dh_atualizacao

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC cd_atendimento, 
# MAGIC cd_tipo_situacao_atendimento, 
# MAGIC dt_atualizacao,
# MAGIC dt_inicio,
# MAGIC dt_termino,
# MAGIC fl_origem_recurso_mercado,
# MAGIC fl_origem_recurso_senai,
# MAGIC fl_origem_recurso_fomento,
# MAGIC fl_origem_recurso_outrasent,
# MAGIC fl_atendimento_rede,
# MAGIC fl_excluido 
# MAGIC FROM tb_atendimento raw
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp 
# MAGIC ORDER BY a.dt_atualizacao
# MAGIC </pre>

# COMMAND ----------

atd_columns = ["cd_atendimento", "cd_tipo_situacao_atendimento", "fl_excluido", "dt_atualizacao", "dt_inicio", "dt_termino", "fl_origem_recurso_mercado", "fl_origem_recurso_senai", "fl_origem_recurso_fomento", "fl_origem_recurso_outrasent", "fl_atendimento_rede"]

new_data = spark.read.parquet(src_atd). \
select(*atd_columns). \
filter((col("dt_atualizacao") > var_max_dh_atualizacao) & (col("cd_atendimento").isNotNull())). \
withColumn("dt_termino_atendimento",
           when(col("cd_tipo_situacao_atendimento").isin(2,3) & col("dt_termino").isNull(), col("dt_atualizacao")).\
           otherwise(col("dt_termino"))).\
drop("dt_termino").\
dropDuplicates()

# COMMAND ----------

# If there's no new data, then just let it die.
if new_data.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Drop records that didn't change cd_tipo_situacao_atendimento and dt_termino for each cd_atendimento compared to the last record.
# MAGIC It's important to avoid versioning records that don't have true updates.

# COMMAND ----------

hash_columns = ["cd_atendimento", "cd_tipo_situacao_atendimento", "dt_inicio", "dt_termino_atendimento", "fl_origem_recurso_mercado", "fl_origem_recurso_senai", "fl_origem_recurso_fomento", "fl_origem_recurso_outrasent", "fl_atendimento_rede", "fl_excluido"]
var_key = ["cd_atendimento"]
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

new_data = new_data.withColumn("row_hash", concat_ws("@", *hash_columns))\
.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dt_atualizacao")))).dropDuplicates()\
.withColumn("delete", when(col("row_hash") == col("lag_row_hash"), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop("delete", "row_hash", "lag_row_hash")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Schema preparation for versioning.
# MAGIC 
# MAGIC For the new records available in raw:
# MAGIC 
# MAGIC * fl_corrente = 1
# MAGIC * dh_inicio_vigencia = dt_atualizacao
# MAGIC * dh_insercao_trs = timestamp.now()

# COMMAND ----------

w = Window.partitionBy('cd_atendimento')
new_data = new_data.select('*', count('cd_atendimento').over(w).alias('count_cd'))

# COMMAND ----------

new_data = new_data\
.withColumn("dt_lead", lead("dt_atualizacao").over(w.orderBy("dt_atualizacao")))\
.withColumnRenamed("dt_atualizacao", "dh_inicio_vigencia")\
.withColumn("dh_fim_vigencia", when(col("count_cd") == 1, lit(None).cast("timestamp")).otherwise(col("dt_lead")))\
.withColumn("fl_corrente", when(col("dt_lead").isNull(), lit(1).cast("int")).otherwise(lit(0).cast("int")))\
.withColumn("cd_ano_inicio_vigencia", year(col("dh_inicio_vigencia")))\
.drop("dt_lead", "count_cd"). \
dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we just need some make-up to get <i>new_data</i> ready to go and finally get to versioning logic.
# MAGIC As some issues were trated before, only a few columns need to be renamed.

# COMMAND ----------

column_name_list = ["fl_origem_recurso_mercado", "fl_origem_recurso_senai", "fl_origem_recurso_fomento", "fl_origem_recurso_outrasent", "fl_atendimento_rede", "fl_excluido"]

for item in column_name_list:
  new_data = new_data.withColumn(item, when(trim(lower(col(item))) == "n", 0).when(trim(lower(col(item))) == "s", 1).otherwise(lit(None).cast("int")))

# COMMAND ----------

column_name_mapping = {"cd_atendimento": "cd_atendimento_sti", 
                       "cd_tipo_situacao_atendimento": "cd_tipo_situacao_atendimento_sti", 
                       "dt_inicio": "dt_inicio_atendimento",
                       "fl_excluido": "fl_excluido_oltp"
                      }
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_atendimento_sti": "int", 
                   "cd_tipo_situacao_atendimento_sti": "int", 
                   "dh_inicio_vigencia": "timestamp", 
                   "dh_fim_vigencia": "timestamp", 
                   "dt_inicio_atendimento": "date",
                   "dt_termino_atendimento": "timestamp",
                   "fl_origem_recurso_mercado": "int",
                   "fl_origem_recurso_senai": "int",
                   "fl_origem_recurso_fomento": "int",
                   "fl_origem_recurso_outrasent": "int",
                   "fl_atendimento_rede": "int",
                   "fl_excluido_oltp": "int",
                   "fl_corrente": "int"
                  }

for c in column_type_map:
  new_data = new_data.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

#new_data.count(), new_data.select(concat_ws("@", *["cd_atendimento_sti", "dh_inicio_vigencia"])).dropDuplicates().count(), new_data.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Things are checked and done if it is the first load.

# COMMAND ----------

if first_load is True:
  # Partitioning as specified by the documentation. repartition(5) is a good parameter since we avoid to write many small files in ADLS
  new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")
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
# MAGIC /* Versionar se e somente se houve alteração em cd_tipo_situacao_atendimento */
# MAGIC 
# MAGIC Para cada dt_atualizacao encontrada
# MAGIC     Verifica se existe na trs atendimento_tecnologia_inovacao_situacao o raw.cd_atendimento = trs.cd_atendimento_sti
# MAGIC         Se existe 
# MAGIC             Se houve alteração: raw.cd_tipo_situacao_atendimento <> trs.cd_tipo_situacao_sti
# MAGIC                                 or raw.dt_termino <> trs.dt_termino_atendimento  
# MAGIC                                 or raw.dt_inicio <> trs.dt_inicio_atendimento
# MAGIC                                 or trs.fl_origem_recurso_mercado <> raw.fl_origem_recurso_mercado
# MAGIC                                 or trs.fl_origem_recurso_senai <> raw.fl_origem_recurso_senai
# MAGIC                                 or trs.fl_origem_recurso_fomento <> raw.fl_origem_recurso_fomento
# MAGIC                                 or trs.fl_origem_recurso_outrasent <> raw.fl_origem_recurso_outrasent
# MAGIC                                 or trs.fl_atendimento_rede <> raw.fl_atendimento_rede
# MAGIC                                 or trs.fl_excluido <> raw.fl_excluido 
# MAGIC                UPDATE trs atendimento_tecnologia_inovacao_situacao (existente)
# MAGIC                     set trs.dh_fim_vigencia = raw.dt_atualizacao, trs.fl_corrente = 0
# MAGIC                INSERT trs atendimento_tecnologia_inovacao_situacao (nova)	
# MAGIC                     trs.cd_atendimento_sti = raw.cd_atendimento
# MAGIC                     trs.cd_tipo_situacao_atendimento_sti = raw.cd_tipo_situacao_atendimento
# MAGIC                     trs.dt_inicio_atendimento = raw.dt_inicio
# MAGIC                     trs.dh_inicio_vigencia = raw.dt_atualizacao
# MAGIC                     trs.dh_fim_vigencia = NULL
# MAGIC                     trs.dt_termino_atendimento = raw.dt_termino
# MAGIC                     trs.fl_origem_recurso_mercado = raw.fl_origem_recurso_mercado
# MAGIC                     trs.fl_origem_recurso_senai = raw.fl_origem_recurso_senai
# MAGIC                     trs.fl_origem_recurso_fomento = raw.fl_origem_recurso_fomento
# MAGIC                     trs.fl_origem_recurso_outrasent = raw.fl_origem_recurso_outrasent
# MAGIC                     trs.fl_atendimento_rede = raw.fl_atendimento_rede
# MAGIC                     trs.fl_excluido = raw.fl_excluido
# MAGIC                     trs.fl_corrente = 1
# MAGIC             Senão, NÃO houve alteração: IGNORAR, nada a fazer
# MAGIC         Senão, NÃO existe: INSERT trs atendimento_tecnologia_inovacao_situacao (nova)	
# MAGIC </pre>

# COMMAND ----------

var_key = list(["cd_atendimento_sti"])
var_window = Window.partitionBy(*var_key)

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = trusted_data.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available and active trusted data that needs to be compared to the new data. This will also lead to what records need to be updated in the next operation.
old_data_to_compare = old_data_active. \
join(new_data.select(var_key).dropDuplicates(),var_key, "inner"). \
withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active. \
join(new_data.select(var_key).dropDuplicates(),var_key, "leftanti") 

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data; and also check if things have changed. If date is the change, then drop these records, if they are NEW!
# After the previous step, for the case where values change, then the most recent has one fl_corrente = 1, all the others fl_corrente = 0. And we have to close "dh_fim_vigencia"

new_data = new_data.withColumn("is_new", lit(1).cast("int"))

# COMMAND ----------

new_data = new_data.union(old_data_to_compare.select(new_data.columns))

# COMMAND ----------

hash_columns = var_key + ["cd_tipo_situacao_atendimento_sti", "dt_inicio_atendimento", "dt_termino_atendimento","fl_origem_recurso_mercado", "fl_origem_recurso_senai", "fl_origem_recurso_fomento", "fl_origem_recurso_outrasent", "fl_atendimento_rede", "fl_excluido_oltp"]

new_data = new_data.withColumn("row_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_row_hash", lag("row_hash", 1).over(var_window.orderBy(asc("dh_inicio_vigencia")))).dropDuplicates()

# COMMAND ----------

#display(new_data)

# COMMAND ----------

# For records where "is_new" = 1, when row_hash != lag_row_hash, we keep it. Otherwise, we drop it cause nothing changed! 
# Then Remove all recordds marked "delete" = 1, these are the ones in which inly date has changed.

new_data = new_data\
.withColumn("delete", when((col("row_hash") == col("lag_row_hash")) & (col("is_new") == 1), 1).otherwise(0))\
.filter(col("delete") == 0)\
.drop(*["delete", "row_hash", "lag_row_hash", "is_new"])

# COMMAND ----------

new_data = new_data.withColumn("lead_dh_inicio_vigencia", lead("dh_inicio_vigencia", 1).over(var_window.orderBy(asc("dh_inicio_vigencia"))))

# COMMAND ----------

# When lead_dh_inicio_vigencia is not NULL, then we must close the period with "lead_dh_inicio_vigencia"
# Also, when it happens, col(fl_corrente) = 0. All these records until now are active records col(fl_corrente) = 1.

new_data = new_data. \
withColumn("dh_fim_vigencia", when(col("lead_dh_inicio_vigencia").isNotNull(), col("lead_dh_inicio_vigencia"))). \
withColumn("fl_corrente", when(col("lead_dh_inicio_vigencia").isNotNull(), lit(0)).otherwise(lit(1))). \
drop("lead_dh_inicio_vigencia")

# COMMAND ----------

#display(new_data.filter(col("cd_atendimento_sti").isin([4640972,8009657,5662051,5662062,5662095,5662102])))

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"

old_data_inactive_to_rewrite = trusted_data. \
filter(col("fl_corrente") == 0). \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite. \
join(new_data.select("cd_ano_inicio_vigencia").dropDuplicates(), ["cd_ano_inicio_vigencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns))

# COMMAND ----------

#new_data.select("cd_atendimento_sti", "dh_inicio_vigencia").groupBy("cd_atendimento_sti", "dh_inicio_vigencia").count().filter(col("count")%2 != 1).show(20,False) 

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(5).write.partitionBy("fl_corrente", "cd_ano_inicio_vigencia").save(path=sink, format="parquet", mode="overwrite")