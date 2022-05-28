# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type versioning (insert + update)
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_pessoa_juridica_caracteristica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/bd_basi/tb_pessoa_juridica
# MAGIC Tabela/Arquivo Destino	/trs/mtd/senai/pessoa_juridica_caracteristica
# MAGIC Particionamento Tabela/Arquivo Destino	TBD
# MAGIC Descrição Tabela/Arquivo Destino	Registra as informações evolutivas no tempo sobre as características das pessoas juridicas O período de vigência de cada versão do registro deve ser obtida como >= data de inicio e < = que data de fim e a posição atual com versão corrente informada = 1 OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), pessoa_juridica
# MAGIC Tipo Atualização	V = versionamento (insert + update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou versiona o registro caso a chave cd_pessoa seja encontrada e alguma informação tenha sido alterada, incluindo um novo registro, setando-o como o corrente e atualizando o existente com o encerramento de sua vigência
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC </pre>
# MAGIC 
# MAGIC Dev: Marcela

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

import json
import re

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

#USE HIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/bd_basi/tb_pessoa_juridica"],"destination": "/mtd/senai/pessoa_juridica_caracteristica"}

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
       'adf_pipeline_name': 'raw_trs_pessoa_juridica_caracteristica',
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

src_tb = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_tb)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import lit, asc, desc, col, when, dense_rank, count, from_utc_timestamp, current_timestamp, max,from_unixtime, unix_timestamp, min, lead, date_sub, to_date
from pyspark.sql import Window
from pyspark.sql.types import LongType, IntegerType, DateType
import datetime
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Max date from dh_ultima_atualizacao_oltp from last load

# COMMAND ----------

var_max_dh_ultima_atualizacao_oltp=None
first_load=False
try:
  df_sink = spark.read.parquet(sink)
  var_max_dh_ultima_atualizacao_oltp = df_sink.select(from_unixtime(unix_timestamp(max(col("dt_inicio_vigencia")),'yyyy-MM-dd HH:mm:ss'))).collect()[0][0]
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))  
except:
  var_max_dh_ultima_atualizacao_oltp= '1990-01-01 00:00:00'
  first_load=True
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting records from raw rl_atendimento_metrica

# COMMAND ----------

# MAGIC %md
# MAGIC Rule to filter raw records:
# MAGIC <pre>
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp ORDER BY a.dt_atualizacao
# MAGIC </pre>

# COMMAND ----------

useful_columns = ["cd_pessoa", "cd_subclasse_cnae", "cd_versao_cnae", "cd_porte", "fl_simples", "fl_matriz", "fl_estrangeira", "fl_contribuinte", "fl_industria_cnae", "fl_industria_fpas", "qt_empregado", "fl_industria", "cd_segmento_nivel_3", "dt_atualizacao"]

df_raw = spark.read.parquet(src_tb)\
.select(*useful_columns)\
.filter(col("DT_ATUALIZACAO")>var_max_dh_ultima_atualizacao_oltp)\
.orderBy(col("DT_ATUALIZACAO"))

# COMMAND ----------

# If there's no new data, then just let it die.
if df_raw.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC Rule to select raw records:
# MAGIC <pre>
# MAGIC SELECT
# MAGIC cd_pessoa, cd_subclasse_cnae, cd_versao_cnae, cd_porte, fl_simples, fl_matriz, fl_estrangeira, fl_contribuinte, fl_industria_cnae, fl_industria_fpas, qt_empregado,
# MAGIC fl_industria, cd_segmento_nivel_3 FROM tb_pessoa_juridica
# MAGIC </pre>

# COMMAND ----------

df_raw = df_raw.withColumnRenamed("cd_pessoa", "cd_pessoa_juridica").withColumnRenamed("dt_atualizacao", "dt_inicio_vigencia")

# COMMAND ----------

df_raw = df_raw.withColumn("dt_fim_vigencia", lit(None))

# COMMAND ----------

df_raw = df_raw.withColumn("fl_corrente", lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC Removed from raw records that do not correspond to an update, because metric fields remained the same.

# COMMAND ----------

w = Window.partitionBy("cd_pessoa_juridica", "cd_subclasse_cnae", "cd_versao_cnae", "cd_porte", "fl_simples","fl_matriz", "fl_estrangeira", "fl_contribuinte", "fl_industria_cnae", "fl_industria_fpas", "qt_empregado", "fl_industria", "cd_segmento_nivel_3").orderBy(asc("dt_inicio_vigencia"))
df_raw = df_raw.withColumn("rank", dense_rank().over(w)).filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC When there is more than one update of the same record on the same day, consider only the last of the day and discard the rest.

# COMMAND ----------

w = Window.partitionBy('cd_pessoa_juridica', to_date(col("dt_inicio_vigencia"), 'yyyy-MM-dd'))
df_raw = df_raw.withColumn('max_dt_inicio_vigencia', max('dt_inicio_vigencia').over(w)) \
                     .filter(col('dt_inicio_vigencia') == col('max_dt_inicio_vigencia')) \
                     .drop('max_dt_inicio_vigencia')

# COMMAND ----------

# MAGIC %md
# MAGIC Add control column dh_insercao_trs.

# COMMAND ----------

#add control fields from trusted_control_field egg
df_raw = tcf.add_control_fields(df_raw, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC Turning to the correct types.

# COMMAND ----------

# MAGIC %md
# MAGIC fl_industria - Indica se a Empresa está enquadrada como Indústria pelo CNPJ (FPAS - 507), 1 - SIm ou 0 - Não

# COMMAND ----------

df_raw = df_raw.withColumn('fl_industria', when(col("fl_industria") == "S", lit(1)).otherwise(when(col("fl_industria") == "N", lit(0))))

# COMMAND ----------

column_name_mapping = {"fl_simples": "cd_indicador_optante_simples",
                      "fl_matriz": "cd_indicador_matriz",
                      "fl_estrangeira": "cd_indicador_estrangeira",
                      "fl_contribuinte": "cd_indicador_contribuinte_industria",
                      "fl_industria_cnae": "cd_indicador_industria_cnae",
                      "fl_industria_fpas": "cd_indicador_industria_fpas"}
  
for key in column_name_mapping:
  df_raw =  df_raw.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

column_type_map = {"cd_pessoa_juridica" : "int", 
                   "cd_subclasse_cnae" : "int", 
                   "cd_versao_cnae" : "int",
                   "cd_porte": "int", 
                   "cd_indicador_optante_simples" : "string", 
                   "cd_indicador_matriz" : "string", 
                   "cd_indicador_estrangeira" : "string", 
                   "cd_indicador_contribuinte_industria" : "string", 
                   "cd_indicador_industria_cnae" : "string", 
                   "cd_indicador_industria_fpas" : "string", 
                   "fl_industria": "int", "qt_empregado": "int" ,
                   "cd_segmento_nivel_3": "int", 
                   "dt_inicio_vigencia": "date", 
                   "dt_fim_vigencia": "date", 
                   "fl_corrente": "int",
                   "dh_insercao_trs" : "timestamp"}

for c in column_type_map:
  df_raw = df_raw.withColumn(c, df_raw[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC If it is the first load, create an empty DataFrame of the same schema that corresponds to sink

# COMMAND ----------

if first_load is True:
  df_sink = spark.createDataFrame([], schema=df_raw.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting records from trusted to update and records from raw to insert

# COMMAND ----------

df_aux = df_sink.select(col("cd_pessoa_juridica").alias("trs_cd_pessoa_juridica"),
                        col("cd_subclasse_cnae").alias("trs_cd_subclasse_cnae"),
                        col("cd_versao_cnae").alias("trs_cd_versao_cnae"),
                        col("cd_porte").alias("trs_cd_porte"),
                        col("cd_indicador_optante_simples").alias("trs_cd_indicador_optante_simples"),
                        col("cd_indicador_matriz").alias("trs_cd_indicador_matriz"),
                        col("cd_indicador_estrangeira").alias("trs_cd_indicador_estrangeira"),
                        col("cd_indicador_contribuinte_industria").alias("trs_cd_indicador_contribuinte_industria"),
                        col("cd_indicador_industria_cnae").alias("trs_cd_indicador_industria_cnae"),
                        col("cd_indicador_industria_fpas").alias("trs_cd_indicador_industria_fpas"),
                        col("fl_industria").alias("trs_fl_industria"),
                        col("qt_empregado").alias("trs_qt_empregado"),
                        col("cd_segmento_nivel_3").alias("trs_cd_segmento_nivel_3"),
                        col("dt_inicio_vigencia").alias("trs_dt_inicio_vigencia"),
                        col("dt_fim_vigencia").alias("trs_dt_fim_vigencia"),
                        col("fl_corrente").alias("trs_fl_corrente"),
                        col("dh_insercao_trs").alias("trs_dh_insercao_trs"),
                        col("kv_process_control").alias("trs_kv_process_control")
                       )

# COMMAND ----------

# MAGIC %md
# MAGIC Rule to evaluate whether raw records are new records or update records:
# MAGIC <pre>
# MAGIC FROM tb_pessoa_juridica rw
# MAGIC LEFT JOIN pessoa_juridica trs
# MAGIC ON (trs.cd_pessoa_juridica = rw.cd_pessoa) AND
# MAGIC     trs.fl_corrente = 1) 
# MAGIC <\pre>

# COMMAND ----------

df_raw = df_raw.join(df_aux,
                            (df_aux.trs_cd_pessoa_juridica == df_raw.cd_pessoa_juridica) &                            
                            (df_aux.trs_fl_corrente == 1),
                            how='left')

# COMMAND ----------

del df_aux

# COMMAND ----------

# MAGIC %md
# MAGIC #### After join:
# MAGIC <pre>
# MAGIC SE trs.cd_pessoa_juridica IS NULL, lançamento não existe 
# MAGIC    -- para o registro lido na raw
# MAGIC    INSERT no registro lido 
# MAGIC </pre>

# COMMAND ----------

df_insert = df_raw.filter(col("trs_cd_pessoa_juridica").isNull()).drop("trs_cd_pessoa_juridica","trs_cd_subclasse_cnae", "trs_cd_versao_cnae", "trs_cd_porte", "trs_cd_indicador_optante_simples", "trs_cd_indicador_matriz", "trs_cd_indicador_estrangeira", "trs_cd_indicador_contribuinte_industria", "trs_cd_indicador_industria_cnae", "trs_cd_indicador_industria_fpas", "trs_fl_industria", "trs_qt_empregado", "trs_cd_segmento_nivel_3", "trs_dt_inicio_vigencia", "trs_dt_fim_vigencia", "trs_fl_corrente", "trs_dh_insercao_trs", "trs_kv_process_control")

# COMMAND ----------

# MAGIC %md
# MAGIC #### After join:
# MAGIC <pre>
# MAGIC OU SE trs.cd_pessoa_juridica <> NULL, existe, mas houve alteraçãoem algum dos campos envolvidos no select então:
# MAGIC    UPDATE trs pessoa_juridica_caracteristica (existente)
# MAGIC                     set trs.dt_fim_vigencia = (raw.dt_atualizacao - 1 dia), trs.fl_corrente = 0
# MAGIC    INSERT trs pessoa_juridica_caracteristica (nova)
# MAGIC </pre>

# COMMAND ----------

df_update = df_raw.filter(col("trs_cd_pessoa_juridica").isNotNull() & 
                             ((col("cd_subclasse_cnae") != col("trs_cd_subclasse_cnae")) | 
                              (col("cd_versao_cnae") != col("trs_cd_versao_cnae")) | 
                              (col("cd_porte") != col("trs_cd_porte")) |
                              (col("cd_indicador_optante_simples") != col("trs_cd_indicador_optante_simples")) |
                              (col("cd_indicador_matriz") != col("trs_cd_indicador_matriz")) |
                              (col("cd_indicador_estrangeira") != col("trs_cd_indicador_estrangeira")) |
                              (col("cd_indicador_contribuinte_industria") != col("trs_cd_indicador_contribuinte_industria")) |
                              (col("cd_indicador_industria_cnae") != col("trs_cd_indicador_industria_cnae")) |
                              (col("cd_indicador_industria_fpas") != col("trs_cd_indicador_industria_fpas")) |
                              (col("qt_empregado") != col("trs_qt_empregado")) |
                              (col("fl_industria") != col("trs_fl_industria")) |
                              (col("cd_segmento_nivel_3") != col("trs_cd_segmento_nivel_3"))                              
                             ))

# COMMAND ----------

df_raw.unpersist()
del df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC Raw register, referring to the update register, to be inserted

# COMMAND ----------

df_insert_u = df_update.select(df_insert.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Raw records to be inserted into trusted is the sum of new records that are not yet released from trusted with replacement records for updates

# COMMAND ----------

df_insert = df_insert.union(df_insert_u)

# COMMAND ----------

del df_insert_u

# COMMAND ----------

# MAGIC %md
# MAGIC Treatment to identify the last update of the insert records that match the fl_corrente = 1.

# COMMAND ----------

df_insert = df_insert.withColumn("rank", dense_rank().over(Window.partitionBy("cd_pessoa_juridica").orderBy(desc("dt_inicio_vigencia"))))

# COMMAND ----------

df_insert = df_insert.withColumn("fl_corrente", when(col("rank") == 1, 1).otherwise(0))

# COMMAND ----------

df_insert = df_insert.drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC For non-current records (fl_current = 0), calculate the effective end date as the start date of the subsequent record minus one day.

# COMMAND ----------

window = Window.orderBy("cd_pessoa_juridica", "dt_inicio_vigencia")

# COMMAND ----------

leadCol = lead(col("dt_inicio_vigencia"), 1).over(window)

# COMMAND ----------

df_insert = df_insert.withColumn("dt_fim_vigencia", when(col("fl_corrente") == 0, date_sub(leadCol, 1)).otherwise(col("dt_fim_vigencia")))

# COMMAND ----------

# MAGIC %md
# MAGIC Trusted records that must be updated.

# COMMAND ----------

df_update = df_update.select(col("trs_cd_pessoa_juridica").alias("cd_pessoa_juridica"),
                        col("dt_inicio_vigencia").alias("raw_dt_inicio_vigencia"),
                        col("trs_cd_subclasse_cnae").alias("cd_subclasse_cnae"),
                        col("trs_cd_versao_cnae").alias("cd_versao_cnae"),
                        col("trs_cd_porte").alias("cd_porte"),
                        col("trs_cd_indicador_optante_simples").alias("cd_indicador_optante_simples"),
                        col("trs_cd_indicador_matriz").alias("cd_indicador_matriz"),
                        col("trs_cd_indicador_estrangeira").alias("cd_indicador_estrangeira"),
                        col("trs_cd_indicador_contribuinte_industria").alias("cd_indicador_contribuinte_industria"),
                        col("trs_cd_indicador_industria_cnae").alias("cd_indicador_industria_cnae"),
                        col("trs_cd_indicador_industria_fpas").alias("cd_indicador_industria_fpas"),
                        col("trs_fl_industria").alias("fl_industria"),
                        col("trs_qt_empregado").alias("qt_empregado"),
                        col("trs_cd_segmento_nivel_3").alias("cd_segmento_nivel_3"),
                        col("trs_dt_inicio_vigencia").alias("dt_inicio_vigencia"),
                        col("trs_dt_fim_vigencia").alias("dt_fim_vigencia"),
                        col("trs_fl_corrente").alias("fl_corrente"),
                        col("trs_dh_insercao_trs").alias("dh_insercao_trs"),
                        col("trs_kv_process_control").alias("kv_process_control")
                       )

# COMMAND ----------

# MAGIC %md
# MAGIC Equalizing schemes

# COMMAND ----------

columns = ["cd_pessoa_juridica",
           "cd_subclasse_cnae",
           "cd_versao_cnae",
           "cd_porte",
           "cd_indicador_optante_simples",
           "cd_indicador_matriz",
           "cd_indicador_estrangeira",
           "cd_indicador_contribuinte_industria",
           "cd_indicador_industria_cnae",
           "cd_indicador_industria_fpas",
           "fl_industria",
           "qt_empregado",                 
           "cd_segmento_nivel_3",
           "dt_inicio_vigencia",
           "dt_fim_vigencia",
           "fl_corrente",
           "dh_insercao_trs",
           "kv_process_control"]

# COMMAND ----------

df_sink = df_sink.select(*columns)

# COMMAND ----------

df_insert = df_insert.select(*columns)

# COMMAND ----------

columns_update = columns + ["raw_dt_inicio_vigencia"]

# COMMAND ----------

df_update = df_update.select(*columns_update)

# COMMAND ----------

# MAGIC %md
# MAGIC Subtracting the records that must be updated in trusted to after perform the transformation in df_update.

# COMMAND ----------

df_sink = df_sink.subtract(df_update.select(df_sink.columns).distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC When there is more than one update for the same trusted record, consider df_update only the record with the earliest date, as the other records are already being handled in df_insert_u.

# COMMAND ----------

df_update = df_update.groupBy(*columns).agg(min("raw_dt_inicio_vigencia").alias("raw_dt_inicio_vigencia"))

# COMMAND ----------

df_update = df_update.withColumn('fl_corrente', lit(0).cast(IntegerType()))

# COMMAND ----------

df_update = df_update.withColumn('dt_fim_vigencia', date_sub('raw_dt_inicio_vigencia', 1)).drop("raw_dt_inicio_vigencia")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute records of sink

# COMMAND ----------

# MAGIC %md
# MAGIC The result will be the sum of the records already in trusted with the updated records and the records to be inserted.

# COMMAND ----------

df_sink = df_sink.union(df_update).union(df_insert)

# COMMAND ----------

#AS it is partitioned by year/month, case there's no changes in a specific year, we can skip it.
#period_with_change = df_insert.select("cd_ano_mes_referencia").distinct().collect()
#period_with_change = [p[0] for p in period_with_change]

# COMMAND ----------

del df_insert, df_update

# COMMAND ----------

#Easier now that we kept only years with some change. Now we can filter it.
#df_sink = df_sink.filter(df_sink["cd_ano_mes_referencia"].isin(period_with_change))

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated

# COMMAND ----------

#df_sink.repartition(4).write.partitionBy(["cd_ano_mes_referencia"]).save(path=sink, format="parquet", mode="overwrite")
df_sink.repartition(4).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_sink.unpersist()

# COMMAND ----------

