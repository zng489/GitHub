# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_lancamento_servico_saude_seguranca_metrica
# MAGIC Tabela/Arquivo Origem	/raw/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi
# MAGIC Tabela/Arquivo Destino	/trs/evt/raw_trs_lancamento_servico_saude_seguranca_metrica
# MAGIC Particionamento Tabela/Arquivo Destino	
# MAGIC Descrição Tabela/Arquivo Destino	Cadastro das métricas apuradas no lançamento de saúde e segurança do trabalhador.
# MAGIC Tipo Atualização	A = append (insert)
# MAGIC Detalhe Atualização	Conforme documento anexo nas regras de leitura/Query Base
# MAGIC Periodicidade/Horario Execução	Diária, após a carga da raw inddesempenho
# MAGIC 
# MAGIC Dev: Marcela Crozara
# MAGIC   -Manutenção: Tiago Shin (29/06/2020)
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
var_tables =  {"origins": ["/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi",
                          "/bdo/inddesempenho/variavel"],
               "destination": "/evt/lancamento_servico_saude_seguranca_metrica"}

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
       'adf_pipeline_name': 'raw_trs_lancamento_saude_seguranca_metrica',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-06-03T16:00:00.0000000Z',
       'adf_trigger_type': 'Manual'
      }
      
"""      

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

source_lanc_evento, source_variavel = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(source_lanc_evento, source_variavel)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, current_date, dense_rank, asc, desc, greatest, trim, substring, when, lit, col, concat_ws, lead, lag, row_number, sum, date_format, round, lower
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from lancamento_saude_seguranca_metrica
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink).cache()
  var_max_dh_ultima_atualizacao_oltp = trusted_data.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

print("First load: {}".format(first_load))
print("Última atualização: {}".format(var_max_dh_ultima_atualizacao_oltp))

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 1: Obtém os registros de lancamento que tiveram métricas alteradas, ordenados para data
# MAGIC de inclusão na Raw. Segue exemplo como resultado:
# MAGIC 
# MAGIC ---- Verificar se houve alteração no campo vl_metrica através do filtro id_valor_lancamento
# MAGIC SELECT DISTINCT
# MAGIC 	 vw_lanc_evento_vl_r_ssi.id_valor_lancamento id_valor_lancamento_oltp
# MAGIC    	,vw_lanc_evento_vl_r_ssi.id_filtro_lancamento id_filtro_lancamento_oltp
# MAGIC     ,vw_lanc_evento_vl_r_ssi.cd_mes    --- incluido 26/05
# MAGIC     ,vw_lanc_evento_vl_r_ssi.cd_ciclo_mes --- incluido 26/05
# MAGIC     ,vw_lanc_evento_vl_r_ssi.id_clientela   --- incluido 04/12
# MAGIC /* Alterado em 26/06/2020  */
# MAGIC 	,variavel.id id_metrica
# MAGIC 	,when 
# MAGIC 	     Case variavel.id = 155 then "qt_consulta" 
# MAGIC 	     Case variavel.id = 8050 then "qt_empresa_atendida" 
# MAGIC 	     Case variavel.id = 159 then "qt_exame" 
# MAGIC 	     Case variavel.id = 101 then "qt_hora_tecnica" 
# MAGIC 	     Case variavel.id = 102 then "qt_pessoa_atendida" 
# MAGIC 	     Case variavel.id = 1054 then "qt_pessoa_beneficiada_contrato" 
# MAGIC 	     Case variavel.id = 157 then "qt_procedimento_enfermagem" 
# MAGIC 	     Case variavel.id = 158 then "qt_procedimento_reabilitacao" 
# MAGIC 	     Case variavel.id = 11050 then "qt_produto_desenvolvido" 
# MAGIC 	     Case variavel.id = 11051 then "qt_projeto_desenvolvido" 
# MAGIC 	     Case variavel.id = 162 then "qt_vacina_aplicada" 
# MAGIC 	     Case variavel.id = 14054 then "qt_vida_ativa"  as cd_metrica
# MAGIC /* Alterado em 26/06/2020  */
# MAGIC 
# MAGIC 	,variavel.nome nm_metrica
# MAGIC     ,CASE WHEN vw_lanc_evento_vl_r_ssi.fl_excluido = 1 THEN 0 ELSE vw_lanc_evento_vl_r_ssi.vl_lancamento_evento END as vl_metrica
# MAGIC 
# MAGIC FROM vw_lanc_evento_vl_r_ssi
# MAGIC 
# MAGIC INNER JOIN variavel 
# MAGIC ON vw_lanc_evento_vl_r_ssi.id_unidadedecontrole = variavel.id 
# MAGIC 
# MAGIC WHERE vw_lanc_evento_vl_r_ssi.dh_insercao_raw  > #var_max_dh_ultima_atualizacao_oltp
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

lanc_evento_columns = ["id_valor_lancamento", "id_filtro_lancamento", "vl_lancamento_evento", "id_unidadedecontrole", "dh_insercao_raw", "fl_excluido", "cd_mes", "cd_ciclo_ano", "id_clientela"]
df_lanc_evento = spark.read.parquet(source_lanc_evento).select(*lanc_evento_columns).\
filter(col("dh_insercao_raw")> var_max_dh_ultima_atualizacao_oltp).\
withColumnRenamed("id_unidadedecontrole", "id").\
withColumn("cd_mes", col("cd_mes") + 1)

# COMMAND ----------

#df_lanc_evento.count() 
#287155
#173

# COMMAND ----------

# If there's no new data, then just let it die.
if df_lanc_evento.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

variavel_columns = ["id", "nome"]
df_variavel = spark.read.parquet(source_variavel).select(*variavel_columns)

# COMMAND ----------

#df_variavel.count() 
#105
#105

# COMMAND ----------

new_data = df_lanc_evento.join(df_variavel, ["id"], "inner").\
dropDuplicates().\
withColumn("vl_lancamento_evento", when(col("fl_excluido") == 1, 0).otherwise(col("vl_lancamento_evento")))

# COMMAND ----------

#new_data.count() 
#287155
#173

# COMMAND ----------

del df_lanc_evento, df_variavel

# COMMAND ----------

new_data = new_data.withColumn("dh_referencia", col("dh_insercao_raw"))

# COMMAND ----------

new_data = new_data.withColumn("cd_metrica", 
                               when(col("id") == 155, "qt_consulta"). \
                               when(col("id") == 8050, "qt_empresa_atendida"). \
                               when(col("id") == 159, "qt_exame"). \
                               when(col("id") == 101, "qt_hora_tecnica"). \
                               when(col("id") == 102, "qt_pessoa_atendida"). \
                               when(col("id") == 1054, "qt_pessoa_beneficiada_contrato"). \
                               when(col("id") == 157, "qt_procedimento_enfermagem"). \
                               when(col("id") == 158, "qt_procedimento_reabilitacao"). \
                               when(col("id") == 11050, "qt_produto_desenvolvido"). \
                               when(col("id") == 11051, "qt_projeto_desenvolvido"). \
                               when(col("id") == 162, "qt_vacina_aplicada"). \
                               when(col("id") == 14054, "qt_vida_ativa"). \
                               otherwise(lit(None).cast("string")))

# COMMAND ----------

column_name_mapping = {"id": "id_metrica",
                       "id_valor_lancamento": "id_valor_lancamento_oltp",
                       "id_filtro_lancamento": "id_filtro_lancamento_oltp",
                       "vl_lancamento_evento": "vl_metrica",
                       "dh_insercao_raw": "dh_ultima_atualizacao_oltp",
                       "cd_mes": "cd_mes_referencia",
                       "cd_ciclo_ano": "cd_ano_referencia",
                       "id_clientela": "cd_clientela_oltp",
                       "nome": "nm_metrica"}
  
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC There's one column that needs to be added cause it might change according to what happens on versioning. 
# MAGIC 
# MAGIC Until this moment, we're dealing with new records, so this column will follow the natural implementation for "cd_movimento" = "E"
# MAGIC 
# MAGIC <pre>
# MAGIC Tabela Origem: vw_lanc_evento_vl_r_ssi
# MAGIC Campo Origem: dh_insercao_raw
# MAGIC Transformação: Mapeamento direto, a maior dentre os registros de um id_valor_lancamento
# MAGIC Campo Destino: dh_referencia 	
# MAGIC Tipo (tamanho): timestamp	
# MAGIC Descrição: Data de Atualizaçao do Registro, usada como data de referência quando há revisão de valores
# MAGIC 
# MAGIC </pre>
# MAGIC 					

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"id_valor_lancamento_oltp" : "bigint",                    
                   "id_filtro_lancamento_oltp": "bigint",
                   "cd_mes_referencia": "int",
                   "cd_ano_referencia": "int",
                   "cd_clientela_oltp": "int",
                   "id_metrica": "int",
                   "cd_metrica": "string",
                   "nm_metrica": "string",
                   "vl_metrica": "decimal(18,6)",
                   "dh_referencia": "timestamp",
                   "dh_ultima_atualizacao_oltp": "timestamp" }

for c in column_type_map:
  new_data = new_data.withColumn(c, new_data[c].cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Add for all records coming from raw:
# MAGIC * cd_movimento = "E"
# MAGIC * fl_corrente = 1

# COMMAND ----------

new_data = new_data.withColumn("cd_movimento", lit("E").cast("string")) \
                   .withColumn("fl_corrente", lit(1).cast("int"))

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

if first_load is True:
  # If it is the first load, create an empty DataFrame of the same schema that corresponds to trusted_data
  trusted_data = spark.createDataFrame([], schema=new_data.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Versioning
# MAGIC 
# MAGIC From all the old data available in trusted, the intersection of *keys_from_new_data* and *trusted_data* ACTIVE RECORDS will give us what has changed. Only active records can change!
# MAGIC 
# MAGIC What changes in old_data_to_change is that fl_corrente = 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Magic words:
# MAGIC 
# MAGIC <pre>
# MAGIC  Chave de comparação de todas as coisas! ["id_valor_lancamento_oltp, "id_metrica"]
# MAGIC 
# MAGIC  Tudo o que vem de raw é ENTRADA, tudo!
# MAGIC  
# MAGIC  Tudo o que tem na trusted antiga e também tem na raw lida (agorinha), deve gerar um novo registro com os dados duplicados da 
# MAGIC trusted mudando:
# MAGIC    cd_movimento = 'S'
# MAGIC    qts e nums, * -1
# MAGIC    dh_referencia = CM_dh_ultima_atualizacao_oltp(raw)
# MAGIC    fl_corrente = 0
# MAGIC 
# MAGIC 
# MAGIC Depois de tudo isso: o registro antigo, da trusted, com cd_movimento = 'E', sofre update para fl_corrente = 0
# MAGIC </pre>

# COMMAND ----------

# We filter active records from the trusted layer
old_data_active = trusted_data.filter(col("fl_corrente") == 1)

# COMMAND ----------

# Just getting what was already available on trusted that needs to append balance. This will also lead to what records need to be updated in the next operation.
old_data_to_balance = old_data_active.join(new_data.select("id_valor_lancamento_oltp", "id_metrica").dropDuplicates(), ["id_valor_lancamento_oltp", "id_metrica"], "inner")

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active.join(new_data.select("id_valor_lancamento_oltp", "id_metrica").dropDuplicates(), ["id_valor_lancamento_oltp", "id_metrica"], "leftanti") 

# COMMAND ----------

# MAGIC %md 
# MAGIC Now there's the need to implement this statement:
# MAGIC 
# MAGIC na verdade se o registro que está na raw é exatamente igual ao que está na trusted, exceto pela data de atualização, então nada a fazer
# MAGIC 
# MAGIC This might mean that we'll have to drop this record from old_data_to_balance and also from new_data.
# MAGIC 
# MAGIC Still to be implemented.

# COMMAND ----------

# Right, so we've gotta check all the occurencies between the old active data and the new data and also check if things have changed. If date is the only thing that has changed, then we can drop these records if they are NEW!
# After the previous step, for the case where values change, then the most recent has fl_corrente = 1, all the others fl_corrente = 0. All the others will have to be balanced with a new records with cd_movimento = "S" and hab=ve the values inverted. Also, dh_ultima_atualizacao_oltp will be the greatest one, the one for the active record. 

new_data = new_data.withColumn("is_new", lit(1).cast("int"))
old_data_to_balance = old_data_to_balance.withColumn("is_new", lit(0).cast("int")).\
withColumn("fl_excluido", lit(0).cast("int"))

# COMMAND ----------

new_data = new_data.union(old_data_to_balance.select(new_data.columns))

# COMMAND ----------

hash_columns = ["id_valor_lancamento_oltp", "id_metrica", "nm_metrica", "vl_metrica", "cd_clientela_oltp"]
new_data = new_data.withColumn("qt_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_qt_hash", lag("qt_hash", 1).over(Window.partitionBy("id_valor_lancamento_oltp", "id_metrica").orderBy(asc("dh_referencia"))))

# COMMAND ----------

# For records where "is_new" = 1, when qt_hash != lag_qt_hash, we keep it. Otherwise, we drop it cause nothing changed! 
new_data = new_data.withColumn("delete", when((col("qt_hash") == col("lag_qt_hash")) & (col("is_new") == 1), 1).otherwise(0))

# COMMAND ----------

# Remove all recrods marked "delete" = 1. Also, we don't need this column anymore.
new_data= new_data.filter(col("delete") == 0).drop(*["delete", "qt_hash", "lag_qt_hash"])

# COMMAND ----------

# Now, case we've got more than one record to add, we've go to balance all the old one from trusted and also the intermediate ones. But the most recent is kept as "cd_movimento" = "E", no balance applied to it. 
new_data = new_data.withColumn("row_number", row_number().over(Window.partitionBy("id_valor_lancamento_oltp", "id_metrica").orderBy(asc("dh_referencia"), desc("fl_excluido"))))

# COMMAND ----------

# Now I'll just have to add who's max("row_number") so it's possible to know what records are old and, hwne generating the balanced ones - except for the newest one - I can also update dh_referencia.
new_data = new_data.withColumn("max_row_number", max("row_number").over(Window.partitionBy("id_valor_lancamento_oltp", "id_metrica")))
# Counting after, no records are lost

# COMMAND ----------

new_data = new_data.withColumn("lead_dh_ultima_atualizacao_oltp", lead("dh_ultima_atualizacao_oltp", 1).over(Window.partitionBy("id_valor_lancamento_oltp", "id_metrica").orderBy(asc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

balanced_records = new_data.filter((col("row_number") != col("max_row_number")) & (col("fl_excluido") != 1)) \
                           .withColumn("cd_movimento", lit("S")) \
                           .withColumn("fl_corrente", lit(0).cast("int")) \
                           .withColumn("dh_referencia", col("lead_dh_ultima_atualizacao_oltp")) 

# COMMAND ----------

#add control fields from trusted_control_field egg
balanced_records = tcf.add_control_fields(balanced_records, var_adf)

# COMMAND ----------

balanced_records = balanced_records.withColumn("vl_metrica", col("vl_metrica") * (-1))

# COMMAND ----------

#balanced_records.count()
#0
#4

# COMMAND ----------

# Now for nd, the old active records and the new records, we apply:
new_data = new_data.withColumn("fl_corrente", when(col("max_row_number") != col("row_number"), 0).otherwise(1))

# COMMAND ----------

# remember that we are overwriting the whole partition
new_data = new_data.union(balanced_records.select(new_data.columns)).drop(*["is_new", "row_number", "max_row_number", "lead_dh_ultima_atualizacao_oltp", "fl_excluido"])

# COMMAND ----------

#new_data.count()
#287155
#181

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "cd_ano_referencia" and "cd_mes_referencia"
old_data_inactive_to_rewrite = trusted_data.filter(col("fl_corrente") == 0).join(new_data.select("cd_ano_referencia", "cd_mes_referencia").dropDuplicates(), ["cd_ano_referencia", "cd_mes_referencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite.join(new_data.select("cd_ano_referencia", "cd_mes_referencia").dropDuplicates(), ["cd_ano_referencia", "cd_mes_referencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite.select(old_data_inactive_to_rewrite.columns))

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns)).orderBy(asc("id_filtro_lancamento_oltp"), asc("id_valor_lancamento_oltp"), asc("id_metrica"), asc("dh_ultima_atualizacao_oltp"), asc("dh_referencia"))

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(5).write.partitionBy("cd_ano_referencia", "cd_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

trusted_data.unpersist()

# COMMAND ----------

