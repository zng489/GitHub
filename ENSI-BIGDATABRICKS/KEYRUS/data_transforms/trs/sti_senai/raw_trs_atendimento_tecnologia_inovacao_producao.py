# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_atendimento_tecnologia_informacao_producao
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/bd_basi/tb_atendimento
# MAGIC /raw/bdo/bd_basi/rl_atendimento_metrica"
# MAGIC Tabela/Arquivo Destino	/trs/evt/atendimento_tecnologia_inovacao_producao
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_mes_referencia
# MAGIC Descrição Tabela/Arquivo Destino	"Lançamentos mensais dos valores e quantidades de serviços de tecnologia e inovação atendidos, informados pelas regionais do SENAI. Podem ser eventos do tipo ""E"", entrada quando um novo lançamento chega, ou do tipo ""S"", saída, quando um lançamento é substituído, então um registro idêntico ao da entrada correspondente é lançado com sinal invertido para a data de referência de sua substituição, mantendo assim , um saldo ao somar valores.
# MAGIC OBS: as informações relativas à identificação (informações estáveis) desta entidade de negócio são tratadas em tabela central (HUB), producao_atendimento_tecnologia_inovacao"
# MAGIC Tipo Atualização	A = append (insert)
# MAGIC Detalhe Atualização	Carga incremental, quando existe um novo lancamento para o ano/mês ou um lançamento existente teve seu valor alterado.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Marcela Crozara
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
var_tables =  {"origins": ["/bdo/bd_basi/rl_atendimento_metrica",
                          "/bdo/bd_basi/tb_atendimento"], 
               "destination": "/evt/atendimento_tecnologia_inovacao_producao"}

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
       'adf_pipeline_name': 'raw_trs_atendimento_tecnologia_inovacao_producao',
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

source_atendimento_metrica, source_atendimento = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(source_atendimento_metrica, source_atendimento)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, current_date, dense_rank, asc, desc, greatest, trim, substring, when, lit, col, concat_ws, lead, lag, row_number, sum, date_format
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")
var_max_dh_insercao_raw = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dt_referencia da atendimento_tecnologia_inovacao_producao
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink).cache()
  var_max_dh_ultima_atualizacao_oltp = trusted_data.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
  var_max_dh_insercao_raw = trusted_data.select(max(col("dh_insercao_trs"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 1: Obtém os registros de atendimentos/mes que tiveram métricas alteradas, ordenados para data
# MAGIC de inclusão na Raw. Segue exemplo como resultado:
# MAGIC 
# MAGIC SELECT 
# MAGIC    r.cd_atendimento as cd_atendimento_matricula_ensino_prof,
# MAGIC    r.cd_metrica_atendimento,
# MAGIC    CASE WHEN r.fl_excluido = 'S' THEN 0 ELSE r.vl_valor_medido END as vl_valor_medido,
# MAGIC    r.dt_atualizacao
# MAGIC    (YEAR(rw.dt_ano_mes_referencia)*100)+MONTH(dt_ano_mes_referencia)+DAY(dt_ano_mes_referencia) as dt_producao,
# MAGIC    dh_insercao_raw
# MAGIC 
# MAGIC FROM rl_atendimento_metrica r
# MAGIC 
# MAGIC INNER JOIN 
# MAGIC    -- Somente Convênios em Ensino Profissional
# MAGIC    (SELECT DISTINCT cd_atendimento FROM tb_atendimento WHERE cd_tipo_atendimento = 4)
# MAGIC    ON r.cd_atendimento = a.cd_atendimento
# MAGIC    
# MAGIC WHERE dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp
# MAGIC ORDER BY cd_atendimento, dt_producao, dh_insercao_raw, dt_atualizacao
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

atendimento_metrica_columns = ["cd_atendimento", "dt_ano_mes_referencia", "dt_atualizacao", "cd_metrica_atendimento", "vl_valor_medido", "fl_excluido", "dh_insercao_raw"]
atendimento_metrica = spark.read.parquet(source_atendimento_metrica).select(*atendimento_metrica_columns)
atendimento_metrica = atendimento_metrica.withColumn("dt_producao", to_date("dt_ano_mes_referencia", "yyyy-MM-dd hh:mm:ss"))
atendimento_metrica = atendimento_metrica.drop("dt_ano_mes_referencia")
atendimento_metrica = atendimento_metrica.filter(col("DT_ATUALIZACAO")>var_max_dh_ultima_atualizacao_oltp) \
                                            .orderBy("cd_atendimento", "dt_producao", "dh_insercao_raw", "dt_atualizacao")

# COMMAND ----------

# If there's no new data, then just let it die.
if atendimento_metrica.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

atendimento_columns = ["cd_atendimento", "cd_tipo_atendimento"]
atendimento = spark.read.parquet(source_atendimento).select(*atendimento_columns).filter(col("cd_tipo_atendimento")==4).dropDuplicates().drop("cd_tipo_atendimento")

new_data = atendimento_metrica.join(atendimento, ["cd_atendimento"], "inner")

# COMMAND ----------

new_data = new_data.filter((col("cd_metrica_atendimento")>=436) & (col("cd_metrica_atendimento")<=442))

# COMMAND ----------

new_data = new_data.withColumn("vl_valor_medido", when(col("fl_excluido") == "S", 0).otherwise(col("vl_valor_medido")))

# COMMAND ----------

new_data = new_data.drop("fl_excluido")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 2: Rankear o atendimento afim de obter o registro de métrica com a data mais recente do mesmo ano / mês de referência, de um atendimento que tem a mesma data de inserção na raw.

# COMMAND ----------

new_data = new_data.withColumn("rank_metrica", dense_rank().over(Window.partitionBy("cd_atendimento", "dt_producao", "dh_insercao_raw","cd_metrica_atendimento").orderBy(desc("dt_atualizacao"))))

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 3: Selecionar somente os valores com datas mais recentes de cada uma das métricas:

# COMMAND ----------

new_data = new_data.filter(col("rank_metrica")==1)

# COMMAND ----------

new_data = new_data.drop("rank_metrica")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 4: Atribui a maior data para cada uma das métricas obtidas, garantindo a atribuição da última métrica alterada para a data mais recente

# COMMAND ----------

new_data = new_data.withColumn("dt_atualizacao", max("dt_atualizacao").over(Window.partitionBy("cd_atendimento", "dt_producao", "dh_insercao_raw")))

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC PASSO 5: Gera um registro único com todas as métricas relacionadas ao atendimento tecnologia inovacao producao
# MAGIC As metricas que não possuirem registros serão atribuídos os valores nulos.
# MAGIC 
# MAGIC     Para cada:
# MAGIC         cd_atendimento
# MAGIC         dt_producao
# MAGIC         dh_atualizacao_unificado
# MAGIC     Pivotear:        
# MAGIC         CASE WHEN cd_metrica_atendimento = 436  THEN vl_valor_medido ELSE NULL END as qt_hora_realizada
# MAGIC         CASE WHEN cd_metrica_atendimento = 437  THEN vl_valor_medido ELSE NULL END as qt_ensaio_realizado
# MAGIC         CASE WHEN cd_metrica_atendimento = 438  THEN vl_valor_medido ELSE NULL END as qt_calibracao_realizada
# MAGIC         CASE WHEN cd_metrica_atendimento = 439  THEN vl_valor_medido ELSE NULL END as qt_aterial_realizado
# MAGIC         CASE WHEN cd_metrica_atendimento = 440  THEN vl_valor_medido ELSE NULL END as qt_relatorio_realizado
# MAGIC         CASE WHEN cd_metrica_atendimento = 441  THEN vl_valor_medido ELSE NULL END as qt_certificado_realizado
# MAGIC         CASE WHEN cd_metrica_atendimento = 442  THEN vl_valor_medido ELSE NULL END as vl_producao
# MAGIC         Outros, Ignorar

# COMMAND ----------

when_otherwise_rules = {"qt_hora_realizada": {"column": "cd_metrica_atendimento", "when": 436, "then": "vl_valor_medido", "otherwise":None},
                        "qt_ensaio_realizado": {"column": "cd_metrica_atendimento", "when": 437, "then": "vl_valor_medido", "otherwise":None}, 
                        "qt_calibracao_realizada": {"column": "cd_metrica_atendimento", "when": 438, "then": "vl_valor_medido", "otherwise":None},
                        "qt_material_realizado": {"column": "cd_metrica_atendimento", "when": 439, "then": "vl_valor_medido", "otherwise":None}, 
                        "qt_relatorio_realizado": {"column": "cd_metrica_atendimento", "when": 440, "then": "vl_valor_medido", "otherwise":None},
                        "qt_certificado_realizado": {"column": "cd_metrica_atendimento", "when": 441, "then": "vl_valor_medido", "otherwise":None}, 
                        "vl_producao": {"column": "cd_metrica_atendimento", "when": 442, "then": "vl_valor_medido", "otherwise":None}}

for r in when_otherwise_rules:
  new_data = new_data.withColumn(r, when(col(when_otherwise_rules[r]["column"]) == when_otherwise_rules[r]["when"], col(when_otherwise_rules[r]["then"])).otherwise(when_otherwise_rules[r]["otherwise"]))

# COMMAND ----------

new_data = new_data.drop("cd_metrica_atendimento", "vl_valor_medido")

# COMMAND ----------

new_data = new_data.withColumn("dh_referencia", col("dt_atualizacao"))

# COMMAND ----------

column_name_mapping = {"cd_atendimento": "cd_atendimento_sti",
                       "dt_atualizacao": "dh_ultima_atualizacao_oltp"}
  
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

new_data = new_data.groupBy("cd_atendimento_sti", "dt_producao", "dh_referencia", "dh_ultima_atualizacao_oltp", "dh_insercao_raw") \
                   .agg(sum("qt_hora_realizada").alias("qt_hora_realizada"), 
                        sum("qt_ensaio_realizado").alias("qt_ensaio_realizado"), 
                        sum("qt_calibracao_realizada").alias("qt_calibracao_realizada"), 
                        sum("qt_material_realizado").alias("qt_material_realizado"), 
                        sum("qt_relatorio_realizado").alias("qt_relatorio_realizado"), 
                        sum("qt_certificado_realizado").alias("qt_certificado_realizado"), 
                        sum("vl_producao").alias("vl_producao")
                       )

# COMMAND ----------

# MAGIC %md
# MAGIC There's one column that needs to be added cause it might change according to what happens on versioning. 
# MAGIC 
# MAGIC Until this moment, we're dealing with new records, so this column will follow the natural implementation for "cd_movimento" = "E"
# MAGIC 
# MAGIC <pre>
# MAGIC Tabela Origem: rl_atendimento_metrica
# MAGIC Campo Origem: dt_atualizacao
# MAGIC Transformação: Mapeamento direto	
# MAGIC Campo Destino: dh_referencia 	
# MAGIC Tipo (tamanho): timestamp	
# MAGIC Descrição: Data de Atualizaçao do Registro, usada como data de referência quando há revisão de valores
# MAGIC </pre>
# MAGIC 					

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_atendimento_sti" : "int",                    
                   "qt_hora_realizada": "int",
                   "qt_ensaio_realizado": "int",
                   "qt_calibracao_realizada": "int",
                   "qt_material_realizado": "int",
                   "qt_relatorio_realizado": "int",
                   "qt_certificado_realizado": "int",
                   "vl_producao": "decimal(18,4)",
                   "dt_producao": "date",
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
                   .withColumn("fl_corrente", lit(1).cast("int")) \
                   .withColumn("cd_ano_referencia", date_format("dt_producao", "yyyy").cast("int")) \
                   .withColumn("cd_mes_referencia", date_format("dt_producao", "MM").cast("int")) 

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
# MAGIC  Chave de comparação de todas as coisas! ["cd_atendimento_sti", "dt_producao"]
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
old_data_to_balance = old_data_active.join(new_data.select("cd_atendimento_sti", "dt_producao").dropDuplicates(), ["cd_atendimento_sti", "dt_producao"], "inner")

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active.join(new_data.select("cd_atendimento_sti", "dt_producao").dropDuplicates(), ["cd_atendimento_sti", "dt_producao"], "leftanti") 

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
old_data_to_balance = old_data_to_balance.withColumn("is_new", lit(0).cast("int"))

# COMMAND ----------

old_data_to_balance = old_data_to_balance.withColumn("dh_insercao_raw", col("dh_insercao_trs"))

# COMMAND ----------

new_data = new_data.union(old_data_to_balance.select(new_data.columns))

# COMMAND ----------

list_dh_insercao_raw = new_data.select("dh_insercao_raw").filter(col("dh_insercao_raw")>var_max_dh_insercao_raw).distinct().orderBy("dh_insercao_raw").collect()

# COMMAND ----------

for item in list_dh_insercao_raw:

  new_data = new_data.withColumn("lag_qt_hora_realizada", lag("qt_hora_realizada", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_qt_ensaio_realizado", lag("qt_ensaio_realizado", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_qt_calibracao_realizada", lag("qt_calibracao_realizada", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_qt_material_realizado", lag("qt_material_realizado", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_qt_relatorio_realizado", lag("qt_relatorio_realizado", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_qt_certificado_realizado", lag("qt_certificado_realizado", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) \
                   .withColumn("lag_vl_producao", lag("vl_producao", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp")))) 
  
  new_data = new_data.withColumn("qt_hora_realizada", when((col("dh_insercao_raw") == item[0]) & (col("qt_hora_realizada").isNull()),col("lag_qt_hora_realizada")).otherwise(col("qt_hora_realizada"))) \
                   .withColumn("qt_ensaio_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_ensaio_realizado").isNull()),col("lag_qt_ensaio_realizado")).otherwise(col("qt_ensaio_realizado"))) \
                   .withColumn("qt_calibracao_realizada", when((col("dh_insercao_raw") == item[0]) & (col("qt_calibracao_realizada").isNull()),col("lag_qt_calibracao_realizada")).otherwise(col("qt_calibracao_realizada"))) \
                   .withColumn("qt_material_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_material_realizado").isNull()),col("lag_qt_material_realizado")).otherwise(col("qt_material_realizado"))) \
                   .withColumn("qt_relatorio_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_relatorio_realizado").isNull()),col("lag_qt_relatorio_realizado")).otherwise(col("qt_relatorio_realizado"))) \
                   .withColumn("qt_certificado_realizado", when((col("dh_insercao_raw")==item[0])&(col("qt_certificado_realizado").isNull()),col("lag_qt_certificado_realizado")).otherwise(col("qt_certificado_realizado"))) \
                   .withColumn("vl_producao", when((col("dh_insercao_raw") == item[0]) & (col("vl_producao").isNull()),col("lag_vl_producao")).otherwise(col("vl_producao"))) 
  
  new_data = new_data.drop("lag_qt_hora_realizada", "lag_qt_ensaio_realizado", "lag_qt_calibracao_realizada", "lag_qt_material_realizado", "lag_qt_relatorio_realizado", "lag_qt_certificado_realizado", "lag_vl_producao")
  
  new_data = new_data.withColumn("qt_hora_realizada", when((col("dh_insercao_raw") == item[0]) & (col("qt_hora_realizada").isNull()), 0).otherwise(col("qt_hora_realizada"))) \
                   .withColumn("qt_ensaio_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_ensaio_realizado").isNull()), 0).otherwise(col("qt_ensaio_realizado"))) \
                   .withColumn("qt_calibracao_realizada", when((col("dh_insercao_raw") == item[0]) & (col("qt_calibracao_realizada").isNull()), 0).otherwise(col("qt_calibracao_realizada"))) \
                   .withColumn("qt_material_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_material_realizado").isNull()), 0).otherwise(col("qt_material_realizado"))) \
                   .withColumn("qt_relatorio_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_relatorio_realizado").isNull()), 0).otherwise(col("qt_relatorio_realizado"))) \
                   .withColumn("qt_certificado_realizado", when((col("dh_insercao_raw") == item[0]) & (col("qt_certificado_realizado").isNull()), 0).otherwise(col("qt_certificado_realizado"))) \
                   .withColumn("vl_producao", when((col("dh_insercao_raw") == item[0]) & (col("vl_producao").isNull()), 0).otherwise(col("vl_producao")))   

# COMMAND ----------

new_data = new_data.drop("dh_insercao_raw")

# COMMAND ----------

qt_columns = [c for c in new_data.columns if c.startswith("qt_")]
hash_columns = ["cd_atendimento_sti", "dt_producao"] + qt_columns
new_data = new_data.withColumn("qt_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_qt_hash", lag("qt_hash", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_referencia"))))

# COMMAND ----------

# For records where "is_new" = 1, when qt_hash != lag_qt_hash, we keep it. Otherwise, we drop it cause nothing changed! 
new_data = new_data.withColumn("delete", when((col("qt_hash") == col("lag_qt_hash")) & (col("is_new") == 1), 1).otherwise(0))

# COMMAND ----------

# Remove all recrods marked "delete" = 1. Also, we don't need this column anymore.
new_data= new_data.filter(col("delete") == 0).drop(*["delete", "qt_hash", "lag_qt_hash"])

# COMMAND ----------

# Now, case we've got more than one record to add, we've go to balance all the old one from trusted and also the intermediate ones. But the most recent is kept as "cd_movimento" = "E", no balance applied to it. 
new_data = new_data.withColumn("row_number", row_number().over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_referencia"))))

# COMMAND ----------

# Now I'll just have to add who's max("row_number") so it's possible to know what records are old and, hwne generating the balanced ones - except for the newest one - I can also update dh_referencia.
new_data = new_data.withColumn("max_row_number", max("row_number").over(Window.partitionBy("cd_atendimento_sti", "dt_producao")))
# Counting after, no records are lost

# COMMAND ----------

new_data = new_data.withColumn("lead_dh_ultima_atualizacao_oltp", lead("dh_ultima_atualizacao_oltp", 1).over(Window.partitionBy("cd_atendimento_sti", "dt_producao").orderBy(asc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

balanced_records = new_data.filter(col("row_number") != col("max_row_number")) \
                           .withColumn("cd_movimento", lit("S")) \
                           .withColumn("fl_corrente", lit(0).cast("int")) \
                           .withColumn("dh_referencia", col("lead_dh_ultima_atualizacao_oltp")) 

# COMMAND ----------

#add control fields from trusted_control_field egg
balanced_records = tcf.add_control_fields(balanced_records, var_adf)

# COMMAND ----------

for i in qt_columns:
   balanced_records = balanced_records.withColumn(i, col(i) * (-1))

# COMMAND ----------

# Now for nd, the old active records and the new records, we apply:
new_data = new_data.withColumn("fl_corrente", when(col("max_row_number") != col("row_number"), 0).otherwise(1))

# COMMAND ----------

# remember that we are overwriting the whole partition
new_data = new_data.union(balanced_records.select(new_data.columns)).drop(*["is_new", "row_number", "max_row_number", "lead_dh_ultima_atualizacao_oltp"])

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "dt_producao"
old_data_inactive_to_rewrite = trusted_data.filter(col("fl_corrente") == 0).join(new_data.select("dt_producao").dropDuplicates(), ["dt_producao"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite.join(new_data.select("dt_producao").dropDuplicates(), ["dt_producao"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns)).orderBy(asc("cd_atendimento_sti"), asc("dt_producao"), asc("dh_ultima_atualizacao_oltp"), asc("dh_referencia"))

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.repartition(5).write.partitionBy("cd_ano_referencia", "cd_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

trusted_data.unpersist()