# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_matricula_educacao_sesi_carga_horaria
# MAGIC Tabela/Arquivo Origem	"Principais:
# MAGIC /raw/bdo/scae/ciclo_matricula
# MAGIC Relacionadas:
# MAGIC /raw/bdo/scae/ciclo
# MAGIC /raw/bdo/scae/matricula
# MAGIC /raw/bdo/scae/turma
# MAGIC /raw/bdo/scae/oferta_curso
# MAGIC /raw/bdo/scae/curso
# MAGIC /raw/bdo/scae/produto_servico"
# MAGIC Tabela/Arquivo Destino	/trs/evt/matricula_educacao_sesi_carga_horaria
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_mes_referencia
# MAGIC Descrição Tabela/Arquivo Destino	Lançamentos mensais da carga horária de alunos em cursos de educação do SESI, informados pelas suas regionais. Podem ser eventos do tipo "E", entrada quando um novo lançamento chega, ou do tipo "S", saída, quando um lançamento é substituído, então um registro idêntico ao da entrada correspondente é lançado com sinal invertido para a data de referência de sua substituição, mantendo assim , um saldo ao somar valores.
# MAGIC Tipo Atualização	
# MAGIC Detalhe Atualização	carga incremental, quando existe um novo lancamento para o ano/mês ou um lançamento existente teve seu valor alterado.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira / Marcela
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC There are many huge tables and joins in this execution, so we'll lower Treshold for broadcastjoin and increase timeout to avoid errors

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1048576)
spark.conf.set("spark.sql.broadcastTimeout", '3000000ms')
sqlContext.setConf("spark.sql.shuffle.partitions", "100")
sqlContext.setConf("spark.default.parallelism", "100")

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
var_tables =  {"origins": ["/bdo/scae/ciclo_matricula", "/bdo/scae/ciclo", "/bdo/scae/turma", "/bdo/scae/oferta_curso", "/bdo/scae/curso", "/bdo/scae/produto_servico", "/bdo/scae/vw_matricula"], "destination": "/evt/matricula_educacao_sesi_carga_horaria_teste"}

var_dls = {
  "folders":{
    "landing":"/tmp/dev/lnd",
    "error":"/tmp/dev/err", 
    "staging":"/tmp/dev/stg", 
    "log":"/tmp/dev/log", 
    "raw":"/raw", 
    "trusted": "/trs",
  }
}

var_adf = {'adf_factory_name': 'cnibigdatafactory', 
       'adf_pipeline_name': 'raw_trs_matricula_educacao_sesi_carga_horaria',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't2',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-12-23T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_cmat, src_cicl, src_trma, src_oftc, src_crso, src_prts, src_matr = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_cmat, src_cicl, src_trma, src_oftc, src_crso, src_prts, src_matr)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Getting new available data

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, current_date, dense_rank, asc, desc, greatest, trim, substring, when, lit, col, concat_ws, lead, lag, row_number, coalesce
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from matric_educ_basica_continuada_carga_hr
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes from the raw source table.

# COMMAND ----------

try:
  trusted_data = spark.read.parquet(sink).coalesce(6).cache()
  var_max_dh_ultima_atualizacao_oltp = trusted_data.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
  print("var_max_dh_ultima_atualizacao_oltp: {}".format(var_max_dh_ultima_atualizacao_oltp))
except AnalysisException:
  var_first_load = True

# COMMAND ----------

# I'm starting the first interaction with absolute no duplicates in my trusted area. This is really good.
#trusted_data.count(), trusted_data.select("cd_matricula_ensino_basico", "cd_ano_mes_referencia").dropDuplicates().count()
#(22471246, 22471246)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Implementation here is quite complex, so let's start from the inner portion of this onion.
# MAGIC -- filtrando apenas matrícula em educação básica e continuada
# MAGIC (
# MAGIC    SELECT DISTINCT M.cod_matricula FROM matricula M
# MAGIC   
# MAGIC    -- para obter o centro de responsabilidade   
# MAGIC    INNER JOIN (SELECT cod_turma, cod_unidade, cod_oferta_curso, dat_inicio, dat_termino,
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_turma ORDER BY dat_registro DESC) as SQ 
# MAGIC 			   FROM turma) T 
# MAGIC          ON M.cod_turma = T.cod_turma AND T.SQ = 1
# MAGIC    INNER JOIN (SELECT DISTINCT cod_oferta_curso, cod_curso FROM oferta_curso) OC 
# MAGIC          ON T.cod_oferta_curso = OC.cod_oferta_curso
# MAGIC    INNER JOIN (SELECT cod_curso, cod_produto_servico, 
# MAGIC                ROW_NUMBER() OVER(PARTITION BY cod_curso ORDER BY dat_registro DESC) as SQ 
# MAGIC 			   FROM curso) C 
# MAGIC          ON OC.cod_curso = C.cod_curso AND C.SQ = 1
# MAGIC    INNER JOIN (SELECT DISTINCT cod_produto_servico, cod_centro_responsabilidade FROM produto_servico) PS 
# MAGIC          ON C.cod_produto_servico = PS.cod_produto_servico   
# MAGIC 
# MAGIC    WHERE (SUBSTRING(PS.cod_centro_responsabilidade, 3, 5) IN ('30301', '30302', '30303', '30304')  -- EDUCACAO BASICA, EDUCACAO CONTINUADA, EDUCACAO PROFISSIONAL e EDUCACAO SUPERIOR
# MAGIC           OR SUBSTRING(PS.cod_centro_responsabilidade, 3, 9) = '305010103') -- FORMACAO CULTURAL (na verdade é um curso em EDUCACAO CONTINUADA)
# MAGIC     AND SUBSTRING(PS.cod_centro_responsabilidade, 3, 7) <> '3030202' -- não contabilizar como matrícula: Eventos em EDUCACAO CONTINUADA    
# MAGIC ) MT
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

matricula_columns = ["cod_matricula", "cod_turma"]
matricula = spark.read.parquet(src_matr)\
.select(*matricula_columns)\
.coalesce(32)\
.dropDuplicates()

turma_columns = ["cod_turma", "cod_oferta_curso", "dat_inicio", "dat_termino", "dat_registro"]
w = Window.partitionBy("cod_turma").orderBy(desc("dat_registro"))
turma = spark.read.parquet(src_trma).select(*turma_columns)\
.coalesce(6)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro",  "dat_inicio", "dat_termino", "row_number")

new_data = matricula.join(turma, ["cod_turma"], "inner")

# COMMAND ----------

ofertac_columns = ["cod_oferta_curso", "cod_curso"]
ofertac = spark.read.parquet(src_oftc).select(*ofertac_columns).coalesce(1).dropDuplicates()

new_data = new_data.join(ofertac, ["cod_oferta_curso"], "inner")

# COMMAND ----------

curso_columns = ["cod_curso", "cod_produto_servico", "dat_registro"]
w = Window.partitionBy("cod_curso").orderBy(desc("dat_registro"))
curso = spark.read.parquet(src_crso).select(*curso_columns)\
.coalesce(4)\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dat_registro", "row_number")


new_data = new_data.join(curso, ["cod_curso"], "inner")

# COMMAND ----------

prodserv_columns = ["cod_produto_servico", "cod_centro_responsabilidade"]

prodserv = spark.read.parquet(src_prts). \
select(*prodserv_columns). \
filter(((substring(col("cod_centro_responsabilidade"), 3, 5).isin("30301", "30302", "30303", "30304")) | 
        (substring(col("cod_centro_responsabilidade"), 3, 9) == "305010103")) & 
       (substring(col("cod_centro_responsabilidade"), 3, 7) != "3030202")). \
coalesce(1). \
dropDuplicates()

new_data = new_data.join(prodserv, ["cod_produto_servico"], "inner"). \
drop("cod_produto_servico", "cod_curso", "cod_oferta_curso", "cod_turma", "cod_centro_responsabilidade")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now implementing this initial portion, which will allow for some filtering on the new available data greater then the last load timestamp and also apply some transformations
# MAGIC 
# MAGIC <pre>
# MAGIC SELECT 
# MAGIC    CM.cod_matricula,
# MAGIC    (YEAR(C.dat_inicio_exercicio)*100)+MONTH(C.dat_inicio_exercicio) as cd_ano_mes_referencia,
# MAGIC    CASE WHEN CM.ind_status = 1 THEN NVL(CM.num_hora_destinada, 0) ELSE 0 END as qt_hr_aula,
# MAGIC    CASE WHEN CM.ind_status = 1 THEN NVL(CM.num_hora_aluno, 0) ELSE 0 END as qt_hr_hora_aluno,
# MAGIC    CASE WHEN CM.ind_status = 1 THEN NVL(CM.num_hora_recon_saberes, 0) ELSE 0 END as qt_hr_reconhec_saberes,
# MAGIC    CASE WHEN CM.ind_status = 1 THEN NVL(CM.num_hora_aluno_recon_saberes, 0) ELSE 0 END as qt_hr_reconhec_saberes_aluno,
# MAGIC    CM.dat_alteracao
# MAGIC 
# MAGIC FROM ciclo_matricula CM
# MAGIC INNER JOIN ciclo C ON C.cod_ciclo = CM.cod_ciclo
# MAGIC    [...]
# MAGIC WHERE C.dat_inicio_exercicio >= '2013-01-01' -- filtrando apenas a partir da data em que os ciclos passaram a ser mensais
# MAGIC AND   C.dat_fim_prest_contas < TO_DATE(TODAY()) -- não pegar ciclos futuros
# MAGIC AND   CM.dat_alteracao > #var_max_dh_ultima_atualizacao_oltp -- trazendo os registros da raw que ainda não foram processados
# MAGIC 
# MAGIC ORDER BY CM.cod_matricula, (YEAR(C.dat_inicio_exercicio)*100)+MONTH(C.dat_inicio_exercicio), CM.dat_alteracao
# MAGIC 
# MAGIC </pre>

# COMMAND ----------

ciclomat_columns = ["cod_matricula", "num_hora_destinada", "num_hora_aluno", "num_hora_recon_saberes", "num_hora_aluno_recon_saberes", "ind_status", "dat_alteracao", "cod_ciclo"]

ciclomat = spark.read.parquet(src_cmat). \
select(*ciclomat_columns). \
filter(col("dat_alteracao") > var_max_dh_ultima_atualizacao_oltp). \
coalesce(10). \
dropDuplicates()

# COMMAND ----------

# If there's no new data, then just let it die.
if var_first_load == False:
  if ciclomat.count() == 0:
    dbutils.notebook.exit('{"new_data_count_is_zero": 1}')

# COMMAND ----------

qt_columns = ["num_hora_destinada", "num_hora_aluno", "num_hora_recon_saberes", "num_hora_aluno_recon_saberes"]
for c in qt_columns:
  ciclomat = ciclomat.withColumn(c, when(col("ind_status") == 1, coalesce(col(c), lit(0))).otherwise(lit(0)))

# COMMAND ----------

ciclo_columns = ["cod_ciclo", "dat_inicio_exercicio", "dat_fim_prest_contas"]

# Here we apply filtering for when 'ciclo' started to be computated monthly
ciclo = spark.read.parquet(src_cicl). \
select(*ciclo_columns). \
filter(col("dat_inicio_exercicio") >= "2013-01-01"). \
filter(col("dat_fim_prest_contas") < datetime.datetime.now()). \
coalesce(1). \
drop("dat_fim_prest_contas"). \
dropDuplicates()

# COMMAND ----------

ciclo = ciclo.withColumn("dat_inicio_exercicio", (year(col("dat_inicio_exercicio")) * 100) + month(col("dat_inicio_exercicio")))

# COMMAND ----------

ciclo_join = ciclomat.join(ciclo, ["cod_ciclo"], "inner"). \
drop("cod_ciclo", "ind_status")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC <i>ciclo_join</i>
# MAGIC INNER JOIN <i>new_data</i>
# MAGIC ON <i>ciclo_join</i>.cod_matricula = <i>new_data</i>.cod_matricula
# MAGIC </pre>

# COMMAND ----------

new_data = new_data.join(ciclo_join, ["cod_matricula"], "inner")\
.filter(col("cod_matricula").isNotNull())\
.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we just need some make-up to get <i>new_data</i> ready to go and finally get to versioning logic.

# COMMAND ----------

column_name_mapping = {"cod_matricula": "cd_matricula_educacao_sesi",
                       "dat_inicio_exercicio": "cd_ano_mes_referencia",
                       "num_hora_destinada": "qt_hr_aula",
                       "num_hora_aluno": "qt_hr_hora_aluno",
                       "num_hora_recon_saberes": "qt_hr_reconhec_saberes",
                       "num_hora_aluno_recon_saberes": "qt_hr_reconhec_saberes_aluno",                       
                       "dat_alteracao": "dh_ultima_atualizacao_oltp"}
  
for key in column_name_mapping:
  new_data = new_data.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC There's one column that needs to be added cause it might change according to what happens on versioning. 
# MAGIC 
# MAGIC Until this moment, we're dealing with new records, so this column will follow the natural implementation for "cd_movimento" = "E"
# MAGIC 
# MAGIC <pre>
# MAGIC Tabela Origem: ciclo_matricula
# MAGIC Campo Origem: dat_alteracao
# MAGIC Transformação: Mapeamento direto	
# MAGIC Campo Destino: dh_referencia 	
# MAGIC Tipo (tamanho): timestamp	
# MAGIC Descrição: Data de Atualizaçao do Registro, usada como data de referência quando há revisão de valores
# MAGIC </pre>
# MAGIC 					

# COMMAND ----------

new_data = new_data.withColumn("dh_referencia", col("dh_ultima_atualizacao_oltp"))

# COMMAND ----------

# These ones so far need to be checked against its types cause we'll use them to compare later to the existing records
column_type_map = {"cd_matricula_educacao_sesi" : "int", 
                   "cd_ano_mes_referencia": "int",
                   "qt_hr_aula": "int",
                   "qt_hr_hora_aluno": "int",
                   "qt_hr_reconhec_saberes": "int",
                   "qt_hr_reconhec_saberes_aluno": "int",
                   "dh_ultima_atualizacao_oltp": "timestamp",
                   "dh_referencia": "timestamp"
                  }
for c in column_type_map:
  new_data = new_data.withColumn(c, col(c).cast(column_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Add for all records coming from raw:
# MAGIC * cd_movimento = "E"
# MAGIC * fl_corrente = 1

# COMMAND ----------

new_data = new_data\
.withColumn("cd_movimento", lit("E").cast("string"))\
.withColumn("fl_corrente", lit(1).cast("int"))

# COMMAND ----------

#add control fields from trusted_control_field egg
new_data = tcf.add_control_fields(new_data, var_adf)

# COMMAND ----------

if var_first_load is True:
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
# MAGIC  Chave de comparação de todas as coisas! ["cd_matricula_educacao_sesi", "cd_ano_mes_referencia"]
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
old_data_to_balance = old_data_active.join(new_data.select("cd_matricula_educacao_sesi", "cd_ano_mes_referencia").dropDuplicates(), ["cd_matricula_educacao_sesi", "cd_ano_mes_referencia"], "inner")

# COMMAND ----------

#The remaining active records of the trusted layer that do not have a new update record but are in the same partitions as the new records must be rewritten
old_data_active_to_rewrite = old_data_active.join(new_data.select("cd_matricula_educacao_sesi", "cd_ano_mes_referencia").dropDuplicates(), ["cd_matricula_educacao_sesi", "cd_ano_mes_referencia"], "leftanti") 

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

new_data = new_data.union(old_data_to_balance.select(new_data.columns))

# COMMAND ----------

qt_columns = [c for c in new_data.columns if c.startswith("qt_")]
hash_columns = ["cd_matricula_educacao_sesi", "cd_ano_mes_referencia"] + qt_columns
new_data = new_data.withColumn("qt_hash", concat_ws("@", *hash_columns))

# COMMAND ----------

new_data = new_data.withColumn("lag_qt_hash", lag("qt_hash", 1).over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_ano_mes_referencia").orderBy(asc("dh_referencia"))))

# COMMAND ----------

# For records where "is_new" = 1, when qt_hash != lag_qt_hash, we keep it. Otherwise, we drop it cause nothing changed! 
new_data = new_data.withColumn("delete", when((col("qt_hash") == col("lag_qt_hash")) & (col("is_new") == 1), 1).otherwise(0))

# COMMAND ----------

# Remove all recrods marked "delete" = 1. Also, we don't need this column anymore.
new_data = new_data.filter(col("delete") == 0).drop(*["delete", "qt_hash", "lag_qt_hash"])

# COMMAND ----------

# Now, case we've got more than one record to add, we've go to balance all the old one from trusted and also the intermediate ones. But the most recent is kept as "cd_movimento" = "E", no balance applied to it. 
new_data = new_data.withColumn("row_number", row_number().over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_ano_mes_referencia").orderBy(asc("dh_referencia"))))

# COMMAND ----------

# Now I'll just have to add who's max("row_number") so it's possible to know what records are old and, hwne generating the balanced ones - except for the newest one - I can also update dh_referencia.
new_data = new_data.withColumn("max_row_number", max("row_number").over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_ano_mes_referencia")))
# Counting after, no records are lost

# COMMAND ----------

new_data = new_data.withColumn("lead_dh_ultima_atualizacao_oltp", lead("dh_ultima_atualizacao_oltp", 1).over(Window.partitionBy("cd_matricula_educacao_sesi", "cd_ano_mes_referencia").orderBy(asc("dh_ultima_atualizacao_oltp"))))

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

# Now for new_data, the old active records and the new records, we apply:
new_data = new_data.withColumn("fl_corrente", when(col("max_row_number") != col("row_number"), 0).otherwise(1))

# COMMAND ----------

# remember that we are overwriting the whole partition
new_data = new_data.union(balanced_records.select(new_data.columns)).drop(*["is_new", "row_number", "max_row_number", "lead_dh_ultima_atualizacao_oltp"])

# COMMAND ----------

# Now that we know what really stays, we can filter the old, unchanged records from trusted data for the partitions we need to overwrite. Partition is by "cd_ano_mes_referencia"
old_data_inactive_to_rewrite = trusted_data.filter(col("fl_corrente") == 0).join(new_data.select("cd_ano_mes_referencia").dropDuplicates(), ["cd_ano_mes_referencia"], "inner")

# COMMAND ----------

#Here we filter the remaining active records from the trusted layer that do not have a new update record for the parts that need to be overwritten.
old_data_active_to_rewrite = old_data_active_to_rewrite.join(new_data.select("cd_ano_mes_referencia").dropDuplicates(), ["cd_ano_mes_referencia"], "inner")

# COMMAND ----------

old_data_to_rewrite = old_data_inactive_to_rewrite.union(old_data_active_to_rewrite)

# COMMAND ----------

new_data = new_data.union(old_data_to_rewrite.select(new_data.columns)).orderBy(asc("cd_matricula_educacao_sesi"), asc("cd_ano_mes_referencia"), asc("dh_ultima_atualizacao_oltp"), asc("dh_referencia"))

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated
# MAGIC 
# MAGIC Needs only to perform this. Pointing to production dir.

# COMMAND ----------

new_data.coalesce(5).write.partitionBy("cd_ano_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

trusted_data.unpersist()

# COMMAND ----------

