# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_matricula_ensino_profissional
# MAGIC Tabela/Arquivo Origem	"/raw/bdo/bd_basi/tb_atendimento
# MAGIC /raw/bdo/bd_basi/tb_atendimento_matricula"
# MAGIC Tabela/Arquivo Destino	/trs/evt/matricula_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dt_entrada)
# MAGIC Descrição Tabela/Arquivo Destino	"Registra as informações de identificação (ESTÁVEIS) dos atendimentos do tipo matrícula em ensino profissional  que geram produção e que são contabilizados em termos de consolidação dos dados de produção SENAI. 
# MAGIC OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria."
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_atendimento seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw bd_basi, que ocorre às 22:00
# MAGIC 
# MAGIC Dev: Thomaz Moreira
# MAGIC Manutenção:
# MAGIC   2020/08/13 11:00 - Thomaz Moreira - Revisão da lógica e otimização
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "64")
sqlContext.setConf("spark.default.parallelism", "64")

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
var_tables = {"origins": ["/bdo/bd_basi/tb_atendimento","/bdo/bd_basi/tb_atendimento_matricula", "/bdo/bd_basi/rl_atendimento_pessoa", "/bdo/bd_basi/tb_produto_servico_centro_resp", "/bdo/bd_basi/tb_curso"],"destination": "/evt/matricula_ensino_profissional"}

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
       'adf_pipeline_name': 'raw_trs_matricula_ensino_profissional',
       'adf_pipeline_run_id': 'development',
       'adf_trigger_id': 'development',
       'adf_trigger_name': 'thomaz',
       'adf_trigger_time': '2020-08-14T16:00:00.0145018Z',
       'adf_trigger_type': 'PipelineActivity'
      }
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)

# COMMAND ----------

src_at, src_atm, src_emp, src_pcr, src_cur = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]

src_at, src_atm, src_emp, src_pcr, src_cur

# COMMAND ----------

var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])

var_sink

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, row_number, desc, greatest, trim, substring, when, lit, col, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

try:
  sink = spark.read.parquet(var_sink)
  var_max_dt_atualizacao = sink.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  var_first_load = True
  
var_first_load, var_max_dt_atualizacao

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation section

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from matricula_ensino_profissional
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* Obter a maior data de atualizacao carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from matricula_ensino_profissional */
# MAGIC /* Ler conjuntamente tb_atendimento e tb_atendimento_matricula da raw (regitrso mais recente de ambas) com as regras para carga incremental, execucao diária, query abaixo:*/ 
# MAGIC SELECT 
# MAGIC at.cd_atendimento, 
# MAGIC at.cd_atendimento_dr, 
# MAGIC at.cd_entidade_regional, 
# MAGIC at.cd_pessoa_unidd_atendto, 
# MAGIC at.cd_pessoa, 
# MAGIC atm.cd_aluno_dr, 
# MAGIC atm.cd_tipo_condicao_aluno, 
# MAGIC atm.cd_tipo_entrada_aluno, 
# MAGIC at.cd_tipo_estado_civ, 
# MAGIC at.cd_tipo_necessidd_esp, 
# MAGIC at.cd_tipo_niv_escolaridade, 
# MAGIC at.cd_tipo_situacao_ocupacional,
# MAGIC emp.cd_pessoa_empresa_atendida,
# MAGIC atm.fl_articulacao_sesi_senai, 
# MAGIC at.cd_tipo_gratuidade, 
# MAGIC atm.cd_curso, 
# MAGIC at.cd_tipo_acao, 
# MAGIC TRIM(SUBSTRING(cr.nr_centro_responsabilidade,3,15) as cd_centro_responsabilidade, 
# MAGIC atm.qt_horas_pratica_profis, 
# MAGIC at.dt_inicio, 
# MAGIC at.dt_termino_previsto, 
# MAGIC at.dt_termino,
# MAGIC at.fl_excluido
# MAGIC GREATEST(at.dt_atualizacao, atm.dt_atualizacao) as dt_atualizacao
# MAGIC 
# MAGIC FROM 
# MAGIC /* atendimento matrícula */
# MAGIC (   
# MAGIC     SELECT *, ROW_NUMBER() OVER(PARTITION BY cd_atendimento ORDER BY dt_atualizacao DESC, dh_insercao_raw DESC) as seq 
# MAGIC     FROM tb_atendimento_matricula
# MAGIC ) atm /* para obter o registro mais recente */
# MAGIC 
# MAGIC INNER JOIN 
# MAGIC /* atendimento */
# MAGIC (
# MAGIC     SELECT *, ROW_NUMBER() OVER(PARTITION BY cd_atendimento ORDER BY dt_atualizacao DESC) as seq 
# MAGIC 	FROM tb_atendimento 
# MAGIC     WHERE at.cd_tipo_atendimento = 1
# MAGIC ) at /* para obter o registro mais recente */
# MAGIC ON (atm.cd_atendimento = at.cd_atendimento AND atm.seq = 1 and at.seq = 1)
# MAGIC 
# MAGIC LEFT JOIN 
# MAGIC /* empresa atendida */
# MAGIC (
# MAGIC     SELECT cd_atendimento, max(cd_pessoa) as cd_pessoa_empresa_atendida 
# MAGIC 	FROM rl_atendimento_pessoa 
# MAGIC 	WHERE cd_tipo_atendto_pessoa = 1 
# MAGIC 	GROUP BY cd_atendimento
# MAGIC ) emp /* Subquery fornecida pela STI para obter a empresa atendida (congelada de bd_basi) */
# MAGIC ON (emp.cd_atendimento = at.cd_atendimento) 
# MAGIC 
# MAGIC LEFT JOIN 
# MAGIC /* curso x centro responsabilidade */
# MAGIC (
# MAGIC     SELECT cur.cd_curso, pcr.cd_tipo_acao, trim(substr(pcr.NR_CENTRO_RESPONSABILIDADE,3,15)) as cd_centro_responsabilidade, 
# MAGIC 	GREATEST(pcr.dt_atualizacao, cur.dt_atualizacao) as dt_atualizacao, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY cur.cd_curso, pcr.cd_tipo_acao ORDER BY GREATEST(pcr.dt_atualizacao, cur.dt_atualizacao) DESC) as seq
# MAGIC     FROM tb_curso cur
# MAGIC     LEFT JOIN tb_produto_servico_centro_resp pcr
# MAGIC     ON c.cd_produto_servico = p.cd_produto_servico
# MAGIC ) cr /* para obter o registro mais recente */
# MAGIC ON (cr.cd_curso = atm.cd_curso AND cr.cd_tipo_acao = at.cd_tipo_acao AND and cr.seq = 1) 
# MAGIC 
# MAGIC /* Filtro para "Delta T" */
# MAGIC WHERE (
# MAGIC -- devido ao OR, só deve ser aplicado após o join
# MAGIC atm.dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp OR
# MAGIC at.dt_atualizacao  > #var_max_dh_ultima_atualizacao_oltp)
# MAGIC ```

# COMMAND ----------

#at
"""
Added here the encode for fl_exluido in advance
"""
at_columns = ["cd_atendimento", 
              "cd_tipo_atendimento", 
              "cd_atendimento_dr", 
              "cd_entidade_regional", 
              "cd_pessoa_unidd_atendto", 
              "cd_pessoa", 
              "cd_tipo_estado_civ", 
              "cd_tipo_necessidd_esp", 
              "cd_tipo_niv_escolaridade", 
              "cd_tipo_situacao_ocupacional", 
              "cd_tipo_gratuidade", 
              "dt_inicio", 
              "dt_termino_previsto", 
              "dt_termino", 
              "cd_tipo_acao", 
              "dt_atualizacao", 
              "fl_excluido",
              "dh_insercao_raw"
              ]

at = spark.read.parquet(src_at)\
.select(*at_columns)\
.filter(col("cd_tipo_atendimento") == 1)\
.withColumn(
  "row_number", row_number().over(Window.partitionBy("cd_atendimento").orderBy(desc("dt_atualizacao")))
           )\
.filter(col("row_number") == 1)\
.drop(*["row_number", "cd_tipo_atendimento"])\
.withColumnRenamed("dt_atualizacao", "dt_atualizacao_at")\
.withColumn("fl_excluido", when(col("fl_excluido") == "S", 1).otherwise(0).cast("int"))\
.coalesce(4)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Following, this has to satisfy this inner join:
# MAGIC 
# MAGIC INNER JOIN ...ON (atm.cd_atendimento = at.cd_atendimento AND atm.seq = 1 and at.seq = 1)
# MAGIC 
# MAGIC So, it is the same as filtering for seq=1 in advance, which is way more performatic.
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First thing is to apply filtering for the maximum available var_max_dt_atualizacao.

# COMMAND ----------

#atm
atm_columns = ["cd_atendimento",
                   "cd_aluno_dr",
                   "cd_tipo_condicao_aluno",
                   "cd_tipo_entrada_aluno",
                   "fl_articulacao_sesi_senai",
                   "cd_curso",
                   "qt_horas_pratica_profis",
                   "dt_atualizacao",
                   "dh_insercao_raw"
                  ]

atm = spark.read.parquet(src_atm)\
.select(*atm_columns)\
.withColumn(
  "row_number", row_number().over(
    Window.partitionBy("cd_atendimento").orderBy(desc("dt_atualizacao"), desc("dh_insercao_raw")))
)\
.filter(col("row_number") == 1)\
.drop("row_number", "dh_insercao_raw")\
.withColumnRenamed("dt_atualizacao", "dt_atualizacao_atm")\
.dropDuplicates()\
.coalesce(4)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC INNER JOIN 
# MAGIC   ...
# MAGIC ON (atm.cd_atendimento = at.cd_atendimento AND atm.seq = 1 and at.seq = 1)
# MAGIC -- o filtro seq=1 já foi aplicado
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC We can take the greatest between both our datasets now so we then just have to compare 2 values, not 3
# MAGIC 
# MAGIC GREATEST(at.dt_atualizacao, atm.dt_atualizacao)
# MAGIC 
# MAGIC Here we also apply filtering for 
# MAGIC 
# MAGIC WHERE
# MAGIC atm.dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp OR
# MAGIC at.dt_atualizacao  > #var_max_dh_ultima_atualizacao_oltp)
# MAGIC </pre>

# COMMAND ----------

"""
Spark is optimizing this filtering applying it to the HashBroadcastJoin as part of the join condition. This makes things run a lot faster. 
And also avoids fetching all the data for joining.
"""
at_join = at\
.join(atm, ["cd_atendimento"], how="inner")\
.filter((col("dt_atualizacao_at") > var_max_dt_atualizacao) | (col("dt_atualizacao_atm") > var_max_dt_atualizacao))\
.withColumn("dt_atualizacao", greatest(col("dt_atualizacao_at"), col("dt_atualizacao_atm")))\
.drop(*["dt_atualizacao_at", "dt_atualizacao_atm"])

at_join = at_join.coalesce(4).cache() if var_first_load is False else at_join.coalesce(24)

# COMMAND ----------

"""
As we will count at_join here, we will activate cache().
If we ahve new records, then it is ok to pay the cost.
Else, we'll have to drop it as soon as there are no events.
"""
#If there's no new data, then just let it die.

if at_join.count() == 0:
  at_join = at_join.unpersist()
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC ...
# MAGIC LEFT JOIN 
# MAGIC /* empresa atendida */
# MAGIC (
# MAGIC     SELECT cd_atendimento, max(cd_pessoa) as cd_pessoa_empresa_atendida 
# MAGIC 	FROM rl_atendimento_pessoa 
# MAGIC 	WHERE cd_tipo_atendto_pessoa = 1 
# MAGIC 	GROUP BY cd_atendimento
# MAGIC ) emp /* Subquery fornecida pela STI para obter a empresa atendida (congelada de bd_basi) */
# MAGIC ON (emp.cd_atendimento = at.cd_atendimento)
# MAGIC </pre>

# COMMAND ----------

"""
This object is ~1.6M records, 42MB in cache size. Can be Kept in 2 partitions.
"""

var_emp_columns = ["cd_atendimento",
                   "cd_pessoa",
                   "cd_tipo_atendto_pessoa"
                  ]

emp = spark.read.parquet(src_emp)\
.select(*var_emp_columns)\
.filter(col("cd_tipo_atendto_pessoa") == 1)\
.select("cd_atendimento", "cd_pessoa")\
.groupBy("cd_atendimento")\
.agg(max(col("cd_pessoa")).alias("cd_pessoa_empresa_atendida"))\
.coalesce(2)

# COMMAND ----------

# Using this notation, no "cd_atendimento" column is duplicated
at_join = at_join.join(emp, ["cd_atendimento"], how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC (
# MAGIC     SELECT cur.cd_curso, pcr.cd_tipo_acao, trim(substr(pcr.NR_CENTRO_RESPONSABILIDADE,3,15)) as cd_centro_responsabilidade, 
# MAGIC 	GREATEST(pcr.dt_atualizacao, cur.dt_atualizacao) as dt_atualizacao, 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY cur.cd_curso, pcr.cd_tipo_acao ORDER BY GREATEST(pcr.dt_atualizacao, cur.dt_atualizacao) DESC) as seq
# MAGIC     FROM tb_curso cur
# MAGIC     LEFT JOIN tb_produto_servico_centro_resp pcr
# MAGIC     ON c.cd_produto_servico = p.cd_produto_servico
# MAGIC ) cr /* para obter o registro mais recente */
# MAGIC ON (cr.cd_curso = atm.cd_curso AND cr.cd_tipo_acao = at.cd_tipo_acao AND and cr.seq = 1) 
# MAGIC </pre>
# MAGIC 
# MAGIC On the subsequent join there's this:
# MAGIC *AND cr.seq = 1*
# MAGIC 
# MAGIC We can filter it in here.
# MAGIC 
# MAGIC And we can also implement this here:
# MAGIC 
# MAGIC ... WHERE cr.dt_atualizacao > #var_max_dh_ultima_atualizacao_oltp

# COMMAND ----------

"""
This object is ~130K records, 3.3MB in cache. Can be kept in 1 partition.
"""
var_cur_columns = ["cd_curso", "dt_atualizacao", "cd_produto_servico"]

cur = spark.read.parquet(src_cur)\
.select(*var_cur_columns)\
.filter(col("dt_atualizacao") > var_max_dt_atualizacao)\
.withColumnRenamed("dt_atualizacao", "dt_atualizacao_cur")\
.coalesce(1)

# COMMAND ----------

"""
This object is ~200 records. can be kept in one partition.
"""

var_pcr_columns = ["cd_produto_servico", 
                   "cd_tipo_acao", 
                   "nr_centro_responsabilidade", 
                   "dt_atualizacao"
                  ]

pcr = spark.read.parquet(src_pcr)\
.select(*var_pcr_columns)\
.withColumn("nr_centro_responsabilidade", trim(substring(col("nr_centro_responsabilidade"), 3, 15)))\
.withColumnRenamed("nr_centro_responsabilidade", "cd_centro_responsabilidade")\
.withColumnRenamed("dt_atualizacao", "dt_atualizacao_pcr")\
.coalesce(1)

# COMMAND ----------

"""
This object is ~160k records, and less than 10K in cache. Can be kept in 1 partition.
"""

cur_join = cur\
.join(pcr, ["cd_produto_servico"], "left")\
.drop("cd_produto_servico")\
.withColumn("dt_atualizacao_join", greatest("dt_atualizacao_pcr", "dt_atualizacao_cur"))\
.drop("dt_atualizacao_pcr", "dt_atualizacao_cur")\
.withColumn("row_number", row_number().over(Window.partitionBy("cd_curso", "cd_tipo_acao").orderBy(desc("dt_atualizacao_join"))))\
.filter(col("row_number") == 1)\
.drop(*["row_number", "dt_atualizacao_join"])\
.dropDuplicates()\
.coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC atd_join
# MAGIC LEFT JOIN cur
# MAGIC ON (cr.cd_curso = atm.cd_curso AND cr.cd_tipo_acao = at.cd_tipo_acao AND cr.seq = 1) 
# MAGIC </pre>

# COMMAND ----------

# Don't cache it yet. First, apply all the narrow tranformations for the makeup below
at_join = at_join\
.join(cur_join, ["cd_curso", "cd_tipo_acao"], "left")


at_join = at_join.coalesce(24) if var_first_load is True else at_join.coalesce(4)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now to the final part!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC These onewill remain with the original name until here:
# MAGIC 
# MAGIC cd_centro_responsabilidade

# COMMAND ----------

column_name_mapping = {"cd_atendimento" : "cd_atendimento_matricula_ensino_prof",
                       "cd_atendimento_dr" : "cd_matricula_ensino_prof_dr",
                       "cd_entidade_regional" : "cd_entidade_regional",
                       "cd_pessoa_unidd_atendto" : "cd_pessoa_unidade_atendimento",
                       "cd_pessoa" : "cd_pessoa_aluno_ensino_prof",
                       "cd_tipo_estado_civ" : "cd_tipo_estado_civil",
                       "cd_tipo_necessidd_esp" : "cd_tipo_necessidade_especial",
                       "cd_tipo_niv_escolaridade" : "cd_tipo_nivel_escolaridade",
                       "cd_tipo_situacao_ocupacional" : "cd_tipo_situacao_ocupacional",
                       "cd_tipo_gratuidade" : "cd_tipo_gratuidade",
                       "cd_tipo_acao" : "cd_tipo_acao", "dt_inicio" : "dt_entrada",
                       "dt_termino_previsto" : "dt_saida_prevista",
                       "dt_termino" : "dt_saida", 
                       "dt_atualizacao" : "dh_ultima_atualizacao_oltp",
                       "cd_aluno_dr" : "cd_aluno_ensino_prof_dr",
                       "cd_tipo_condicao_aluno" : "cd_tipo_condicao_aluno",
                       "cd_tipo_entrada_aluno" : "cd_tipo_entrada_aluno",
                       "fl_articulacao_sesi_senai" : "fl_articulacao_sesi_senai",
                       "cd_curso" : "id_curso_ensino_profissional",
                       "qt_horas_pratica_profis" : "qt_horas_pratica_profis",
                       "dt_atualizacao": "dh_ultima_atualizacao_oltp",
                       "fl_excluido": "fl_excluido_oltp"
                      }
  
for key in column_name_mapping:
  at_join =  at_join.withColumnRenamed(key, column_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As it is required to partition by year(dt_entrada), we must create this column, cause Spark won't accept functions as partition definition.

# COMMAND ----------

at_join = at_join.withColumn("dt_entrada_ano", year(at_join["dt_entrada"]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Adding the time control column

# COMMAND ----------

#add control fields from trusted_control_field egg
at_join = tcf.add_control_fields(at_join, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data quality section
# MAGIC 
# MAGIC cd_matricula_ensino_prof_dr : Mapeamento direto, mas tirar os brancos do início e fim
# MAGIC 
# MAGIC cd_aluno_ensino_prof_dr	: Mapeamento direto, mas tirar os brancos do início e fim
# MAGIC 
# MAGIC fl_articulacao_sesi_senai : Flag que identifica se a matrícula possui articulação entre o SESI e o SENAI. (1-SIM ou 0-Não)

# COMMAND ----------

var_trim_columns = ["cd_matricula_ensino_prof_dr", "cd_aluno_ensino_prof_dr"]
for c in var_trim_columns:
  at_join = at_join.withColumn(c, trim(col(c)))

# COMMAND ----------

at_join = at_join.withColumn('fl_articulacao_sesi_senai', when(col("fl_articulacao_sesi_senai") == "S", lit(1)).otherwise(lit(0)))

# COMMAND ----------

var_column_type_map = {"cd_atendimento_matricula_ensino_prof" : "int",
                   "cd_matricula_ensino_prof_dr" : "string",
                   "cd_entidade_regional" : "int", 
                   "cd_pessoa_unidade_atendimento" : "int",
                   "cd_pessoa_aluno_ensino_prof" : "int",
                   "cd_aluno_ensino_prof_dr" : "string",
                   "cd_tipo_condicao_aluno" : "int",
                   "cd_tipo_entrada_aluno" : "int",
                   "cd_tipo_estado_civil" : "int",
                   "cd_tipo_necessidade_especial" : "int",
                   "cd_tipo_nivel_escolaridade" : "int",
                   "cd_tipo_situacao_ocupacional" : "int",
                   "cd_pessoa_empresa_atendida" : "int",
                   "fl_articulacao_sesi_senai" : "int",
                   "cd_tipo_gratuidade" : "int",
                   "id_curso_ensino_profissional" : "int",
                   "cd_tipo_acao" : "int",
                   "qt_horas_pratica_profis" : "int",
                   "dt_entrada" : "date",
                   "dt_saida_prevista" : "date",
                   "dt_saida" : "date",
                   "dh_ultima_atualizacao_oltp" : "timestamp",
                   "dh_insercao_trs" : "timestamp", "fl_excluido_oltp": "int"
                  }

for c in var_column_type_map:
  at_join = at_join.withColumn(c, col(c).cast(var_column_type_map[c]))

# COMMAND ----------

"""
Now cache it! And activate it!
But Only if it is not the first load. Cause it will take a lot of time if it is. And can crush the cluster.

"""
if var_first_load is False:
  at_join = at_join.cache()
  at_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now it gets beautiful because if it is the first load, then we'll union this with an empty DataFrame of the same schema. If not, then the magic of updating partitions happen. Previous data has already the same schema, so, no need to worry.

# COMMAND ----------

if var_first_load is True:
  sink = spark.createDataFrame([], schema=at_join.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the only thing left is to implement this:
# MAGIC 
# MAGIC Carga incremental por dt_atualizacao, que insere ou sobrescreve o registro caso a chave cd_atendimento seja encontrada.
# MAGIC 
# MAGIC We must union the existing trusted object with the new data and rank it by the most recent modification, keeping only the most recent one. Also, there's the need to keep control on what was old and what is new. AS it is partitioned by year, case there's no changes in a specific year, we can skip it.
# MAGIC 
# MAGIC Let's get the years in which we've got some changes.

# COMMAND ----------

"""
This is just a list. Must be coalesced to 1 partition.
"""
years_with_change_new = at_join.select(col("dt_entrada_ano"))\
.distinct()\
.coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Harmonizing schema with the available data in ADLS will always make it work.

# COMMAND ----------

var_sink_columns = sink.columns

# COMMAND ----------

sink = sink.union(at_join.select(sink.columns))\
.withColumn("rank", dense_rank().over(Window.partitionBy("cd_atendimento_matricula_ensino_prof").orderBy(desc("dh_ultima_atualizacao_oltp"))))

sink = sink.coalesce(24) if var_first_load is True else sink.coalesce(8)

# COMMAND ----------

"""
A list, just the same, must be kept in 1 partition
"""

years_with_change_old = sink\
.filter(col("rank") == 2)\
.select("dt_entrada_ano")\
.distinct()\
.coalesce(1)

# COMMAND ----------

"""
Real small object. Might be cached and then used to provide fast processing.
Count is to activate caching.

"""

years_with_change = years_with_change_new\
.union(years_with_change_old)\
.dropDuplicates()\
.coalesce(1)\
.cache()

years_with_change.count()

# COMMAND ----------

sink = sink\
.filter(col("rank") == 1)\
.drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Easier now that we kept only years with some change - the new ones and the old ones to overwrite. Now we can limit our space just to it.

# COMMAND ----------

sink = sink\
.join(years_with_change, ["dt_entrada_ano"], "inner")\
.select(var_sink_columns)\
.dropDuplicates()

sink = sink.coalesce(24) if var_first_load is True else sink.coalesce(8)

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated

# COMMAND ----------

sink.write.partitionBy("dt_entrada_ano").save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

years_with_change = years_with_change.unpersist()

if var_first_load is False:
  at_join = at_join.unpersist()