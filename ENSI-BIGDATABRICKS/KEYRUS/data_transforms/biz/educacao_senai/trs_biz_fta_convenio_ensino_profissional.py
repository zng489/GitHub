# Databricks notebook source
# MAGIC %md
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	trs_biz_fta_convenio_ensino_profissional
# MAGIC Tabela/Arquivo Origem	"/trs/evt/convenio_ensino_profissional
# MAGIC /trs/evt/convenio_ensino_prof_carga_horaria"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fta_convenio_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção SENAI de convênios anuais em termos de cooperação em ensino profissional
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Thomaz Moreira
# MAGIC </pre>

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

import json
import re
from datetime import datetime

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables = {"origins": ["/evt/convenio_ensino_profissional",
                         "/evt/convenio_ensino_prof_carga_horaria"],
              "destination": "/producao/fta_convenio_ensino_profissional",
              "databricks": {
                "notebook": "/biz/educacao_senai/trs_biz_fta_convenio_ensino_profissional"
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
       'adf_pipeline_name': 'trs_biz_fta_convenio_ensino_profissional',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-28T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2013, "month": 12, "dt_closing": "2020-03-02"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_cepr, src_crpch = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_cepr, src_crpch)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

from pyspark.sql.functions import sum, max, current_timestamp, from_utc_timestamp, to_date, year, current_date, asc, desc, when, lit, col, month, year
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from datetime import date
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_parameters = {}
if "closing" in var_user_parameters:
  if "year" and "month" and "dt_closing" in var_user_parameters["closing"] :
    var_parameters["prm_ano_fechamento"] = var_user_parameters["closing"]["year"]
    var_parameters["prm_mes_fechamento"] = var_user_parameters["closing"]["month"]
    splited_date = var_user_parameters["closing"]["dt_closing"].split('-', 2)
    var_parameters["prm_data_corte"] = date(int(splited_date[0]), int(splited_date[1]), int(splited_date[2]))
else:
  var_parameters["prm_ano_fechamento"] = datetime.now().year
  var_parameters["prm_mes_fechamento"] = datetime.now().month
  var_parameters["prm_data_corte"] = datetime.now()
  
print(var_parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC /*  
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento da Produção de Convênios em Termo de Cooperação em Ensino Profissional
# MAGIC do SENAI do mês: #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC   SELECT 
# MAGIC    #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC    #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC    #prm_data_corte AS dt_fechamento,
# MAGIC    m.cd_entidade_regional,
# MAGIC    m.cd_centro_responsabilidade,    
# MAGIC    SUM(c.qt_matricula)   AS qt_matricula_indireta, 
# MAGIC    SUM(c.qt_matricula_concluinte) AS qt_matricula_indireta_concluinte, 
# MAGIC    SUM(c.qt_hr_aluno_hora)  AS qt_hr_aluno_hora_matricula_indireta
# MAGIC    
# MAGIC    FROM convenio_ensino_prof_carga_horaria c
# MAGIC    
# MAGIC    INNER JOIN m MAT (PASSO 2)
# MAGIC    ON c.cd_atendimento_convenio_ensino_prof = m.cd_atendimento_convenio_ensino_prof
# MAGIC 
# MAGIC    -- FILTROS REGISTROS CARGA HORARIA
# MAGIC    WHERE ((m.fl_excluido_oltp = 0 OR (m.fl_excluido_oltp = 1 AND m.dh_ultima_atualizacao_oltp > #prm_data_corte))) -- não foi excluído ou foi excluído após a data de corte
# MAGIC    AND   YEAR(c.cd_ano_mes_referencia) = #prm_ano_fechamento 
# MAGIC    AND   MONTH(c.cd_ano_mes_referencia) LTE #prm_mes_fechamento 
# MAGIC    AND   c.dh_referencia LTE #prm_data_corte
# MAGIC    
# MAGIC    GROUP BY  m.cd_entidade_regional, m.cd_centro_responsabilidade,  
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and filter data 

# COMMAND ----------

cepr_columns = ["cd_atendimento_convenio_ensino_prof", "cd_entidade_regional", "cd_centro_responsabilidade", "fl_excluido_oltp", "dh_ultima_atualizacao_oltp"]

cepr = spark.read.parquet(src_cepr). \
select(*cepr_columns). \
filter(
  (col("fl_excluido_oltp") == 0) |\
  ((col("fl_excluido_oltp") == 1) & (col("dh_ultima_atualizacao_oltp") > var_parameters["prm_data_corte"]))).\
drop("fl_excluido_oltp", "dh_ultima_atualizacao_oltp")

# COMMAND ----------

crpch_columns = ["cd_atendimento_convenio_ensino_prof", "dh_referencia", "cd_ano_mes_referencia", "qt_matricula", "qt_matricula_concluinte", "qt_hr_aluno_hora"]

# Closings goes from january to the specified month received as parameters
crpch = spark.read.parquet(src_crpch). \
select(*crpch_columns). \
filter(
  (year(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) == var_parameters["prm_ano_fechamento"]) &
  (month(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) <= var_parameters["prm_mes_fechamento"]) &
  (col("dh_referencia") <= var_parameters["prm_data_corte"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join by cd_atendimento_convenio_ensino_prof

# COMMAND ----------

df = cepr.join(crpch, ["cd_atendimento_convenio_ensino_prof"], "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBy cd_entidade_regional and cd_centro_responsabilidade

# COMMAND ----------

df = df.groupBy("cd_entidade_regional", "cd_centro_responsabilidade"). \
agg(sum(col("qt_matricula")).alias("qt_matricula"),
    sum(col("qt_matricula_concluinte")).alias("qt_matricula_concluinte"),
    sum(col("qt_hr_aluno_hora")).alias("qt_hr_aluno_hora")    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add columns, rename and change types

# COMMAND ----------

# The columns added below will already be verified against it's definitive type and name. No need to verify it later.
df = df.withColumn("cd_ano_fechamento", lit((var_parameters["prm_ano_fechamento"])).cast("int")). \
withColumn("cd_mes_fechamento", lit((var_parameters["prm_mes_fechamento"])).cast("int")). \
withColumn("dt_fechamento", lit((var_parameters["prm_data_corte"])).cast("date"))

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

del cepr
del crpch

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now it's just make-up and write

# COMMAND ----------

var_mapping = {"cd_entidade_regional": {"name": "cd_entidade_regional", "type": "int"},
                "cd_centro_responsabilidade": {"name": "cd_centro_responsabilidade", "type": "string"},
                "qt_matricula": {"name": "qt_matricula_indireta", "type": "int"},
                "qt_matricula_concluinte": {"name": "qt_matricula_indireta_concluinte", "type": "int"},
                "qt_hr_aluno_hora": {"name": "qt_hr_aluno_hora_matricula_indireta", "type": "int"}
              }

for cl in var_mapping:
  df = df.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC If there's no data and it is the first load, we must save the data frame without partitioning, because saving an empty partitioned dataframe does not save the metadata.
# MAGIC When there is no new data in the business and you already have other data from other loads, nothing happens.
# MAGIC And when we have new data, it is normally saved with partitioning.

# COMMAND ----------

if df.count()==0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df. \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As we partition it by cd_ano_fechamento and cd_mes_fechamento, these partition will be overwritten, an no worries about keeping old records.
# MAGIC 
# MAGIC Records are few, so one partition will do. 

# COMMAND ----------

df. \
coalesce(1). \
write. \
partitionBy("cd_ano_fechamento", "cd_mes_fechamento").\
save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

