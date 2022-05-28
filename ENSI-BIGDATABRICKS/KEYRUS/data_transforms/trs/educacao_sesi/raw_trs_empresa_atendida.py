# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	raw_trs_empresa_atendida
# MAGIC Tabela/Arquivo Origem	/raw/bdo/scae/matricula
# MAGIC Tabela/Arquivo Destino	/trs/mtd/sesi/empresa_atendida
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dh_ultima_atualizacao_oltp)
# MAGIC Descrição Tabela/Arquivo Destino	"Registra as informações de identificação (ESTÁVEIS) das empresas atendidas vinculadas às ações educativas do SESI. 
# MAGIC OBS: informações relativas à esta entidade de negócio que necessitam acompanhamento de alterações no tempo devem tratadas em tabela satélite própria."
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Carga incremental por dh_ultima_atualizacao_oltp, que insere ou sobrescreve o registro caso a chave cd_cnpj seja encontrada.
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw scae, que ocorre às 22:00
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
var_tables =  {"origins": ["/bdo/scae/vw_matricula"], "destination": "/mtd/sesi/empresa_atendida"}

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
       'adf_pipeline_name': 'raw_trs_empresa_atendida',
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

src_mt = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_mt)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, current_date, dense_rank, desc, greatest, trim, substring, when, lit, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

var_first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md 
# MAGIC From documentation: 
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from empresa_atendida
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_atualizacao = df_sink.select(max(df_sink["dh_ultima_atualizacao_oltp"])).collect()[0][0]
except AnalysisException:
  var_first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC SELECT DISTINCT
# MAGIC num_cnpj_vinculo, nom_razao_social, dat_registro
# MAGIC FROM matricula
# MAGIC WHERE dat_registro > #var_max_dh_ultima_atualizacao_oltp
# MAGIC /* Se existir mais de uma versão no período utilizar a mais recente */
# MAGIC </pre>

# COMMAND ----------

matricula_columns = ["num_cnpj_vinculo", "nom_razao_social", "dat_registro", "dat_efetivacao_matricula"]

mat = spark.read.parquet(src_mt). \
select(*matricula_columns). \
filter(col("dat_registro") > var_max_dt_atualizacao). \
dropDuplicates()

# COMMAND ----------

# If there's no new data, then just let it die.
if mat.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

# dat_efetivacao_matricula and nr_reg were inserted as parameters for deciding on which record to keep since there's records with distinct descriptions for the same dat_registro. In the end, the longest name will prevail, if all dates are the same.

mat = mat.withColumn("row_number", row_number().over(Window.partitionBy("num_cnpj_vinculo").orderBy(desc("dat_registro"), desc("dat_efetivacao_matricula"))))

# COMMAND ----------

#display(mat.filter(col("num_cnpj_vinculo").isin(['17331106000170','04617464000100', '11354945000128', '14403837000277', '30948608000103', '03462517000190'])))

# COMMAND ----------

mat = mat.filter(col("row_number") == 1).drop(*["row_number", "dat_efetivacao_matricula"]).distinct()

# COMMAND ----------

# Checking for duplicate CNPJ
#mat.groupBy("num_cnpj_vinculo").count().filter(col("count") > 1).show(10, False)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now to the final part!

# COMMAND ----------

var_name_mapping = {"num_cnpj_vinculo": "cd_cnpj", 
                       "nom_razao_social": "nm_razao_social",
                       "dat_registro": "dh_ultima_atualizacao_oltp"}
  
for key in var_name_mapping:
  mat =  mat.withColumnRenamed(key, var_name_mapping[key])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Adding time control column

# COMMAND ----------

#add control fields from trusted_control_field egg
mat = tcf.add_control_fields(mat, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As it is required to partition by year("dh_atualizacao_oltp"), we must create this column, cause Spark won't accept functions as partition definition.

# COMMAND ----------

mat = mat.withColumn("ano", year(mat["dh_ultima_atualizacao_oltp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now it gets beautiful because if it is the first load, then we'll union this with an empty DataFrame of the same schema. If not, then the magic of updating partitions happen. Previous data has already the same schema, so, no need to worry.

# COMMAND ----------

if var_first_load is True:
  df_sink = spark.createDataFrame([], schema=mat.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC We must union the existing trusted object with the new data and rank it by the most recent modification, keeping only the most recent one. Also, there's the need to keep control on what was old and what is new. AS it is partitioned by year, case there's no changes in a specific year, we can skip it.
# MAGIC 
# MAGIC Let's get the years in which we've got some changes.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Harmonizing schema with the available data in ADLS will always make it work.

# COMMAND ----------

mat.columns

# COMMAND ----------

df_sink = df_sink.union(mat.select(df_sink.columns))
df_sink_columns = df_sink.columns

# COMMAND ----------

df_sink = df_sink.withColumn("rank", dense_rank().over(Window.partitionBy("cd_cnpj").orderBy(desc("dh_ultima_atualizacao_oltp"))))

# COMMAND ----------

# We need to overwrite also the years with the old data before the update. If this doesn't happen, then we'll have duplicates.
# Years with change will contain mat["ano"] and df_sink["ano"] for when df_sink["rank"] = 1.
years_with_change_new = mat.select("ano").distinct()

# COMMAND ----------

years_with_change_old = df_sink. \
filter(col("rank") == 2). \
select("ano"). \
dropDuplicates()

# COMMAND ----------

#df_sink.filter(col("cd_cnpj") == "00048785003945").show(20, False)

# COMMAND ----------

years_with_change = years_with_change_new.union(years_with_change_old).dropDuplicates()

# COMMAND ----------

df_sink = df_sink.filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Easier now that we kept only years with some change. Now we can filter it.

# COMMAND ----------

df_sink = df_sink.join(years_with_change, ["ano"], "inner").select(df_sink_columns).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic overwrite will guarantee that only this data is updated

# COMMAND ----------

df_sink.coalesce(3).write.partitionBy("ano").save(path=sink, format="parquet", mode="overwrite")