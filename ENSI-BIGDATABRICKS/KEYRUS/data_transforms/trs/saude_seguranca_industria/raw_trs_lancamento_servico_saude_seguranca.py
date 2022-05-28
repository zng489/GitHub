# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type update
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_lancamento_servico_saude_seguranca
# MAGIC Tabela/Arquivo Origem	/raw/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi /raw/bdo/inddesempenho/form /raw/bdo/inddesempenho/entidade
# MAGIC Tabela/Arquivo Destino	/trs/evt/lancamento_servico_saude_seguranca
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_referencia || cd_mes_referencia
# MAGIC Descrição Tabela/Arquivo Destino	Cadastro dos lançamentos de serviços de saúde e segurança do trabalhador.
# MAGIC Tipo Atualização	U = update (insert/update)
# MAGIC Detalhe Atualização	Caso exista atualização de campos para a chave id_filtro_lancamento_oltp aplicar o update no registro.
# MAGIC Periodicidade/Horario Execução	Diária, após a carga da raw inddesempenho
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC </pre>

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
var_tables =  {"origins": ["/bdo/inddesempenho/vw_lanc_evento_vl_r_ssi",
                           "/bdo/inddesempenho/form", 
                           "/bdo/inddesempenho/entidade"], 
               "destination": "/evt/lancamento_servico_saude_seguranca"}

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
       'adf_pipeline_name': 'raw_trs_lancamento_servico_saude_seguranca',
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

src_ssi, src_fm, src_ent = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_ssi, src_fm)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, max, current_timestamp, from_utc_timestamp, col, when, udf, array, lag
from pyspark.sql import Window
from pyspark.sql.types import StringType
import datetime
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC **FROM DOCUMENTATION:**
# MAGIC <pre>
# MAGIC "/* Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from lancamento_servico_saude_seguranca */
# MAGIC /* Ler conjuntamente vw_lanc_evento_vl_r_ssi e form da raw (regitros mais recente de ambas) com as regras para carga incremental, execucao diária, query abaixo:*/
# MAGIC /* Inserir caso o id_filtro_lancamento_oltp não existir. Alterar somente se algum campo da tabela trs diferente da raw lida, para o id_filtro_lancamento_oltp existente.  */
# MAGIC select  distinct
# MAGIC          vw_lanc_evento_vl_r_ssi.cd_ciclo_ano     cd_ano_referencia
# MAGIC          ,vw_lanc_evento_vl_r_ssi.cd_mes + 1     cd_mes_referencia
# MAGIC 	        ,vw_lanc_evento_vl_r_ssi.id_lancamento_evento id_lancamento_evento_oltp
# MAGIC        	 ,vw_lanc_evento_vl_r_ssi.id_filtro_lancamento id_filtro_lancamento_oltp
# MAGIC        	 ,vw_lanc_evento_vl_r_ssi.cd_centro_responsabilidade 	cd_centro_responsabilidade
# MAGIC         ,entidade.codigooba as cd_unidade_atendimento_oba    ---- alteração 07/07
# MAGIC         ,vw_lanc_evento_vl_r_ssi.id_estabelecimento cd_estabelecimento_atendido
# MAGIC         ,vw_lanc_evento_vl_r_ssi.id_clientela cd_clientela_oltp 
# MAGIC        	 ,form.tipo  cd_tipo_producao
# MAGIC 	        ,vw_lanc_evento_vl_r_ssi.id_produto    cd_produto	
# MAGIC FROM vw_lanc_evento_vl_r_ssi
# MAGIC INNER JOIN form ON vw_lanc_evento_vl_r_ssi.id_form = form.id
# MAGIC LEFT JOIN entidade ON vw_lanc_evento_vl_r_ssi.id_unidade_atendimento = entidade.id
# MAGIC WHERE vw_lanc_evento_vl_r_ssi.dh_insercao_raw  > #var_max_dh_ultima_atualizacao_oltp"	
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get max date of dh_ultima_atualizacao_oltp from last load

# COMMAND ----------

first_load = False
var_max_dh_ultima_atualizacao_oltp = datetime.date(1800, 1, 1)

# COMMAND ----------

try:
  df_trs = spark.read.parquet(sink)
  var_max_dh_ultima_atualizacao_oltp = df_trs.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

print("Is first load?", first_load)
print("Max dt inicio vigencia", var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading raw and Applying filters

# COMMAND ----------

var_useful_columns_vw_lanc_evento_vl_r_ssi = ["cd_ciclo_ano", "cd_mes", "id_lancamento_evento", "id_filtro_lancamento", "cd_centro_responsabilidade", "id_estabelecimento", "id_clientela", "id_produto", "id_form", "dh_insercao_raw", "id_unidade_atendimento"]

# COMMAND ----------

df_ssi = spark.read.parquet(src_ssi)\
.select(*var_useful_columns_vw_lanc_evento_vl_r_ssi)\
.filter(col("dh_insercao_raw") > var_max_dh_ultima_atualizacao_oltp)

# COMMAND ----------

# If there's no new data, then just let it die.
if df_ssi.count() == 0:
  dbutils.notebook.exit('{"new_data_is_zero": 1}')

# COMMAND ----------

var_useful_columns_form = ["id", "tipo"]

# COMMAND ----------

df_form = spark.read.parquet(src_fm)\
.select(*var_useful_columns_form)\
.withColumnRenamed("id", "id_form")

# COMMAND ----------

var_useful_columns_entidade = ["codigooba", "id"]

# COMMAND ----------

df_entidade = spark.read.parquet(src_ent)\
.select(*var_useful_columns_entidade)\
.withColumnRenamed("id", "id_unidade_atendimento")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join tables

# COMMAND ----------

df = df_ssi.join(df_form, ["id_form"], "inner")\
.join(df_entidade, ["id_unidade_atendimento"], "left")\
.drop("id_form", "id_unidade_atendimento")\
.distinct()

# COMMAND ----------

del df_ssi, df_form, df_entidade

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and change types

# COMMAND ----------

var_mapping = {"cd_ciclo_ano": {"name": "cd_ano_referencia", "type": "int"},
               "cd_mes": {"name": "cd_mes", "type": "int"},
              "id_lancamento_evento": {"name": "id_lancamento_evento_oltp", "type": "int"},
              "id_filtro_lancamento": {"name": "id_filtro_lancamento_oltp", "type": "long"},   
              "cd_centro_responsabilidade": {"name": "cd_centro_responsabilidade", "type": "string"},      
              "codigooba": {"name": "cd_unidade_atendimento_oba", "type": "int"},      
              "id_estabelecimento": {"name": "cd_estabelecimento_atendido_oltp", "type": "string"},      
              "id_clientela": {"name": "cd_clientela_oltp", "type": "int"},      
              "tipo": {"name": "cd_tipo_producao", "type": "int"},      
              "id_produto": {"name": "cd_produto", "type": "int"},     
              "dh_insercao_raw": {"name": "dh_ultima_atualizacao_oltp", "type": "timestamp"}
              }

for cl in var_mapping:
  df = df.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

df = df.withColumn("cd_mes_referencia", col("cd_mes") + 1 )\
.withColumn("dh_inclusao_lancamento_evento", col("dh_ultima_atualizacao_oltp"))\
.drop("cd_mes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add timestamp of the process

# COMMAND ----------

#add control fields from trusted_control_field egg
df = tcf.add_control_fields(df, var_adf)

# COMMAND ----------

#var_num_records = df.count()
#var_num_records

# COMMAND ----------

#var_num_records == df.select("id_filtro_lancamento_oltp").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update (Insert & Delete) 

# COMMAND ----------

if first_load == True:
  df_trs = df
  
elif first_load == False:
  df_trs = df_trs.union(df.select(df_trs.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Check if there's some difference between records for each id_filtro_lancamento_oltp. If there are not any differences, keep the oldest record

# COMMAND ----------

id_columns = ['id_lancamento_evento_oltp','id_filtro_lancamento_oltp', 'cd_centro_responsabilidade', 'cd_unidade_atendimento_oba', 'cd_estabelecimento_atendido_oltp', 'cd_clientela_oltp', 'cd_produto','cd_tipo_producao']

# COMMAND ----------

concat_udf = udf(lambda cols: "".join([str(x).strip() + "|" if x is not None else "|" for x in cols]), StringType())

# COMMAND ----------

w = Window.partitionBy("id_filtro_lancamento_oltp").orderBy(col("dh_ultima_atualizacao_oltp").asc())
df_trs = df_trs.withColumn("unique_id", concat_udf(array(*id_columns)))\
.withColumn("changed", (col("unique_id") != lag('unique_id', 1, 0).over(w)).cast("int"))\
.filter(col("changed") == 1)\
.drop("unique_id", "changed")

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure we get only the last record for key id_filtro_lancamento_oltp

# COMMAND ----------

w = Window.partitionBy("id_filtro_lancamento_oltp").orderBy(col("dh_ultima_atualizacao_oltp").desc())
df_trs = df_trs.withColumn("rank", row_number().over(w))\
.filter(col("rank") == 1)\
.drop("rank")

# COMMAND ----------

#df_trs.count()

# COMMAND ----------

del df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write in sink
# MAGIC Table is small in volume. We can keep as 1 file without worries. 

# COMMAND ----------

df_trs.coalesce(1).write.partitionBy("cd_ano_referencia", "cd_mes_referencia").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

