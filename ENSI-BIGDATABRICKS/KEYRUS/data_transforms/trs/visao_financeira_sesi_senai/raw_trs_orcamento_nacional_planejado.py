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
# MAGIC Processo	raw_trs_orcamento_nacional_planejado <br/>
# MAGIC Tabela/Arquivo Origem	/raw/bdo/protheus11/akd010<br/>
# MAGIC Tabela/Arquivo Destino	/trs/evt/orcamento_nacional_planejado<br/>
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dt_lancamento) / cd_tipo_orcamento / MONTH(dt_lancamento)<br/>
# MAGIC Descrição Tabela/Arquivo Destino	Lançamentos financeiros de receitas e despesas planejados, informados pelas regionais e departamento nacional SESI e SENAI, consolidadoras.<br/>
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)<br/>
# MAGIC Detalhe Atuaização	carga full no ano com substituição da partição completa, execução mensal<br/>
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw akd010<br/>
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC Manutenção:
# MAGIC   2020/06/10 - Thomaz Moreira: inclusão de cláusula de avaliação do parâmetro var_ano, assumindo ano corrente quando var_ano = 0

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
import datetime

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables =  {"origins": ["/bdo/protheus11/akd010"], "destination": "/evt/orcamento_nacional_planejado"}

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
       'adf_pipeline_name': 'raw_trs_orcamento_nacional_planejado',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"var_ano": 1900}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters", var_user_parameters)

# COMMAND ----------

src_akd = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_akd)

# COMMAND ----------

var_sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(var_sink)

# COMMAND ----------

var_ano = var_user_parameters["var_ano"]

"""
Evaluation for var_ano to consider the actual year when its value is 0
"""
var_ano = datetime.datetime.now().year if var_ano == 0 else var_ano

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section - read and apply filters

# COMMAND ----------

from pyspark.sql.functions import trim, col, substring, when, lit, from_utc_timestamp, current_timestamp, from_unixtime, unix_timestamp, year, month, to_date, regexp_replace
from pyspark.sql.types import IntegerType, LongType, DateType
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

useful_columns = ["AKD_TPSALD", "AKD_CODPLA", "AKD_OPER", "AKD_LOTE", "AKD_ID","AKD_DATA", "AKD_HIST", "AKD_CC", "AKD_ITCTB", "AKD_CO", "AKD_VALOR1", "AKD_TIPO", "R_E_C_N_O_", "AKD_XDTIMP", "AKD_XFILE", "AKD_STATUS", "AKD_FILIAL", "D_E_L_E_T_", "YEAR"] 

# COMMAND ----------

df = spark.read.parquet(src_akd).select(*useful_columns)

# COMMAND ----------

#Check if AKD_DATA is not null and if it is transform to default value
df = df.withColumn("AKD_DATA", when(trim(col("AKD_DATA")).isNull(), "19000101").otherwise(col("AKD_DATA")))

# COMMAND ----------

df = df.withColumn("D_E_L_E_T_", when(col("D_E_L_E_T_").isNull(), lit(0)).otherwise(col("D_E_L_E_T_")))\
.filter((trim(col("AKD_TPSALD")).isin('O1','O2','O3')) &\
               ((col("AKD_CO").like("4%")) | (col("AKD_CO").like("3%"))) &\
               (substring(col("AKD_OPER"),2,1).cast(IntegerType()).isin(2,3)) &\
               (trim(col("AKD_OPER")).like("%0000")) &\
               (col("AKD_STATUS") == 1) &\
               (col("D_E_L_E_T_") != "*") &\
               (substring(col("AKD_FILIAL"),1,2) == substring(col("AKD_OPER"),1,2)) &\
               (year(to_date(col("AKD_DATA"), 'yyyyMMdd')) == var_ano))\
.drop("AKD_STATUS", "AKD_FILIAL", "D_E_L_E_T_")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation section - variables

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"cd_planejamento": trim(col("AKD_CODPLA")),
                 "cd_entidade_regional_erp_oltp": trim(col("AKD_OPER")),
                 "cd_lote_lancamento": trim(col("AKD_LOTE")),
                 "ds_lancamento": trim(col("AKD_HIST")),
                 "cd_unidade_organizacional": trim(col("AKD_CC")),
                 "cd_conta_contabil": trim(col("AKD_CO")),
                 "nm_arq_referencia": trim(col("AKD_XFILE")),
                 "cd_entidade_nacional": substring(col("AKD_OPER"),2,1).cast(IntegerType()),
                 "cd_centro_responsabilidade": trim(substring(col("AKD_ITCTB"),3,25)),
                 "qt_lancamento": lit(1),
                 "vl_lancamento": col("AKD_VALOR1")*when(col("AKD_TIPO") != '1', lit(-1)).otherwise(lit(1)),
                 "AKD_DATA": from_unixtime(unix_timestamp(col('AKD_DATA'), 'yyyyMMdd')),
                 "AKD_XDTIMP": from_unixtime(unix_timestamp(when(trim(col("AKD_XDTIMP")) == '', lit(None)).otherwise(col('AKD_XDTIMP')), 'yyyyMMdd')),
                 "dt_ultima_atualizacao_oltp": when(col("AKD_XDTIMP").isNull(), col("AKD_DATA")).otherwise(col("AKD_XDTIMP"))
                 }

#Apply transformations defined in column_map
for c in var_column_map:
  df = df.withColumn(c, var_column_map[c])

# COMMAND ----------

var_mapping = {"dt_ultima_atualizacao_oltp": {"name": "dt_ultima_atualizacao_oltp", "type": "date"},
                "AKD_DATA": {"name": "dt_lancamento", "type": "date"},
                "AKD_ID": {"name": "cd_item_lote_lancamento", "type": "int"},
                "R_E_C_N_O_": {"name": "id_registro_oltp", "type": "long"},
                "AKD_TPSALD": {"name": "cd_tipo_planejamento", "type": "string"}}

for cl in var_mapping:
  df = df.withColumn(cl, col(cl).cast(var_mapping[cl]["type"])).withColumnRenamed(cl, var_mapping[cl]["name"])

# COMMAND ----------

df = df.withColumn("cd_ano_lancamento", year(col("dt_lancamento")))\
.drop("AKD_CODPLA", "AKD_LOTE", "AKD_HIST", "AKD_CC", "AKD_CO","AKD_XFILE", "AKD_OPER", "AKD_ITCTB", "AKD_TIPO", "AKD_VALOR1", "AKD_XDTIMP")

# COMMAND ----------

#add control fields from trusted_control_field egg
df = tcf.add_control_fields(df, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Salvando tabela no destino

# COMMAND ----------

df.write.partitionBy(["cd_ano_lancamento"]).save(path=var_sink, format="parquet", mode="overwrite")

# COMMAND ----------

