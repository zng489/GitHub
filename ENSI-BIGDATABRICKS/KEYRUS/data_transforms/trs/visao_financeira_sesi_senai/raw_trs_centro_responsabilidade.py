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
# MAGIC Processo	raw_trs_centro_responsabilidade</br>
# MAGIC Tabela/Arquivo Origem	/raw/bdo/protheus11/ctd010</br>
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/centro_responsabilidade</br>
# MAGIC Particionamento Tabela/Arquivo Destino	Não há</br>
# MAGIC Descrição Tabela/Arquivo Destino	Os Centros de Responsabilidade (PCR) forma uma estrutura de códigos que visa orientar, organizar e consolidar nacionalmente a construção do orçamento das Receitas e Despesas pelas linhas de negócios desenvolvidas pelo Sistema Indústria</br>
# MAGIC Tipo Atualização	F (Substituição Full da tabela: Truncate/Insert)</br>
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw ctd010</br>
# MAGIC 
# MAGIC Dev: Tiago Shin

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
var_tables =  {"origins": ["/bdo/protheus11/ctd010"], "destination": "/mtd/corp/centro_responsabilidade"}

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
       'adf_pipeline_name': 'raw_trs_centro_responsabilidade',
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

src_ctd = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_ctd)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import substring, trim, lit, asc, desc, row_number, col, when, concat, length, upper, lower, dense_rank, count, from_utc_timestamp, current_timestamp
from pyspark.sql import Window
from pyspark.sql.types import LongType
import datetime
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

useful_columns = ["CTD_ITEM", "CTD_DESC01", "CTD_CLASSE", "CTD_ITSUP", "R_E_C_N_O_", "D_E_L_E_T_"]

# COMMAND ----------

dfa = spark.read.parquet(src_ctd).select(*useful_columns)\
.filter((col("R_E_C_N_O_") >= 1802) & (col("D_E_L_E_T_") != "*"))\
.dropDuplicates()\
.drop("D_E_L_E_T_") 

# COMMAND ----------

dfa = dfa.withColumn("CTD_ITEM_id", trim(substring(dfa["CTD_ITEM"], 3, 50)))\
.withColumn("sq", dense_rank().over(Window.partitionBy("CTD_ITEM_id").orderBy(desc("R_E_C_N_O_"))))

# COMMAND ----------

dfa = dfa.filter(dfa["sq"] == 1).drop("sq")\
.withColumn("CTD_ITEM_ano_ref", substring(dfa["CTD_ITEM"], 1, 2))\
.withColumn("CTD_ITSUP_id", trim(substring(dfa["CTD_ITSUP"], 3, 50)))\
.withColumn("CTD_ITEM_id", trim(substring(dfa["CTD_ITEM"], 3, 50)))\
.select("CTD_ITEM_id",trim(col("CTD_DESC01")).alias("CTD_DESC01"), "CTD_ITSUP_id", "R_E_C_N_O_", "CTD_ITEM_ano_ref")

# COMMAND ----------

dfa.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating code-column objects for all levels

# COMMAND ----------

df_n1 = dfa.filter(length(col("CTD_ITEM_id")) == 1)\
.select(col("CTD_ITEM_id").alias("cd_natureza_cr_n1"), 
        col("CTD_DESC01").alias("ds_natureza_cr_n1"))\
.dropDuplicates()

# COMMAND ----------

df_n2 = dfa.filter(length(col("CTD_ITEM_id")) == 3)\
.select(col("CTD_ITEM_id").alias("cd_linha_acao_cr_n2"), 
        col("CTD_DESC01").alias("ds_linha_acao_cr_n2"))\
.dropDuplicates()

# COMMAND ----------

df_n3 = dfa.filter(length(col("CTD_ITEM_id")) == 5)\
.select(col("CTD_ITEM_id").alias("cd_centro_responsabilidade_cr_n3"), 
        col("CTD_DESC01").alias("ds_centro_responsabilidade_cr_n3"))\
.dropDuplicates()

# COMMAND ----------

df_n4 = dfa.filter(length(col("CTD_ITEM_id")) == 7)\
.select(col("CTD_ITEM_id").alias("cd_centro_responsabilidade_cr_n4"), 
        col("CTD_DESC01").alias("ds_centro_responsabilidade_cr_n4"))

# COMMAND ----------

df_n5 = dfa.filter(length(col("CTD_ITEM_id")) == 9)\
.select(col("CTD_ITEM_id").alias("cd_centro_responsabilidade_cr_n5"), 
        col("CTD_DESC01").alias("ds_centro_responsabilidade_cr_n5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter by length of substring

# COMMAND ----------

dfa = dfa.filter(length(col("CTD_ITEM_id")) == 9).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Levels development

# COMMAND ----------

# MAGIC %md
# MAGIC Level 1 - NATUREZA CR

# COMMAND ----------

dfa = dfa.join(df_n1, substring(dfa["CTD_ITEM_id"],1,1) == df_n1["cd_natureza_cr_n1"], how='left')

# COMMAND ----------

dfa = dfa.withColumn("cd_natureza_cr_n1", 
                     when(col("cd_natureza_cr_n1").isNotNull(), dfa["cd_natureza_cr_n1"])\
                     .otherwise(substring(col("CTD_ITEM_id"),1,1)))\
.withColumn("ds_natureza_cr_n1", 
            when(col("ds_natureza_cr_n1").isNotNull(), dfa["ds_natureza_cr_n1"])\
            .otherwise(concat(substring(col("CTD_ITEM_id"),1,1),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del df_n1 

# COMMAND ----------

# MAGIC %md
# MAGIC Nível 2 - LINHA AÇÃO CR

# COMMAND ----------

#df_n2.select('cd_linha_acao_cr_n2').groupBy("cd_linha_acao_cr_n2").agg(count("cd_linha_acao_cr_n2").alias("count")).filter(col("count") > 1).show()

# COMMAND ----------

#Utilizar o inner join faz com que descartemos todos os códigos de CTD_ITEM_id que possuem menos de 3 caracteres ou os caracteres iniciais não correspondem a nenhum valor de cd_linha_acao_cr_n2 da tabela de descrição. A query retorna alguns valores com chave de R_E_C_N_O_ duplicados, então é necessário dropar pelos valores distintos da chave.
dfa = dfa.join(df_n2, substring(dfa["CTD_ITEM_id"],1,3) == df_n2["cd_linha_acao_cr_n2"], how='left')

# COMMAND ----------

dfa = dfa.withColumn("cd_linha_acao_cr_n2", 
                     when(col("cd_linha_acao_cr_n2").isNotNull(), dfa["cd_linha_acao_cr_n2"])\
                     .otherwise(substring(col("CTD_ITEM_id"),1,3)))\
.withColumn("ds_linha_acao_cr_n2", 
            when(col("ds_linha_acao_cr_n2").isNotNull(), dfa["ds_linha_acao_cr_n2"])\
            .otherwise(concat(substring(col("CTD_ITEM_id"),1,3),lit("NÃO INFORMADO"))))

# COMMAND ----------

del df_n2

# COMMAND ----------

# MAGIC %md
# MAGIC Nível 3 - CENTRO RESPONSABILIDADE CR

# COMMAND ----------

#df_n3.select('cd_centro_responsabilidade_cr_n3').groupBy("cd_centro_responsabilidade_cr_n3").agg(count("cd_centro_responsabilidade_cr_n3").alias("count")).filter(col("count") > 1).show()

# COMMAND ----------

dfa = dfa.join(df_n3, substring(dfa["CTD_ITEM_id"],1,5) == df_n3["cd_centro_responsabilidade_cr_n3"], how='left')

# COMMAND ----------

dfa = dfa.withColumn("cd_centro_responsabilidade_cr_n3", 
                     when(col("cd_centro_responsabilidade_cr_n3").isNotNull(), dfa["cd_centro_responsabilidade_cr_n3"])\
                     .otherwise(substring(col("CTD_ITEM_id"),1,5)))\
.withColumn("ds_centro_responsabilidade_cr_n3", 
            when(col("ds_centro_responsabilidade_cr_n3").isNotNull(), dfa["ds_centro_responsabilidade_cr_n3"])\
            .otherwise(concat(substring(col("CTD_ITEM_id"),1,5),lit("NÃO INFORMADO"))))

# COMMAND ----------

del df_n3

# COMMAND ----------

# MAGIC %md
# MAGIC Nível 4 - CENTRO RESPONSABILIDADE CR

# COMMAND ----------

#df_n4.select('cd_centro_responsabilidade_cr_n4').groupBy("cd_centro_responsabilidade_cr_n4").agg(count("cd_centro_responsabilidade_cr_n4").alias("count")).filter(col("count") > 1).show()

# COMMAND ----------

dfa = dfa.join(df_n4, substring(dfa["CTD_ITEM_id"],1,7) == df_n4["cd_centro_responsabilidade_cr_n4"], how='left')

# COMMAND ----------

#dfa.filter(col("cd_centro_responsabilidade_cr_n4").isNull()).count() #0

# COMMAND ----------

dfa = dfa.withColumn("cd_centro_responsabilidade_cr_n4", 
                     when(col("cd_centro_responsabilidade_cr_n4").isNotNull(), dfa["cd_centro_responsabilidade_cr_n4"])\
                     .otherwise(substring(col("CTD_ITEM_id"),1,7)))\
.withColumn("ds_centro_responsabilidade_cr_n4", 
            when(col("ds_centro_responsabilidade_cr_n4").isNotNull(), dfa["ds_centro_responsabilidade_cr_n4"])\
            .otherwise(concat(substring(col("CTD_ITEM_id"),1,7),lit("NÃO INFORMADO"))))

# COMMAND ----------

del df_n4

# COMMAND ----------

# MAGIC %md
# MAGIC Nível 5 - CENTRO RESPONSABILIDADE CR

# COMMAND ----------

#df_n5.select('cd_centro_responsabilidade_cr_n5').groupBy("cd_centro_responsabilidade_cr_n5").agg(count("cd_centro_responsabilidade_cr_n5").alias("count")).filter(col("count") > 1).show()

# COMMAND ----------

dfa = dfa.join(df_n5, substring(dfa["CTD_ITEM_id"],1,9) == df_n5["cd_centro_responsabilidade_cr_n5"], how='left')

# COMMAND ----------

dfa = dfa.withColumn("cd_centro_responsabilidade_cr_n5",
                     when(col("cd_centro_responsabilidade_cr_n5").isNotNull(), dfa["cd_centro_responsabilidade_cr_n5"])\
                     .otherwise(substring(col("CTD_ITEM_id"),1,9)))\
.withColumn("ds_centro_responsabilidade_cr_n5",
            when(col("ds_centro_responsabilidade_cr_n5").isNotNull(), dfa["ds_centro_responsabilidade_cr_n5"])\
            .otherwise(concat(substring(col("CTD_ITEM_id"),1,9),lit("NÃO INFORMADO"))))

# COMMAND ----------

del df_n5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming columns and droping unnecessary columns

# COMMAND ----------

dfa = dfa.withColumnRenamed("CTD_ITEM_ano_ref", "cd_ano_referencia")\
.withColumn('id_registro_oltp', col('R_E_C_N_O_').cast(LongType()))\
.drop("R_E_C_N_O_", "CTD_ITEM", "CTD_DESC01", "CTD_CLASSE", "CTD_ITSUP","CTD_ITSUP_id","CTD_ITEM_id")

# COMMAND ----------

#add control fields from trusted_control_field egg
dfa = tcf.add_control_fields(dfa, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in sink

# COMMAND ----------

dfa.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

dfa.unpersist()

# COMMAND ----------

