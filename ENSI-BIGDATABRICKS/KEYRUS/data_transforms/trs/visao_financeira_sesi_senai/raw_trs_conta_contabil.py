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
# MAGIC Processo	raw_trs_conta_contabil</br>
# MAGIC Tabela/Arquivo Origem	/raw/bdo/protheus11/ct1010</br>
# MAGIC Tabela/Arquivo Destino	/trs/mtd/corp/conta_contabil</br>
# MAGIC Particionamento Tabela/Arquivo Destino	Não há</br>
# MAGIC Descrição Tabela/Arquivo Destino	Conta Contábil é uma estrutura de códigos para apropriação de receita e despesa, sejam elas orçadas ou realizadas, atualmente contemplando SESI e SENAI</br>
# MAGIC Tipo Atualização	F (Substituição Full da tabela: Truncate/Insert)</br>
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw ct1010</br>
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
var_tables =  {"origins": ["/bdo/protheus11/ct1010"], "destination": "/mtd/corp/conta_contabil"}

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
       'adf_pipeline_name': 'raw_trs_conta_contabil',
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

src_ct = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]][0]
print(src_ct)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import trim, col, substring, dense_rank, desc, length, when, concat, lit, from_utc_timestamp, current_timestamp
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, LongType
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

useful_columns = ["CT1_FILIAL", "CT1_CONTA", "CT1_DESC01", "CT1_CTASUP", "R_E_C_N_O_", "D_E_L_E_T_"]

# COMMAND ----------

dfa = spark.read.parquet(src_ct)\
.select(*useful_columns)\
.filter((substring(col("CT1_CONTA"),1,1)).isin('3','4') &\
        (substring(col("CT1_FILIAL"),2,1)).isin('2','3') &\
        (trim(col("D_E_L_E_T_")) != "*"))\
.dropDuplicates()\
.drop("D_E_L_E_T_")  

# COMMAND ----------

dfa = dfa.withColumn("SQ", dense_rank().over(Window.partitionBy("CT1_FILIAL", "CT1_CONTA").orderBy(desc("R_E_C_N_O_"))))

# COMMAND ----------

dfa = dfa.filter(dfa["SQ"] == 1).drop("SQ")

# COMMAND ----------

dfa.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating code-column objects for all levels

# COMMAND ----------

conta1 = dfa.filter(length(trim(col("CT1_CONTA"))) == 1)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n1"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n1"), 
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta2 = dfa.filter(length(trim(col("CT1_CONTA"))) == 2)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n2"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n2"), 
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta3 = dfa.filter(length(trim(col("CT1_CONTA"))) == 4)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n3"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n3"), 
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta4 = dfa.filter(length(trim(col("CT1_CONTA"))) == 6)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n4"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n4"),
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta5 = dfa.filter(length(trim(col("CT1_CONTA"))) == 8)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n5"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n5"), 
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta6 = dfa.filter(length(trim(col("CT1_CONTA"))) == 11)\
.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n6"), 
        trim(col("CT1_DESC01")).alias("ds_conta_contabil_n6"), 
        col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

conta7 = dfa.select(trim(col("CT1_CONTA")).alias("cd_conta_contabil_n7"),
                    trim(col("CT1_DESC01")).alias("ds_conta_contabil_n7"),
                    col("CT1_FILIAL").alias("CT1_FILIAL_x"))\
.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering 

# COMMAND ----------

df_aux = dfa.select(col("CT1_FILIAL").alias("CT1_FILIAL_x"), col("CT1_CTASUP").alias("CT1_CTASUP_x"))

# COMMAND ----------

dfa = dfa.join(df_aux, [dfa["CT1_FILIAL"] == df_aux["CT1_FILIAL_x"], dfa["CT1_CONTA"] == df_aux["CT1_CTASUP_x"]], "left_anti").dropDuplicates()

# COMMAND ----------

#dfa.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Levels development

# COMMAND ----------

# MAGIC %md
# MAGIC Level 1 - CONTA1

# COMMAND ----------

dfa = dfa.join(conta1, 
               [dfa["CT1_FILIAL"] == conta1["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,1) == conta1["cd_conta_contabil_n1"]], 
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

dfa = dfa.withColumn("cd_conta_contabil_n1",
                     when(col("cd_conta_contabil_n1").isNotNull(), dfa["cd_conta_contabil_n1"])\
                     .otherwise(substring(col("CT1_CONTA"),1,1)))\
.withColumn("ds_conta_contabil_n1",
            when(col("ds_conta_contabil_n1").isNotNull(), dfa["ds_conta_contabil_n1"])\
            .otherwise(concat(substring(col("CT1_CONTA"),1,1),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del conta1

# COMMAND ----------

# MAGIC %md
# MAGIC Level 2 - CONTA2

# COMMAND ----------

dfa = dfa.join(conta2,
               [dfa["CT1_FILIAL"] == conta2["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,2) == conta2["cd_conta_contabil_n2"]], 
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

dfa = dfa.withColumn("cd_conta_contabil_n2", 
                     when(col("cd_conta_contabil_n2").isNotNull(), dfa["cd_conta_contabil_n2"])\
                     .otherwise(substring(col("CT1_CONTA"),1,2)))\
.withColumn("ds_conta_contabil_n2", 
                     when(col("ds_conta_contabil_n2").isNotNull(), dfa["ds_conta_contabil_n2"])\
                     .otherwise(concat(substring(col("CT1_CONTA"),1,2),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del conta2

# COMMAND ----------

# MAGIC %md
# MAGIC Level 3 - CONTA3

# COMMAND ----------

dfa = dfa.join(conta3, 
               [dfa["CT1_FILIAL"] == conta3["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,4) == conta3["cd_conta_contabil_n3"]], 
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

dfa = dfa.withColumn("cd_conta_contabil_n3", 
                     when(col("cd_conta_contabil_n3").isNotNull(), dfa["cd_conta_contabil_n3"])\
                     .otherwise(substring(col("CT1_CONTA"),1,4)))\
.withColumn("ds_conta_contabil_n3", 
            when(col("ds_conta_contabil_n3").isNotNull(), dfa["ds_conta_contabil_n3"])\
            .otherwise(concat(substring(col("CT1_CONTA"),1,4),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del conta3

# COMMAND ----------

# MAGIC %md
# MAGIC Level 4 - CONTA4

# COMMAND ----------

dfa = dfa.join(conta4, 
               [dfa["CT1_FILIAL"] == conta4["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,6) == conta4["cd_conta_contabil_n4"]], 
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

dfa = dfa.withColumn("cd_conta_contabil_n4", 
                     when(col("cd_conta_contabil_n4").isNotNull(), dfa["cd_conta_contabil_n4"])\
                     .otherwise(substring(col("CT1_CONTA"),1,6)))\
.withColumn("ds_conta_contabil_n4", 
            when(col("ds_conta_contabil_n4").isNotNull(), dfa["ds_conta_contabil_n4"])\
            .otherwise(concat(substring(col("CT1_CONTA"),1,6),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del conta4

# COMMAND ----------

# MAGIC %md
# MAGIC Level 5 - CONTA5

# COMMAND ----------

dfa = dfa.join(conta5, 
               [dfa["CT1_FILIAL"] == conta5["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,8) == conta5["cd_conta_contabil_n5"]],
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

dfa = dfa.withColumn("cd_conta_contabil_n5", 
                     when(col("cd_conta_contabil_n5").isNotNull(), dfa["cd_conta_contabil_n5"])\
                     .otherwise(substring(col("CT1_CONTA"),1,8)))\
.withColumn("ds_conta_contabil_n5", 
            when(col("ds_conta_contabil_n5").isNotNull(), dfa["ds_conta_contabil_n5"])\
            .otherwise(concat(substring(col("CT1_CONTA"),1,8),lit(" NÃO INFORMADO"))))

# COMMAND ----------

del conta5

# COMMAND ----------

# MAGIC %md
# MAGIC Level 6 - CONTA6

# COMMAND ----------

dfa = dfa.join(conta6, 
               [dfa["CT1_FILIAL"] == conta6["CT1_FILIAL_x"], substring(dfa["CT1_CONTA"],1,11) == conta6["cd_conta_contabil_n6"]],
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

#If cd_conta_contabil_n6 is null, it represents cd_conta_contabil_n5. You didn't see wrong, it actually gets the value from level 5.
#If ds_conta_contabil_n6 is null, it represents ds_conta_contabil_n5. You didn't see wrong, it actually gets the value from level 5.
dfa = dfa.withColumn("cd_conta_contabil_n6", 
                     when(col("cd_conta_contabil_n6").isNotNull(), dfa["cd_conta_contabil_n6"])\
                     .otherwise(dfa["cd_conta_contabil_n5"]))\
.withColumn("ds_conta_contabil_n6", 
            when(col("ds_conta_contabil_n6").isNotNull(), dfa["ds_conta_contabil_n6"])\
            .otherwise(dfa["ds_conta_contabil_n5"]))

# COMMAND ----------

del conta6

# COMMAND ----------

# MAGIC %md
# MAGIC Level 7 - CONTA7

# COMMAND ----------

dfa = dfa.join(conta7, 
               [dfa["CT1_FILIAL"] == conta7["CT1_FILIAL_x"], trim(dfa["CT1_CONTA"]) == conta7["cd_conta_contabil_n7"]], 
               how='left')\
.drop("CT1_FILIAL_x")

# COMMAND ----------

#If cd_conta_contabil_n7 is null, it represents cd_conta_contabil_n6. You didn't see wrong, it actually gets the value from level 6.
#If ds_conta_contabil_n7 is null, it represents ds_conta_contabil_n6. You didn't see wrong, it actually gets the value from level 6.
dfa = dfa.withColumn("cd_conta_contabil_n7", 
                     when(col("cd_conta_contabil_n7").isNotNull(), dfa["cd_conta_contabil_n7"])\
                     .otherwise(dfa["cd_conta_contabil_n6"]))\
.withColumn("ds_conta_contabil_n7",
            when(col("ds_conta_contabil_n7").isNotNull(), dfa["ds_conta_contabil_n7"])
            .otherwise(dfa["ds_conta_contabil_n6"]))

# COMMAND ----------

del conta7

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating and renaming columns

# COMMAND ----------

dfa = dfa.withColumn('cd_entidade_nacional',substring(col("CT1_FILIAL"),2,1).cast(IntegerType()))\
.withColumn('id_registro_oltp', col('R_E_C_N_O_').cast(LongType()))\
.drop("CT1_FILIAL", "R_E_C_N_O_", "CT1_CONTA", "CT1_DESC01", "CT1_CTASUP")\
.dropDuplicates()

# COMMAND ----------

#add control fields from trusted_control_field egg
dfa = tcf.add_control_fields(dfa, var_adf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Salvando tabela no destino

# COMMAND ----------

dfa.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

dfa.unpersist()

# COMMAND ----------

