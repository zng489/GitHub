# Databricks notebook source
# MAGIC %md
# MAGIC # About trusted area objects:
# MAGIC * these notebooks are very specific to each of the tasks they are performing
# MAGIC * maybe you don't need to use parameters
# MAGIC * don't worry if you're not generalizing enough

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

src_tba, src_tbb,  src_tbc,  src_tbd,  src_tbe,  src_tbf,  src_tbg,  src_tbh, src_tbi, src_tbj, src_tbk, src_tbl, src_tbm, src_tbn, src_tbo = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["raw"],t)  for t in var_tables["origins"]]
print(src_tba, src_tbb,  src_tbc,  src_tbd,  src_tbe,  src_tbf,  src_tbg,  src_tbh, src_tbi, src_tbj, src_tbk, src_tbl, src_tbm, src_tbn, src_tbo)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC Implementing update logic

# COMMAND ----------

import datetime
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, max, to_date, year, month, dayofmonth, dayofweek, date_format, current_date, dense_rank, desc, greatest, trim, when, col, lit, from_unixtime, concat, trim, round
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

first_load = False
var_max_dt_atualizacao = datetime.datetime.strptime("01/01/1800 00:00", "%d/%m/%Y %H:%M")

# COMMAND ----------

# MAGIC %md
# MAGIC From documentation:
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dh_ultima_atualizacao_oltp = SELECT MAX(dh_ultima_atualizacao_oltp) from curso_ensino_profissional
# MAGIC 
# MAGIC This one is the trusted object. If it's the first time, then everything comes!

# COMMAND ----------

try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_atualizacao = df_sink.select(max(col("dh_ultima_atualizacao_oltp"))).collect()[0][0]
except AnalysisException:
  first_load = True

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First thing is to apply filtering for the maximum available var_max_dt_atualizacao.

# COMMAND ----------

classifications = spark.read.parquet(src_tba)
groups = spark.read.parquet(src_tbb)
course_performances = spark.read.parquet(src_tbc)
users_in_groups = spark.read.parquet(src_tbd)
users_extended = spark.read.parquet(src_tbe)
groups_in_groups = spark.read.parquet(src_tbf)
assignments_in_groups = spark.read.parquet(src_tbg)
assignments = spark.read.parquet(src_tbh)
groups_extended = spark.read.parquet(src_tbi)
assignments_in_master = spark.read.parquet(src_tbj)
assignments_status = spark.read.parquet(src_tbk)
users = spark.read.parquet(src_tbl)
performances = spark.read.parquet(src_tbm)
performances_steps = spark.read.parquet(src_tbn)
questionnaire_questions = spark.read.parquet(src_tbo)


# COMMAND ----------

anos = [2019, 2020, 2021, 2022]
# anos = [2019]
li = {2022: ['GESTÃO E MERCADO','FORMAÇÃO CONTINUADA DE DOCENTES E INSTRUTORES','SAÚDE E SEGURANÇA NA INDÚSTRIA','ATUALIZAÇÃO TECNOLÓGICA','INOVAÇÃO E SERVIÇOS TECNOLÓGICOS', 'LIDERANÇA'], 2021: ['GESTÃO E MERCADO','FORMAÇÃO CONTINUADA DE DOCENTES E INSTRUTORES','SAÚDE E SEGURANÇA NA INDÚSTRIA','ATUALIZAÇÃO TECNOLÓGICA','INOVAÇÃO E SERVIÇOS TECNOLÓGICOS','LIDERANÇA'], 2020:['GESTÃO E MERCADO','FORMAÇÃO CONTINUADA DE DOCENTES E INSTRUTORES','SAÚDE E SEGURANÇA NA INDÚSTRIA','ATUALIZAÇÃO TECNOLÓGICA','INOVAÇÃO E SERVIÇOS TECNOLÓGICOS','LIDERANÇA'], 2019: ['SESI EDUCAÇÃO','SENAI - PSAI','SENAI - PSCD','Cursos Livres (Ciclo 1/19)','Cursos Livres (Ciclo 2/19)','Cursos Livres (Ciclo 3/19)','Cursos Livres (Ciclo 4/19)','Cursos Livres (Ciclo 5/19)','FORMAÇÃO CONTINUADA DE DOCENTES E INSTRUTORES','INOVAÇÃO E SERVIÇOS TECNOLÓGICOS','LIDERANÇA','ATUALIZAÇÃO TECNOLÓGICA','GESTÃO E MERCADO','PALESTRAS','SAÚDE E SEGURANÇA NA INDÚSTRIA','SENAI TECNOLOGIA','CNI','SESI EDUCAÇÃO']}

li_groups = {2022:['22', 'Engaja SESI:'], 2021:['21', 'Engaja SESI:'],2020:['20', 'Engaja SESI:'], 2019:['19','Programa de Aperfeiçoamento Consultor SESI em Soluções de Saúde Corporativa (T1/18)']}

# COMMAND ----------

# MAGIC %md
# MAGIC performance

# COMMAND ----------

assignments = assignments.filter(assignments.assignment_type == "lesson")

assignments = assignments.filter(assignments.lesson_id == 1900)

assignments_in_groups =assignments_in_groups.select(col("assignment_id").alias("e_assignment_id"), "group_id")

assigquant = assignments.join(assignments_in_groups, (assignments.assignment_id == assignments_in_groups.e_assignment_id))

assigquant = assigquant.select("group_id", "assignment_id", "lesson_id")


# COMMAND ----------

tz1 = groups_in_groups.filter(groups_in_groups.parent_id == 717)
tz1 = tz1.join(groups, groups.group_id == tz1.group_id).drop(groups.group_id)
tz1 = tz1.select("group_id", "group_name")
tz1 = tz1.join(users_in_groups, users_in_groups.group_id == tz1.group_id).drop(users_in_groups.group_id)
tz1 = tz1.select(col("user_id").alias("e_user_id"), col("group_name").alias('UF'))

tz2 = users_in_groups.filter((users_in_groups.group_id == 377) | (users_in_groups.group_id == 260) | (users_in_groups.group_id == 578))
tz2 = tz2.join(groups, (tz2.group_id == groups.group_id)).drop(groups.group_id)
tz2 = tz2.select(col("user_id").alias("e_user_id"), col("group_name").alias('UF'))
tz1 = tz1.union(tz2)

# COMMAND ----------

display(tz1)

# COMMAND ----------

groups.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Questionario

# COMMAND ----------

performances_steps = performances_steps.where(col("step_question_number").isin({308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318}))
satisfacao = performances_steps.join(questionnaire_questions, questionnaire_questions.question_id == performances_steps.step_question_number)
satisfacao = satisfacao.filter(satisfacao.sub_question == 0)
satisfacao = satisfacao.join(performances, performances.performance_id == satisfacao.performance_id).drop(satisfacao.performance_id)
satisfacao = satisfacao.select("step_question_number", "step_number", "step_frame_number", "answer_text", "question_id", "question_text", "user_id", "performance_id", "lesson_id", "assignment_id")
satisfacao = satisfacao.join(assigquant, (satisfacao.assignment_id == assigquant.assignment_id) & (satisfacao.lesson_id == assigquant.lesson_id)).drop( assigquant.assignment_id).drop(assigquant.lesson_id)

# COMMAND ----------

display(satisfacao)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, LongType, IntegerType, TimestampType
emptyRDD = spark.sparkContext.emptyRDD()

schema = StructType([
  StructField('classification_id', LongType(), True),
  StructField('classification_name', StringType(), True),
  StructField('classification_type', StringType(), True),
  StructField('nm_arq_in', IntegerType(), True),
  StructField('nr_reg', IntegerType(), True),
  StructField('dh_arq_in', TimestampType(), True),
  StructField('dh_insercao_raw', TimestampType(), True),
  StructField('kv_process_control', StructType([
             StructField('adf_factory_name', StringType(), True),
             StructField('adf_pipeline_name', StringType(), True),
             StructField('adf_pipeline_run_id', StringType(), True),
             StructField('adf_trigger_id', StringType(), True),
             StructField('adf_trigger_name', StringType(), True),
             StructField('adf_trigger_time', TimestampType(), True),
             StructField('adf_trigger_type', StringType(), True)
             ])),
   StructField('ano', IntegerType(), True)
  ])
classificationstemp2 = spark.createDataFrame(emptyRDD,schema)
classificationstemp2.printSchema()


schema = StructType([
  StructField('group_id', LongType(), True),
  StructField('group_name', StringType(), True),
  StructField('group_description', StringType(), True),
  StructField('group_summary', StringType(), True),
  StructField('classification_id', LongType(), True),
  StructField('domain_id', LongType(), True),
  StructField('group_open_date', LongType(), True),
  StructField('group_close_date', LongType(), True),
  StructField('nm_arq_in', IntegerType(), True),
  StructField('nr_reg', IntegerType(), True),
  StructField('dh_arq_in', TimestampType(), True),
  StructField('dh_insercao_raw', TimestampType(), True),
  StructField('kv_process_control', StructType([
             StructField('adf_factory_name', StringType(), True),
             StructField('adf_pipeline_name', StringType(), True),
             StructField('adf_pipeline_run_id', StringType(), True),
             StructField('adf_trigger_id', StringType(), True),
             StructField('adf_trigger_name', StringType(), True),
             StructField('adf_trigger_time', TimestampType(), True),
             StructField('adf_trigger_type', StringType(), True)
             ])),
   StructField('anogroup', IntegerType(), True)
  ])
groupstemp2 = spark.createDataFrame(emptyRDD,schema)
groupstemp2.printSchema()

# COMMAND ----------

# classificationstemp2 = spark.createDataFrame(classifications)
# classificationstemp2 = classificationstemp2.filter(classifications.classification_name == '')
for t in anos:
  classificationstemp = classifications.filter(classifications.classification_name.isin(li[t]))
  classificationstemp = classificationstemp.withColumn("ano", lit(t))
  groupstemp = groups.where(
    groups['group_name'].rlike("|".join(["(" + pat + ")" for pat in li_groups[t]]))
  )
  groupstemp = groupstemp.withColumn("anogroup", lit(t))

  classificationstemp2 = classificationstemp2.union(classificationstemp)
  groupstemp2 = groupstemp2.union(groupstemp)
  print(t)
classifications = classificationstemp2
groups = groupstemp2

column_name_mapping = {"classification_id": "id_iniciativa"}
  
for key in column_name_mapping:
  classifications =  classifications.withColumnRenamed(key, column_name_mapping[key])

column_name_mapping = {"field_1": "cargahoraria1", "field_11":"cargahoraria2"}
  
for key in column_name_mapping:
  groups_extended =  groups_extended.withColumnRenamed(key, column_name_mapping[key])
  
resultado = classifications.join(groups, (classifications.id_iniciativa == groups.classification_id) & (classifications.ano == groups.anogroup))

column_name_mapping = {"group_id": "id_group"}
  
for key in column_name_mapping:
  users_in_groups =  users_in_groups.withColumnRenamed(key, column_name_mapping[key])

resultado = resultado.join(users_in_groups, (resultado.group_id == users_in_groups.id_group))

resultado = resultado.join(groups_extended, (resultado.group_id == groups_extended.e_group_id), "left")

resultado = resultado.join(users_extended, (resultado.user_id == users_extended.e_user_id), "left")


resultado = resultado.select("ano", "id_iniciativa", "classification_name", "group_id", "group_name", "group_summary", from_unixtime(col("group_open_date")).alias("data_inicio"), from_unixtime(col("group_close_date")).alias("data_termino"), "user_id", from_unixtime(col("addition_time")).alias("data_matricula"), "cargahoraria1", "cargahoraria2", col("field_3").alias("entidade"), col("field_44").alias("ocupacao"), col("field_46").alias("graduacao"), col("field_48").alias("area"), "field_3")

resultado = resultado.na.fill(value='',subset=["cargahoraria1"])
resultado = resultado.na.fill(value='',subset=["cargahoraria2"])

resultado = resultado.withColumn("cargahorariaespaco", lit(' '))



resultado = resultado.select("ano", "id_iniciativa", "classification_name", "group_id", "group_name", "group_summary", "data_inicio", "data_termino", "user_id", "data_matricula", "cargahoraria1", "cargahoraria2", trim(concat(resultado.cargahoraria1,resultado.cargahorariaespaco, resultado.cargahoraria2)).alias("cargahoraria3"), "entidade", "ocupacao", "graduacao", "area", year("data_matricula").alias('anomatricula'),
    month("data_matricula").alias('mesmatricula'),
    dayofmonth("data_matricula").alias('diamatricula'),
    dayofweek("data_matricula").alias('diasemanamatricula'),
    date_format("data_matricula", "YYYYMM").alias("anomesmatricula"))


resultado = resultado.join(tz1, (resultado.user_id == tz1.e_user_id), "left")

resultado = resultado.select ("ano", "id_iniciativa", "classification_name", "group_id", "group_name", "group_summary", "data_inicio", "data_termino", "user_id", "data_matricula", "cargahoraria1", "cargahoraria2", "cargahoraria3", "entidade", "ocupacao", "graduacao", "area", "anomatricula", 'mesmatricula', 'diamatricula', 'diasemanamatricula', "anomesmatricula", "UF")

                     
users = users.withColumn("nomeespaco", lit(' '))
users = users.select(col("user_id").alias("e_user_id"), trim(concat(users.user_first_name, users.nomeespaco, users.user_last_name)).alias("nome"), col("user_email").alias("email"), col('user_birthdate').alias('data_nascimento'), col('user_gender').alias('sexo'), "employment_date", "user_company")
                           
resultado = resultado.join(users, (resultado.user_id == users.e_user_id))
resultado = resultado.select ("ano", "nome", "email", "data_nascimento", 'sexo', "employment_date", "user_company", "id_iniciativa", "classification_name", "group_id", "group_name", "group_summary", "data_inicio", "data_termino", "user_id", "data_matricula", "cargahoraria1", "cargahoraria2", "cargahoraria3", "entidade", "ocupacao", "graduacao", "area", "anomatricula", 'mesmatricula', 'diamatricula', 'diasemanamatricula', "anomesmatricula", "UF")

resultado = resultado.join(satisfacao, (satisfacao.user_id == resultado.user_id) & (satisfacao.group_id == resultado.group_id)).drop( satisfacao.user_id).drop(satisfacao.group_id)


# COMMAND ----------

# type(log_sessions.data_fim_sessao.values[0])

# COMMAND ----------

# display(log_sessions.filter(log_sessions.data_fim_sessao.isNull()))

# COMMAND ----------

display(resultado)

# COMMAND ----------

if first_load is True:
  df_sink = spark.createDataFrame([], schema=resultado.schema)

# COMMAND ----------

if first_load is True:
  df_sink = spark.createDataFrame([], schema=resultado.schema)

# COMMAND ----------

resultado = resultado.select(df_sink.columns)

# COMMAND ----------

df_sink = df_sink.union(resultado)

# COMMAND ----------

sink

# COMMAND ----------

df_sink.coalesce(1).write.save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------


