# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About Business area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	trs_biz_fte_producao_ensino_profissional
# MAGIC Tabela/Arquivo Origem	/trs/evt/matricula_ensino_profissional /trs/evt/matricula_ensino_prof_situacao /trs/evt/matricula_ensino_prof_carga_horaria
# MAGIC Tabela/Arquivo Destino	/biz/producao/fte_producao_ensino_profissional
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção SENAI de matrículas anuais em ensino profissional
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Tiago Shin
# MAGIC      05/08/20 - Marcela - Agregação por matricula. Mudança do nome para fte
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business specific parameter section

# COMMAND ----------

import json
import re
from datetime import datetime, timedelta, date
import collections

# COMMAND ----------

var_tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_user_parameters = json.loads(re.sub("\'", '\"', dbutils.widgets.get("user_parameters")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_tables = {"origins": ["/evt/matricula_ensino_profissional",
                          "/evt/matricula_ensino_prof_situacao",
                          "/evt/matricula_ensino_prof_carga_horaria"],
              "destination": "/producao/fte_producao_ensino_profissional",
              "databricks": {
                "notebook": "/biz/educacao_senai/trs_biz_fte_producao_ensino_profissional"
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
       'adf_pipeline_name': 'trs_biz_fte_producao_ensino_profissional',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-10-09T11:25:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 6, "dt_closing": "2020-07-16"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_mep, src_meps, src_mepch = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_mep, src_meps, src_mepch)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, to_date, coalesce
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql.utils import AnalysisException
from trs_control_field import trs_control_field as tcf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get parameters prm_ano_fechamento, prm_mes_fechamento and prm_data_corte

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION <b>
# MAGIC ```
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento da Produção de Matrículas em Ensino Profissional do SENAI do mês: 
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC ```

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
# MAGIC ### Step 1

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 1
# MAGIC   Obter a última versão da situação da matrícula, isto é, do registro de matricula_ensino_prof_situacao cuja vigência 
# MAGIC   seja anterior ao parâmetro de data de corte #prm_data_corte e verifica se ela atende aos filtros de seleção de
# MAGIC   fechamento do mês
# MAGIC   - cuja data de entrada seja anterior ao último dia do mês de parâmetro #prm_ano_fechamento + #prm_mes_fechamento
# MAGIC   - cuja data de saída seja posterior ao primeiro dia do ano de parâmetro #prm_ano_fechamento ou a saída ainda não ocorreu
# MAGIC 
# MAGIC   ***** Considerar apenas o SQ = 1 ******
# MAGIC   ======================================================================================================== */
# MAGIC   ( 
# MAGIC     SELECT * FROM
# MAGIC      ( 
# MAGIC 	   SELECT 
# MAGIC    	   s.cd_atendimento_matricula_ensino_prof,
# MAGIC        s.cd_tipo_situacao_matricula,
# MAGIC    	   s.dt_saida, 
# MAGIC 	   ROW_NUMBER() OVER(PARTITION BY s.cd_atendimento_matricula_ensino_prof ORDER BY s.dh_inicio_vigencia DESC) SQ,
# MAGIC 	   CASE WHEN s.cd_tipo_gratuidade IN (1, 101, 102, 103) THEN 1 ELSE 0 END AS fl_gratuidade_regimental, --alterado em 11/02/2021
# MAGIC 	   s.fl_excluido_oltp,
# MAGIC 	   s.dt_entrada
# MAGIC 	   
# MAGIC    	   FROM matricula_ensino_prof_situacao s
# MAGIC        WHERE CAST(s.dh_inicio_vigencia AS DATE) <= #prm_data_corte AND (CAST(s.dh_fim_vigencia AS DATE) >= #prm_data_corte OR s.dh_fim_vigencia IS NULL)
# MAGIC      )
# MAGIC  
# MAGIC     WHERE SQ = 1
# MAGIC     AND NVL(dt_saida,'2090-12-31') >= TO_DATE(#prm_ano_fechamento, 01 (janeiro), 01 (primeiro dia))
# MAGIC 	AND fl_excluido_oltp = 0  
# MAGIC 	AND dt_entrada <= TO_DATE(#prm_ano_fechamento+#prm_mes_fechamento+<último dia do mês>)
# MAGIC 
# MAGIC ) "STATUS" 
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from matricula_ensino_prof_situacao and apply filters

# COMMAND ----------

useful_columns_matricula_ensino_prof_situacao = ["cd_atendimento_matricula_ensino_prof","cd_tipo_situacao_matricula", "dh_inicio_vigencia", "dh_fim_vigencia", "dt_saida", "cd_tipo_gratuidade", "fl_excluido_oltp", "dt_entrada"]

# COMMAND ----------

df_sit = spark.read.parquet(src_meps)\
.select(*useful_columns_matricula_ensino_prof_situacao)\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) & \
        ((col("dh_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
         (col("dh_fim_vigencia").isNull())))\
.withColumn("fl_gratuidade_regimental", when(col("cd_tipo_gratuidade").isin(1, 101, 102, 103), lit(1)).otherwise(lit(0)))\
.drop("dh_fim_vigencia", "cd_tipo_gratuidade")

# COMMAND ----------

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only last record by cd_atendimento_matricula_ensino_prof ordered by dh_inicio_vigencia

# COMMAND ----------

w = Window.partitionBy("cd_atendimento_matricula_ensino_prof").orderBy(col("dh_inicio_vigencia").desc())
df_sit = df_sit.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("row_number", "dh_inicio_vigencia")\
.filter((coalesce("dt_saida",lit('2090-12-31').cast("date")) >= date(var_parameters["prm_ano_fechamento"], 1, 1)) &\
        (col("fl_excluido_oltp")==0) &\
        (col("dt_entrada") <= last_day_of_month(date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"], 1))))\
.coalesce(4)\
.cache()

df_sit.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 2
# MAGIC   Obter os dados das matrículas selecionadas no PASSO 1
# MAGIC     ======================================================================================================== */
# MAGIC    ( SELECT
# MAGIC      cd_entidade_regional,
# MAGIC 	 cd_centro_responsabilidade,
# MAGIC      cd_atendimento_matricula_ensino_prof,
# MAGIC 	 cd_matricula_ensino_prof_dr, 
# MAGIC    	 CASE WHEN NVL(cd_tipo_necessidade_especial, 9) <> 9 THEN 1 ELSE 0 END AS fl_matricula_necessidade_especial,
# MAGIC 	 1                                                                     AS qt_matricula_ensino_prof
# MAGIC      
# MAGIC 	 FROM matricula_ensino_profissional m
# MAGIC      INNER JOIN "STATUS" s
# MAGIC      ON  s.cd_atendimento_matricula_ensino_prof = m.cd_atendimento_matricula_ensino_prof	
# MAGIC    ) "MATRICULA"
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from matricula_ensino_profissional and apply filters

# COMMAND ----------

useful_columns_matricula_ensino_profissional = ["cd_atendimento_matricula_ensino_prof", "cd_centro_responsabilidade", "cd_entidade_regional", "cd_matricula_ensino_prof_dr", "cd_tipo_necessidade_especial"]

# COMMAND ----------

df_mat = spark.read.parquet(src_mep)\
.select(*useful_columns_matricula_ensino_profissional)\
.withColumn("fl_matricula_necessidade_especial", 
            when(when(col("cd_tipo_necessidade_especial").isNull(), 9)\
                 .otherwise(col("cd_tipo_necessidade_especial")) != 9,
                 lit(1))\
            .otherwise(lit(0)))\
.withColumn("qt_matricula_ensino_prof", lit(1))\
.drop("cd_tipo_necessidade_especial")

# COMMAND ----------

# MAGIC %md
# MAGIC Join matricula_ensino_prof_situacao with the matricula_ensino_profissional 

# COMMAND ----------

df_mat = df_mat.join(df_sit, ["cd_atendimento_matricula_ensino_prof"], "inner")\
.select("cd_entidade_regional", "cd_centro_responsabilidade", "cd_atendimento_matricula_ensino_prof", "cd_matricula_ensino_prof_dr", "fl_matricula_necessidade_especial", "qt_matricula_ensino_prof")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC  /* ========================================================================================================
# MAGIC   PASSO 3
# MAGIC   Para as matrículas encontradas no PASSO 1, calcular os indicadores de carga horária acumulado do ano para 
# MAGIC   ano de fechamento até o mês de fechamento, cujos valores sumarizados sejam anteriores ao parâmetro de data 
# MAGIC   de corte #prm_data_corte
# MAGIC   ======================================================================================================== */ 
# MAGIC   ( SELECT
# MAGIC     ch.cd_atendimento_matricula_ensino_prof,
# MAGIC     SUM(ch.qt_hr_hora_aluno)    AS qt_hr_escolar,
# MAGIC     SUM(ch.qt_hr_aluno_hora)    AS qt_hr_escolar_descontada_falta,
# MAGIC     SUM(ch.qt_hr_pratica_prof)  AS qt_hr_pratica_prof,
# MAGIC     SUM(ch.qt_hr_estagio)       AS qt_hr_estagio
# MAGIC     FROM  matricula_ensino_prof_carga_horaria ch
# MAGIC     INNER JOIN  "STATUS" s  -- PASSO 1 | alterado em 13/07/2020 de MATRICULA para STATUS uma vez que este é mais restritivo
# MAGIC     ON ch.cd_atendimento_matricula_ensino_prof = s.cd_atendimento_matricula_ensino_prof	  
# MAGIC     WHERE YEAR(ch.cd_ano_mes_referencia) = #prm_ano_fechamento
# MAGIC           AND MONTH(ch.cd_ano_mes_referencia) <= #prm_mes_fechamento
# MAGIC           AND CAST(ch.dh_referencia AS DATE) <= #prm_data_corte
# MAGIC     GROUP BY ch.cd_atendimento_matricula_ensino_prof
# MAGIC    ) "CARGA_HORARIA" 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only useful columns at matricula_ensino_prof_carga_horaria and apply filters

# COMMAND ----------

useful_columns_matricula_ensino_prof_carga_horaria = ["cd_atendimento_matricula_ensino_prof", "cd_ano_mes_referencia", "qt_hr_aluno_hora", "qt_hr_estagio", "qt_hr_pratica_prof", "qt_hr_hora_aluno", "dh_referencia"]

# COMMAND ----------

df_car = spark.read.parquet(src_mepch)\
.select(*useful_columns_matricula_ensino_prof_carga_horaria)\
.filter((year(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) == var_parameters["prm_ano_fechamento"]) &\
        (month(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) <= var_parameters["prm_mes_fechamento"]) &\
        (col("dh_referencia") <= var_parameters["prm_data_corte"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Now, group by cd_atendimento_matricula_ensino_pro and cd_ano_mes_referencia and sum the indicators

# COMMAND ----------

df_car = df_car.groupBy("cd_atendimento_matricula_ensino_prof")\
.agg(sum("qt_hr_aluno_hora").cast("int").alias("qt_hr_escolar_descontada_falta"), 
     sum("qt_hr_estagio").cast("int").alias("qt_hr_estagio"),
     sum("qt_hr_pratica_prof").cast("int").alias("qt_hr_pratica_prof"), 
     sum("qt_hr_hora_aluno").cast("int").alias("qt_hr_escolar"))

# COMMAND ----------

# MAGIC %md
# MAGIC With the grouped dataframe, we can now join with matricula and select useful columns

# COMMAND ----------

df_car = df_sit.join(df_car, ["cd_atendimento_matricula_ensino_prof"], "inner")\
.select("cd_atendimento_matricula_ensino_prof", "qt_hr_escolar", "qt_hr_escolar_descontada_falta", "qt_hr_pratica_prof", "qt_hr_estagio")

# COMMAND ----------

#df_car.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC  /* ========================================================================================================
# MAGIC   PASSO 4
# MAGIC   Sumarizar e calcular os indicadores a cruzando as informações colhidas nos passos anteriores e aplicando
# MAGIC   o filtro final
# MAGIC   ======================================================================================================== */   
# MAGIC   SELECT
# MAGIC   #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC   #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC   #prm_data_corte     AS dt_fechamento,
# MAGIC   m.cd_entidade_regional,
# MAGIC   m.cd_centro_responsabilidade,
# MAGIC   m.cd_atendimento_matricula_ensino_prof,
# MAGIC   m.cd_matricula_ensino_prof_dr,
# MAGIC   SUM(m.qt_matricula_ensino_prof)                                           AS qt_matricula_ensino_prof,
# MAGIC   SUM(CASE WHEN NVL(s.cd_tipo_situacao_matricula, 0) = 2 THEN 1 ELSE 0 END) AS qt_matricula_ensino_prof_concluinte,
# MAGIC   
# MAGIC   -- em 03/09/2020 o campo fl_gratuidade_regimental passou a vir da tabela de status e não maois da de matrícula
# MAGIC   SUM(m.qt_matricula_ensino_prof*s.fl_gratuidade_regimental)                AS qt_matricula_ensino_prof_gratuidade_reg,
# MAGIC   
# MAGIC   SUM(m.qt_matricula_ensino_prof*m.fl_matricula_necessidade_especial)       AS qt_matricula_ensino_prof_necsdd_especial,
# MAGIC   
# MAGIC   -- em 07/10/2020 o campo de data de entrada passou a vir da tabela de status e não maois da de matrícula, por isso setado somente aqui no passo 4
# MAGIC   SUM(CASE WHEN YEAR(dt_entrada) < #prm_ano_fechamento THEN m.qt_matricula_ensino_prof ELSE 0 END) AS qt_matricula_ensino_prof_remanescente,
# MAGIC   SUM(CASE WHEN YEAR(dt_entrada) = #prm_ano_fechamento THEN m.qt_matricula_ensino_prof ELSE 0 END) AS qt_matricula_ensino_prof_nova
# MAGIC   --SUM(m.qt_matricula_ensino_prof_remanescente)                              AS qt_matricula_ensino_prof_remanescente, 
# MAGIC   --SUM(m.qt_matricula_ensino_prof_nova)                                      AS qt_matricula_ensino_prof_nova,
# MAGIC   
# MAGIC   SUM(ch.qt_hr_escolar)                                                     AS qt_hr_escolar, 
# MAGIC   
# MAGIC   -- em 03/09/2020 o campo fl_gratuidade_regimental passou a vir da tabela de status e não maois da de matrícula
# MAGIC   SUM(ch.qt_hr_escolar*s.fl_gratuidade_regimental)                          AS qt_hr_escolar_gratuidade_reg,
# MAGIC   
# MAGIC   SUM(ch.qt_hr_escolar_descontada_falta)                                    AS qt_hr_escolar_descontada_falta,
# MAGIC   SUM(ch.qt_hr_pratica_prof)                                                AS qt_hr_pratica_prof, 
# MAGIC   SUM(ch.qt_hr_estagio)                                                     AS qt_hr_estagio
# MAGIC   
# MAGIC   FROM "MATRICULA" m
# MAGIC   INNER JOIN "STATUS" s
# MAGIC   ON s.cd_atendimento_matricula_ensino_prof = m.cd_atendimento_matricula_ensino_prof
# MAGIC   LEFT JOIN "CARGA_HORARIA" ch
# MAGIC   c.cd_atendimento_matricula_ensino_prof = m.cd_atendimento_matricula_ensino_prof
# MAGIC   
# MAGIC   WHERE 
# MAGIC   -- só contabiliza matrículas que não estejam congeladas (não finalizadas, mas sem produção em carga horária)
# MAGIC   (s.cd_tipo_situacao_matricula <> 32 OR (s.cd_tipo_situacao_matricula = 32 AND ch.qt_hr_escolar > 0))
# MAGIC  
# MAGIC  GROUP BY m.cd_entidade_regional, m.cd_centro_responsabilidade, m.cd_atendimento_matricula_ensino_prof, m.cd_matricula_ensino_prof_dr 
# MAGIC 
# MAGIC   
# MAGIC /* ========================================================================================================
# MAGIC   CONCLUSÃO - Registro único por cd_entidade_regional, cd_centro_responsabilidade:
# MAGIC   cd_ano_fechamento,
# MAGIC   cd_mes_fechamento, 
# MAGIC   dt_fechamento,
# MAGIC   cd_entidade_regional,
# MAGIC   cd_centro_responsabilidade, 
# MAGIC   qt_matricula_ensino_prof,
# MAGIC   qt_matricula_ensino_prof_gratuidade_reg,
# MAGIC   qt_matricula_ensino_prof_concluinte,
# MAGIC   qt_matricula_ensino_prof_necsdd_especial,
# MAGIC   qt_matricula_ensino_prof_remanescente,
# MAGIC   qt_matricula_ensino_prof_nova
# MAGIC   qt_hr_escolar,
# MAGIC   qt_hr_escolar_gratuidade_reg,
# MAGIC   qt_hr_escolar_descontada_falta,
# MAGIC   qt_hr_pratica_prof,
# MAGIC   qt_hr_estagio
# MAGIC   ======================================================================================================== */
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Joining all tables

# COMMAND ----------

df = df_mat\
.join(df_sit, ["cd_atendimento_matricula_ensino_prof"], "left")\
.join(df_car,["cd_atendimento_matricula_ensino_prof"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC Filter by cd_tipo_situacao_matricula

# COMMAND ----------

df = df.filter((col("cd_tipo_situacao_matricula") != 32) |\
               ((col("cd_tipo_situacao_matricula") == 32) &\
                (col("qt_hr_escolar") > 0)))

# COMMAND ----------

# MAGIC %md
# MAGIC Columns transformations before grouping

# COMMAND ----------

var_qt_list =[]
for i in df.columns:
  if i.startswith("qt_") is True:
    var_qt_list.append(i)
    
df = df.fillna(0, subset=var_qt_list)

# COMMAND ----------

df = df.withColumn("cd_tipo_situacao_matricula", when(col("cd_tipo_situacao_matricula").isNull(), lit(0))\
                   .otherwise(col("cd_tipo_situacao_matricula")))\
.withColumn("qt_matricula_ensino_prof_concluinte", when(col("cd_tipo_situacao_matricula") == 2, lit(1))\
            .otherwise(lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy cd_atendimento_matricula_ensino_prof

# COMMAND ----------

df = df.groupBy("cd_entidade_regional", "cd_centro_responsabilidade", "cd_atendimento_matricula_ensino_prof", "cd_matricula_ensino_prof_dr")\
.agg(sum("qt_matricula_ensino_prof").cast("int").alias("qt_matricula_ensino_prof"), 
     sum("qt_matricula_ensino_prof_concluinte").cast("int").alias("qt_matricula_ensino_prof_concluinte"),
     sum((col("qt_matricula_ensino_prof")*col("fl_gratuidade_regimental"))).cast("int").alias("qt_matricula_ensino_prof_gratuidade_reg"),
     sum((col("qt_matricula_ensino_prof")*col("fl_matricula_necessidade_especial"))).cast("int").alias("qt_matricula_ensino_prof_necsdd_especial"),
     sum(when(year("dt_entrada") < var_parameters["prm_ano_fechamento"], col("qt_matricula_ensino_prof")).otherwise(lit(0))).cast("int").alias("qt_matricula_ensino_prof_remanescente"),
     sum(when(year("dt_entrada") == var_parameters["prm_ano_fechamento"], col("qt_matricula_ensino_prof")).otherwise(lit(0))).cast("int").alias("qt_matricula_ensino_prof_nova"),
     sum("qt_hr_escolar").cast("int").alias("qt_hr_escolar"),
     sum((col("qt_hr_escolar")*col("fl_gratuidade_regimental"))).cast("int").alias("qt_hr_escolar_gratuidade_reg"),
     sum("qt_hr_escolar_descontada_falta").cast("int").alias("qt_hr_escolar_descontada_falta"),
     sum("qt_hr_pratica_prof").cast("int").alias("qt_hr_pratica_prof"),
     sum("qt_hr_estagio").cast("int").alias("qt_hr_estagio"))

# COMMAND ----------

# MAGIC %md
# MAGIC Add columns

# COMMAND ----------

var_mapping = {"cd_ano_fechamento": lit(var_parameters["prm_ano_fechamento"]).cast("int"),
               "cd_mes_fechamento": lit(var_parameters["prm_mes_fechamento"]).cast("int"),
               "dt_fechamento": lit(var_parameters["prm_data_corte"]).cast("date")}

for cl in var_mapping:
  df = df.withColumn(cl, var_mapping[cl])

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

#df.count()

# COMMAND ----------

#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write on ADLS

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
# MAGIC <pre>
# MAGIC Dynamic overwrite will guarantee that only this view will be updated by cd_ano_fechamento and cd_mes_fechamento
# MAGIC We use coalesce equals to 4 to avoid saving multiple files on adls
# MAGIC </pre>

# COMMAND ----------

df.repartition(4).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_sit.unpersist()