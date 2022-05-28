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
# MAGIC Processo	trs_biz_fte_producao_educacao_sesi (versão 3.1)
# MAGIC Tabela/Arquivo Origem	"/trs/evt/matricula_educacao_sesi
# MAGIC '/trs/mtd/sesi/curso_educacao_sesi
# MAGIC /trs/evt/matricula_educacao_sesi_situacao
# MAGIC /trs/evt/matricula_educacao_sesi_carga_horaria
# MAGIC /trs/evt/matricula_educacao_sesi_caracteristica"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fte_producao_educacao_sesi
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção SESI de matrículas anuais em ensino 
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento) 
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
# MAGIC Dev: Tiago Shin / Marcela
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
var_tables = {"origins": ["/evt/matricula_educacao_sesi",
                          "/mtd/sesi/curso_educacao_sesi",
                          "/evt/matricula_educacao_sesi_situacao",
                          "/evt/matricula_educacao_sesi_carga_horaria",
                          "/evt/matricula_educacao_sesi_caracteristica",
                          "/evt/matric_educacao_sesi_tipo_vinculo"],
              "destination": "/producao/fte_producao_educacao_sesi",
              "databricks": {
                "notebook": "/biz/educacao_sesi/trs_biz_fte_producao_educacao_sesi"
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
       'adf_pipeline_name': 'trs_biz_fta_producao_educacao_sesi',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-05-27T17:57:06.0829994Z',
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

src_mes, src_ces, src_mess, src_mesch, src_mesc, src_tpv = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_mes, src_ces, src_mess, src_mesch, src_mesc, src_tpv)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, to_date, from_json, coalesce
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
# MAGIC <pre>
# MAGIC /*  
# MAGIC Obter os parâmtros informados pela UNIGEST para fechamento da Produção de Matrículas em Ensino do SESI: 
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
# MAGIC obter #prm_ano_fechamento, #prm_mes_fechamento e #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
# MAGIC */ 
# MAGIC </pre>

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
# MAGIC   Obter as matrículas que estão ou estiveram ativas ao longo do ano:
# MAGIC   - cuja data de entrada seja anterior ao último dia do mês de parâmetro #prm_ano_fechamento + #prm_mes_fechamento
# MAGIC   - cuja data de saída seja posterior ao primeiro dia do ano de parâmetro #prm_ano_fechamento ou a saída ainda não ocorreu
# MAGIC    ======================================================================================================== */
# MAGIC    ( 
# MAGIC      SELECT
# MAGIC      m.cd_entidade_regional,
# MAGIC 	 m.cd_centro_responsabilidade,
# MAGIC      m.cd_matricula_educacao_sesi, 
# MAGIC      m.cd_matricula_educacao_sesi_dr, 
# MAGIC 	 m.dt_saida_prevista,
# MAGIC      m.dt_entrada_prevista,
# MAGIC      m.cd_tipo_financiamento,
# MAGIC      1 AS qt_matricula_educacao_sesi,
# MAGIC      c.cd_produto_servico_educacao_sesi,
# MAGIC          
# MAGIC      -- incluída em 18/12/2020
# MAGIC 	 CASE WHEN m.dt_saida_prevista <= TO_DATE(#prm_ano_fechamento+#prm_mes_fechamento+"ultimo dia") THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_turma_concluida
# MAGIC      
# MAGIC 	 CASE WHEN c.cd_produto_servico_educacao_sesi IN (3,91,270,473,474) THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_foco_industria
# MAGIC      
# MAGIC 	 FROM matricula_educacao_sesi m
# MAGIC 	 
# MAGIC      INNER JOIN curso_educacao_sesi c ON c.cd_curso_educacao_sesi = m.cd_curso_educacao_sesi
# MAGIC          
# MAGIC      WHERE m.dt_saida_prevista >= TO_DATE(#prm_ano_fechamento+01+01) 
# MAGIC      AND c.cd_produto_servico_educacao_sesi NOT IN (270,3) 
# MAGIC 
# MAGIC    ) "MATRICULA"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from matricula_educacao_sesi and apply filters

# COMMAND ----------

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

# COMMAND ----------

useful_columns_matricula_educacao_sesi = ["cd_entidade_regional", "cd_centro_responsabilidade", "cd_matricula_educacao_sesi", "cd_matricula_educacao_sesi_dr", "cd_curso_educacao_sesi", "dt_saida_prevista", "dt_entrada_prevista", "cd_tipo_financiamento"]

# COMMAND ----------

df_mes = spark.read.parquet(src_mes)\
.select(*useful_columns_matricula_educacao_sesi)\
.filter(col("dt_saida_prevista") >= date(var_parameters["prm_ano_fechamento"], 1, 1))\
.drop("dh_atualizacao_entrada_oltp", "dh_atualizacao_saida_oltp")

# COMMAND ----------

#df_mes.count(), df_mes.dropDuplicates().count() #(2375119, 2375119)

# COMMAND ----------

if df_mes.count() == 0:
  dbutils.notebook.exit('{"new_records_is_zero": 1}')

# COMMAND ----------

useful_columns_curso_educacao_sesi = ["cd_curso_educacao_sesi", "cd_produto_servico_educacao_sesi"]

# COMMAND ----------

df_ces = spark.read.parquet(src_ces)\
.select(*useful_columns_curso_educacao_sesi)\
.filter(~(col("cd_produto_servico_educacao_sesi").isin(270,3)))

# COMMAND ----------

#df_ces.count(), df_ces.dropDuplicates().count() #(25941, 25941)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join dataframes

# COMMAND ----------

df_mat = df_mes\
.join(df_ces, ["cd_curso_educacao_sesi"], "inner")\
.withColumn("qt_matricula_educacao_sesi", lit(1))\
.drop("cd_curso_educacao_sesi")

# COMMAND ----------

#df_mat.count(), df_mat.dropDuplicates().count() #(2344285, 2344285)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new columns

# COMMAND ----------

var_mapping = {"qt_matricula_educacao_sesi_foco_industria": col("cd_produto_servico_educacao_sesi").isin(3,91,270,473,474),
               "qt_matricula_educacao_sesi_turma_concluida": col("dt_saida_prevista") <= last_day_of_month(date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"], 1))}

for cl in var_mapping:
  df_mat = df_mat.withColumn(cl, when(var_mapping[cl], lit(1)).otherwise(lit(0)))

# COMMAND ----------

df_mat = df_mat\
.coalesce(4)\
.cache()


# Activate cache
df_mat.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.1

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC  /* =======================================================================================================
# MAGIC   PASSO 1.1 (NOVO) incluído em 03/09/2020
# MAGIC   Para as matrículas encontradas no PASSO 1, obter a última versão de suas características, isto é, do registro 
# MAGIC   de cd_matricula_educacao_sesi cuja vigência seja anterior ao parâmetro de data de corte #prm_data_corte
# MAGIC   ***** Considerar apenas o SQ = 1 ****** 
# MAGIC   ======================================================================================================== */ 
# MAGIC   SELECT 
# MAGIC   cd_matricula_educacao_sesi, 
# MAGIC   qt_matricula_educacao_sesi_gratuidade_reg,
# MAGIC   qt_matricula_educacao_sesi_paga,
# MAGIC   qt_matricula_educacao_sesi_gratuidade_nreg,
# MAGIC   qt_matricula_educacao_sesi_ebep
# MAGIC   FROM
# MAGIC   ( SELECT   
# MAGIC     mc.cd_matricula_educacao_sesi,    	
# MAGIC 	mc.cd_tipo_financiamento,
# MAGIC 	mc.fl_excluido_oltp,    
# MAGIC     CASE WHEN mc.cd_tipo_financiamento = 2              THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_gratuidade_reg,
# MAGIC     CASE WHEN mc.cd_tipo_financiamento = 1              THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_paga,
# MAGIC     CASE WHEN mc.cd_tipo_financiamento = 3              THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_gratuidade_nreg,
# MAGIC     CASE WHEN mc.fl_ebep = 1                            THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_ebep,
# MAGIC 	ROW_NUMBER() OVER(PARTITION BY mc.cd_matricula_educacao_sesi ORDER BY mc.dh_inicio_vigencia DESC) SQ
# MAGIC 	
# MAGIC     FROM matricula_educacao_sesi_caracteristica mc
# MAGIC 	INNER JOIN "MATRICULA" m -- PASSO 1
# MAGIC 	ON mc.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi	
# MAGIC 	WHERE CAST(mc.dh_inicio_vigencia AS DATE) <= #prm_data_corte AND (CAST(mc.dh_fim_vigencia AS DATE) >= #prm_data_corte OR mc.dh_fim_vigencia IS NULL)
# MAGIC   ) "MAT_CARAC" 
# MAGIC   -- FILTRAR SOMENTE SQ = 1 versão na data de corte e que a matrícula não esteja excluída 
# MAGIC   WHERE SQ = 1 AND fl_excluido_oltp = 0
# MAGIC  
# MAGIC ```

# COMMAND ----------

useful_columns_matricula_educacao_sesi_caracteristica = ["cd_matricula_educacao_sesi", "cd_tipo_financiamento", "fl_excluido_oltp", "fl_ebep", "dh_inicio_vigencia", "dh_fim_vigencia"]

# COMMAND ----------

df_mesc = spark.read.parquet(src_mesc)\
.select(*useful_columns_matricula_educacao_sesi_caracteristica)\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) &\
        ((col("dh_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
         (col("dh_fim_vigencia").isNull())))\
.drop("dh_fim_vigencia")

# COMMAND ----------

var_mapping = {"qt_matricula_educacao_sesi_gratuidade_reg": col("cd_tipo_financiamento") == 2,
               "qt_matricula_educacao_sesi_paga": col("cd_tipo_financiamento") == 1,
               "qt_matricula_educacao_sesi_gratuidade_nreg": col("cd_tipo_financiamento") == 3,
               "qt_matricula_educacao_sesi_ebep": col("fl_ebep") == 1}

for cl in var_mapping:
  df_mesc = df_mesc.withColumn(cl, when(var_mapping[cl], lit(1)).otherwise(lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join dataframes

# COMMAND ----------

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dh_inicio_vigencia").desc())

df_mat_carac = df_mesc\
.join(df_mat.select("cd_matricula_educacao_sesi"), ["cd_matricula_educacao_sesi"], "inner")\
.withColumn("row_number", row_number().over(w))\
.filter((col("row_number") == 1) &\
        (col("fl_excluido_oltp") == 0))\
.drop("cd_tipo_financiamento", "fl_excluido_oltp", "fl_ebep", "row_number", "dh_inicio_vigencia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 2
# MAGIC   Para as matrículas encontradas no PASSO 1, obter a última versão de seu status, isto é, do registro 
# MAGIC   de matricula_educacao_sesi cuja vigência seja anterior ao parâmetro de data de corte #prm_data_corte
# MAGIC   ***** Considerar apenas o SQ = 1 ******
# MAGIC   ======================================================================================================== */
# MAGIC SELECT
# MAGIC   cd_matricula_educacao_sesi,
# MAGIC   cd_tipo_situacao_matricula,
# MAGIC   SUM(CASE WHEN YEAR(dt_saida) = #prm_ano_fechamento AND cd_tipo_situacao_matricula IN (11,12) THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_concluinte, 
# MAGIC   SUM(CASE WHEN YEAR(dt_saida) = #prm_ano_fechamento AND cd_tipo_situacao_matricula = 20 THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_transf_turma,
# MAGIC   SUM(CASE WHEN YEAR(dt_saida) = #prm_ano_fechamento AND cd_tipo_situacao_matricula = 19 THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_transf_interna,
# MAGIC 
# MAGIC FROM
# MAGIC (SELECT
# MAGIC       cd_matricula_educacao_sesi,
# MAGIC       cd_tipo_situacao_matricula,   
# MAGIC       CASE WHEN NVL(dt_situacao_saida, TO_DATE('2090-01-01') < dt_saida_prevista THEN dt_situacao_saida ELSE dt_saida_prevista END AS dt_saída,   
# MAGIC  FROM  
# MAGIC  ( SELECT 
# MAGIC      s.cd_matricula_educacao_sesi,
# MAGIC      s.cd_tipo_situacao_matricula,    
# MAGIC      m.dt_saida_prevista
# MAGIC      CASE WHEN S.cd_tipo_situacao_matricula NOT IN (1, 24, 25, 7, 17) THEN s.dh_inicio_vigencia ELSE NULL END AS dt_situacao_saida, --ALTERADO EM 28/04/2021
# MAGIC      ROW_NUMBER() OVER(PARTITION BY cd_matricula_educacao_sesi ORDER BY dh_inicio_vigencia DESC, id_matricula_educacao_sesi_situacao_oltp DESC, dh_ultima_atualizacao_oltp DESC) SQ     --ALTERADO EM 27/04/2021
# MAGIC    FROM matricula_educacao_sesi_situacao s
# MAGIC    INNER JOIN "MATRICULA" m
# MAGIC    ON  s.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi    
# MAGIC 
# MAGIC    --ALTERADO EM 27/04/2021
# MAGIC    WHERE CAST(s.dh_inicio_vigencia as DATE) <= #prm_data_corte 
# MAGIC    AND (NVL(CAST(s.dh_fim_vigencia), '2090-01-01') as DATE >= #prm_data_corte )
# MAGIC    AND (CAST(dh_primeira_atualizacao_oltp as DATE) <= #prm_data_corte)
# MAGIC    AND (
# MAGIC         (fl_excluido == 0 --não está excluido)    
# MAGIC         OR 
# MAGIC         (fl_excluido == 1 --está escluido
# MAGIC          AND dh_ultima_atualizacao_oltp > #prm_data_corte)
# MAGIC        )
# MAGIC  ) "STATUS" 
# MAGIC 
# MAGIC  --ADICIONADO EM 22/03/2021
# MAGIC  WHERE
# MAGIC     SQ = 1 -- FILTRAR SOMENTE SQ = 1
# MAGIC     -- saída ainda não ocorreu ou data de saída posterior ao primeiro dia do ANO de fechamento
# MAGIC     AND( m.dt_situacao_saida IS NULL -- não tenho data de saída ==> contar
# MAGIC     OR ( m.dt_situacao_saida IS NOT NULL -- tenho a data de saída
# MAGIC          AND m.dt_situacao_saida >= TO_DATE(#prm_ano_fechamento+01+01) -- aconteceu no ano do fechamento ==> contar
# MAGIC        )
# MAGIC     )
# MAGIC  )
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only required columns from matricula_educacao_sesi_situacao and apply filters

# COMMAND ----------

useful_columns_matricula_educacao_sesi_situacao = ["cd_matricula_educacao_sesi","cd_tipo_situacao_matricula", "id_matricula_educacao_sesi_situacao_oltp", "dh_inicio_vigencia", "dh_fim_vigencia", "dh_ultima_atualizacao_oltp", "dh_primeira_atualizacao_oltp", "fl_excluido"]

# COMMAND ----------

df_mess = spark.read.parquet(src_mess)\
.select(*useful_columns_matricula_educacao_sesi_situacao)\
.withColumn("dt_situacao_saida", when(col("cd_tipo_situacao_matricula").isin(1, 24, 25, 7, 17)==False, col("dh_inicio_vigencia")).otherwise(lit(None).cast("timestamp")))\
.withColumn("dt_situacao_entrada", when(col("cd_tipo_situacao_matricula").isin(1, 24), col("dh_inicio_vigencia")).otherwise(lit(None).cast("timestamp")))\
.coalesce(8)\
.cache()

# Activate cache
df_mess.count()

# COMMAND ----------

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dh_inicio_vigencia").desc(), col("id_matricula_educacao_sesi_situacao_oltp").desc(), col("dh_ultima_atualizacao_oltp").desc())

df_sit = df_mess\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) & \
        (coalesce(col("dh_fim_vigencia").cast("date"), lit(date(2090, 1, 1))) >= var_parameters["prm_data_corte"]) &\
        (col("dh_primeira_atualizacao_oltp").cast("date") <= var_parameters ["prm_data_corte"]) &\
        ((col("fl_excluido")==0) |\
         ((col("fl_excluido")==1) &\
          (col("dh_ultima_atualizacao_oltp").cast("date") > var_parameters ["prm_data_corte"]))))\
.join(df_mat.select("cd_matricula_educacao_sesi", "dt_saida_prevista"),
                     ["cd_matricula_educacao_sesi"],
                     "inner")\
.withColumn("row_number", row_number().over(w))\
.filter((col("row_number") == 1) &\
        ((col("dt_situacao_saida").isNull()) |\
         ((col("dt_situacao_saida").isNull() == False) &\
          (col("dt_situacao_saida") >= date(var_parameters["prm_ano_fechamento"], 1, 1)))))\
.withColumn("dt_saida", when(coalesce(col("dt_situacao_saida"), lit(date(2090, 1, 1))) < col("dt_saida_prevista"), col("dt_situacao_saida"))\
                         .otherwise(col("dt_saida_prevista")))\
.drop("row_number", "id_matricula_educacao_sesi_situacao_oltp", "dh_ultima_atualizacao_oltp", "dh_primeira_atualizacao_oltp",
      "dh_fim_vigencia", "dh_inicio_vigencia", "dt_situacao_saida", "dt_situacao_entrada", "dt_saida_prevista", "fl_excluido")\
.coalesce(1)\
.cache()

# Activate cache
df_sit.count()

# COMMAND ----------

df_sit_saida = df_sit\
.withColumn("qt_matricula_educacao_sesi_concluinte",
            when((year(col("dt_saida")) == var_parameters["prm_ano_fechamento"]) &\
                 (col("cd_tipo_situacao_matricula").isin(11,12)),
                 lit(1)).otherwise(lit(0)))\
.withColumn("qt_matricula_educacao_sesi_transf_turma", 
            when((year(col("dt_saida")) == var_parameters["prm_ano_fechamento"]) &\
                 (col("cd_tipo_situacao_matricula") == 20),
                 lit(1)).otherwise(lit(0)))\
.withColumn("qt_matricula_educacao_sesi_transf_interna", 
            when((year(col("dt_saida")) == var_parameters["prm_ano_fechamento"]) &\
                 (col("cd_tipo_situacao_matricula") == 19),
                 lit(1)).otherwise(lit(0)))\
.drop("dt_saida")\
.coalesce(1)\
.cache()

# Activate cache
df_sit_saida.count()

# COMMAND ----------

df_sit = df_sit.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.1

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 2.1
# MAGIC   Para as matrículas encontradas no PASSO 1, obter a primeira versão de seu status de entrada, isto é,
# MAGIC   do registro de matricula_educacao_sesi cuja vigência seja anterior ao parâmetro de data de corte #prm_data_corte
# MAGIC   ***** Considerar apenas o SQ = 1 ******
# MAGIC   ======================================================================================================== */
# MAGIC 
# MAGIC --ADICIONADO EM 23/03/2021
# MAGIC 
# MAGIC SELECT cd_matricula_educacao_sesi,           
# MAGIC        CASE WHEN YEAR(dt_entrada) < #prm_ano_fechamento THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_remanescente,
# MAGIC        CASE WHEN YEAR(dt_entrada) = #prm_ano_fechamento THEN 1 ELSE 0 END AS qt_matricula_educacao_sesi_nova,
# MAGIC FROM  
# MAGIC ( SELECT 
# MAGIC     s.cd_matricula_educacao_sesi,
# MAGIC     s.cd_tipo_situacao_matricula,
# MAGIC     CASE WHEN s.dh_inicio_vigencia > m.dt_entrada_prevista THEN s.dh_inicio_vigencia ELSE m.dt_entrada_prevista END AS dt_entrada,    
# MAGIC     m.cd_produto_servico_educacao_sesi,  --ADICIONADO EM 28/04/2021
# MAGIC     ROW_NUMBER() OVER(PARTITION BY cd_matricula_educacao_sesi ORDER BY dh_inicio_vigencia ASC, id_matricula_educacao_sesi_situacao_oltp ASC, dh_primeira_atualizacao_oltp ASC) SQ     --ALTERADO EM 27/04/2021
# MAGIC     FROM matricula_educacao_sesi_situacao s
# MAGIC     INNER JOIN "MATRICULA" m
# MAGIC     ON  s.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi    
# MAGIC     
# MAGIC     --ALTERADO EM 28/04/2021
# MAGIC     WHERE 
# MAGIC     ((m.cd_produto_servico_educacao_sesi IN (450, 570, 470, 471, 571, 573, 574) AND s.cd_tipo_situacao_matricula == 24)
# MAGIC      OR (m.cd_produto_servico_educacao_sesi NOT IN (450, 570, 470, 471, 571, 573, 574) AND s.cd_tipo_situacao_matricula == 1))
# MAGIC     
# MAGIC     --ALTERADO EM 27/04/2021
# MAGIC     AND (
# MAGIC          (fl_excluido == 0 --não está excluido)    
# MAGIC          OR 
# MAGIC          (fl_excluido == 1 --está escluido
# MAGIC           AND dh_ultima_atualizacao_oltp > #prm_data_corte)
# MAGIC         )
# MAGIC  ) "SITUACAO_ENTRADA" 
# MAGIC 
# MAGIC 
# MAGIC  WHERE
# MAGIC     SQ = 1 -- FILTRAR SOMENTE SQ = 1
# MAGIC     -- entrada existente, anterior ao último dia do ANO/MÊS de fechamento que tenha sido setada antes da data de corte
# MAGIC     AND dt_entrada <= TO_DATE(#prm_ano_fechamento+#prm_mes_fechamento+<último dia do mês>)    
# MAGIC     AND CAST(dh_primeira_atualizacao_oltp as DATE) <= #prm_data_corte      --ALTERADO EM 27/04/2021
# MAGIC 
# MAGIC ```

# COMMAND ----------

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dh_inicio_vigencia").asc(), col("id_matricula_educacao_sesi_situacao_oltp").asc(), col("dh_primeira_atualizacao_oltp").asc(),)

df_sit_entrada = df_mess\
.filter((col("cd_tipo_situacao_matricula").isin(1,24)) &\
        ((col("fl_excluido")==0) |\
         ((col("fl_excluido")==1) &\
          (col("dh_ultima_atualizacao_oltp").cast("date") > var_parameters ["prm_data_corte"]))))\
.join(df_mat.select("cd_matricula_educacao_sesi", "dt_entrada_prevista", "cd_produto_servico_educacao_sesi"),
                     ["cd_matricula_educacao_sesi"],
                     "inner")\
.filter(((col("cd_produto_servico_educacao_sesi").isin(450, 570, 470, 471, 571, 573, 574)) &
         (col("cd_tipo_situacao_matricula")==24)) |\
        (~(col("cd_produto_servico_educacao_sesi").isin(450, 570, 470, 471, 571, 573, 574)) &
         (col("cd_tipo_situacao_matricula")==1)))\
.withColumn("dt_entrada", when(col("dh_inicio_vigencia") > col("dt_entrada_prevista"), col("dh_inicio_vigencia"))\
                         .otherwise(col("dt_entrada_prevista")))\
.withColumn("row_number", row_number().over(w))\
.filter((col("row_number") == 1) &\
        (col("dt_entrada") <= last_day_of_month(date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"], 1))) &\
        (col("dh_primeira_atualizacao_oltp").cast("date") <= var_parameters["prm_data_corte"]))\
.withColumn("qt_matricula_educacao_sesi_remanescente",
            when(year(col("dt_entrada")) < var_parameters["prm_ano_fechamento"],
                 lit(1)).otherwise(lit(0)))\
.withColumn("qt_matricula_educacao_sesi_nova",
            when(year(col("dt_entrada")) == var_parameters["prm_ano_fechamento"],
                 lit(1)).otherwise(lit(0)))\
.drop("row_number", "cd_produto_servico_educacao_sesi", "id_matricula_educacao_sesi_situacao_oltp", "dh_ultima_atualizacao_oltp","dh_primeira_atualizacao_oltp",
      "dh_fim_vigencia","dh_inicio_vigencia","dt_situacao_saida", "dt_situacao_entrada", "dt_entrada_prevista","dt_entrada", "cd_tipo_situacao_matricula", "fl_excluido")\
.coalesce(1)\
.cache()


# Activate cache
df_sit_entrada.count()

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
# MAGIC     ch.cd_matricula_educacao_sesi,
# MAGIC 	SUM(NVL(ch.qt_hr_aula,0))             AS qt_hr_aula,
# MAGIC     SUM(CASE WHEN m.cd_tipo_financiamento = 2 THEN NVL(ch.qt_hr_aula,0) ELSE 0 END) AS qt_hr_aula_gratuidade_reg,
# MAGIC     SUM(CASE WHEN m.cd_tipo_financiamento = 3 THEN NVL(ch.qt_hr_aula,0) ELSE 0 END) AS qt_hr_aula_gratuidade_nreg,
# MAGIC     SUM(CASE WHEN m.cd_tipo_financiamento = 1 THEN NVL(ch.qt_hr_aula,0) ELSE 0 END) AS qt_hr_aula_paga,
# MAGIC     SUM(NVL(ch.qt_hr_reconhec_saberes,0)) AS qt_hr_reconhec_saberes,
# MAGIC     SUM(CASE WHEN m.cd_tipo_financiamento = 2 THEN NVL(ch.qt_hr_reconhec_saberes,0) ELSE 0 END) AS qt_hr_recon_sab_gratuidade_reg
# MAGIC     FROM  matricula_educacao_sesi_carga_horaria ch 
# MAGIC     INNER JOIN  "MATRICULA" m  -- PASSO 1
# MAGIC     ON ch.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi	   
# MAGIC     WHERE YEAR(ch.cd_ano_mes_referencia) = #prm_ano_fechamento 
# MAGIC           AND MONTH(ch.cd_ano_mes_referencia) <= #prm_mes_fechamento 
# MAGIC           AND ch.dh_referencia <= #prm_data_corte
# MAGIC     GROUP BY ch.cd_matricula_educacao_sesi
# MAGIC    ) "CARGA_HORARIA"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read only useful columns at matricula_educacao_sesi_carga_horaria and apply filters

# COMMAND ----------

useful_columns_matricula_educacao_sesi_carga_horaria = ["cd_matricula_educacao_sesi", "qt_hr_aula", "qt_hr_reconhec_saberes", "cd_ano_mes_referencia", "dh_referencia"]

# COMMAND ----------

df_car = spark.read.parquet(src_mesch)\
.select(*useful_columns_matricula_educacao_sesi_carga_horaria)\
.filter((year(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) == var_parameters["prm_ano_fechamento"]) &\
        (month(to_date(col("cd_ano_mes_referencia").cast("string"), "yyyyMM")) <= var_parameters["prm_mes_fechamento"]) &\
        (col("dh_referencia").cast("date") <= var_parameters["prm_data_corte"]))\
.drop("dh_referencia", "cd_ano_mes_referencia")\
.join(df_mat.select("cd_tipo_financiamento", "cd_matricula_educacao_sesi").distinct(),
                     ["cd_matricula_educacao_sesi"],
                     "inner")\
.fillna(0, subset=["qt_hr_aula", "qt_hr_reconhec_saberes"])\
.withColumn("qt_hr_aula_gratuidade_reg", when(col("cd_tipo_financiamento") == 2, col("qt_hr_aula")).otherwise(lit(0)))\
.withColumn("qt_hr_aula_gratuidade_nreg", when(col("cd_tipo_financiamento") == 3, col("qt_hr_aula")).otherwise(lit(0)))\
.withColumn("qt_hr_aula_paga", when(col("cd_tipo_financiamento") == 1, col("qt_hr_aula")).otherwise(lit(0)))\
.withColumn("qt_hr_recon_sab_gratuidade_reg", when(col("cd_tipo_financiamento") == 2, col("qt_hr_reconhec_saberes")).otherwise(lit(0)))\
.groupBy("cd_matricula_educacao_sesi")\
.agg(sum("qt_hr_aula").cast("int").alias("qt_hr_aula"),      
     sum("qt_hr_aula_gratuidade_reg").cast("int").alias("qt_hr_aula_gratuidade_reg"), 
     sum("qt_hr_aula_gratuidade_nreg").cast("int").alias("qt_hr_aula_gratuidade_nreg"), 
     sum("qt_hr_aula_paga").cast("int").alias("qt_hr_aula_paga"), 
     sum("qt_hr_reconhec_saberes").cast("int").alias("qt_hr_reconhec_saberes"),
     sum("qt_hr_recon_sab_gratuidade_reg").cast("int").alias("qt_hr_recon_sab_gratuidade_reg"))\
.coalesce(1)\
.cache()

df_car.count()

# COMMAND ----------

#df_car.count(), df_car.dropDuplicates().count() #(355336, 355336)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.1

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 3A (NOVO em 16/12/2020)
# MAGIC   Para as matrículas encontradas no PASSO 1, identificar a quantidade de matrículas cujo vínculo seja de
# MAGIC   trabalhadores na indústria cuja vigência seja anterior ao parâmetro de data de corte #prm_data_corte
# MAGIC   ***** Considerar apenas o SQ = 1 ******
# MAGIC   ======================================================================================================== */
# MAGIC   ( SELECT
# MAGIC     v.cd_matricula_educacao_sesi,
# MAGIC 	v.cd_tipo_vinculo_matricula,
# MAGIC 	ROW_NUMBER() OVER(PARTITION BY v.cd_matricula_educacao_sesi ORDER BY v.dh_ultima_atualizacao_oltp DES) SQ
# MAGIC     FROM  matric_educacao_sesi_tipo_vinculo v
# MAGIC     INNER JOIN  "MATRICULA" m  -- PASSO 1
# MAGIC     ON v.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi	  
# MAGIC    	WHERE CAST(mc.dh_inicio_vigencia AS DATE) <= #prm_data_corte AND (CAST(mc.dh_fim_vigencia AS DATE) >= #prm_data_corte OR mc.dh_fim_vigencia IS NULL)
# MAGIC    ) "VINCULO"
# MAGIC   -- FILTRAR SOMENTE SQ = 1
# MAGIC ```

# COMMAND ----------

useful_columns_matricula_educacao_sesi_tipo_vinculo = ["cd_matricula_educacao_sesi", "cd_tipo_vinculo_matricula", "dh_ultima_atualizacao_oltp", "dh_inicio_vigencia", "dh_fim_vigencia"]

# COMMAND ----------

w = Window.partitionBy("cd_matricula_educacao_sesi").orderBy(col("dh_ultima_atualizacao_oltp").desc())

df_tpv = spark.read.parquet(src_tpv)\
.select(*useful_columns_matricula_educacao_sesi_tipo_vinculo)\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) &\
        ((col("dh_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
         (col("dh_fim_vigencia").cast("date").isNull())))\
.join(df_mat.select("cd_matricula_educacao_sesi"),
      ["cd_matricula_educacao_sesi"],
      "inner")\
.withColumn("row_number", row_number().over(w))\
.filter(col("row_number") == 1)\
.drop("dh_inicio_vigencia", "dh_fim_vigencia", "dh_ultima_atualizacao_oltp", "row_number")\
.coalesce(1)\
.cache()

df_tpv.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 4 (NOVO em 23/06/2020)
# MAGIC   Para as matrículas encontradas no PASSO 1, identificar os reconhecimentos de saberes ocorridos no ano
# MAGIC   cuja data de atualização seja <= a data de corte do fechamento #prm_data_corte
# MAGIC   ======================================================================================================== */  
# MAGIC   ( SELECT DISTINCT s.cd_matricula_educacao_sesi AS cd_matricula_educacao_sesi_recon_sab
# MAGIC        FROM matricula_educacao_sesi_situacao s
# MAGIC     INNER JOIN "MATRICULA" m -- PASSO 1
# MAGIC     ON  s.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi    
# MAGIC     WHERE s.cd_tipo_situacao_matricula = 24 
# MAGIC     AND CAST(s.dh_inicio_vigencia AS DATE) <= (#prm_ano_fechamento, #prm_mes_fechamento, último dia do mês)    
# MAGIC     AND CAST(s.dh_primeira_atualizacao_oltp AS DATE) <= #prm_data_corte     --ALTERADO EM 27/04/2021
# MAGIC 
# MAGIC     --ADICIONADO EM 27/04/2021
# MAGIC     AND (
# MAGIC          (fl_excluido == 0 --não está excluido)    
# MAGIC          OR 
# MAGIC          (fl_excluido == 1 --está escluido
# MAGIC           AND dh_ultima_atualizacao_oltp > #prm_data_corte)
# MAGIC         )
# MAGIC 
# MAGIC  ) "RECON_SAB"
# MAGIC 
# MAGIC ```

# COMMAND ----------

def upper_lower_bound_date(prm_ano_fechamento, prm_mes_fechamento):
  if prm_mes_fechamento == 12:
    prm_mes_fechamento_up =1
    prm_ano_fechamento_up = prm_ano_fechamento + 1
  else:
    prm_mes_fechamento_up = prm_mes_fechamento +1
    prm_ano_fechamento_up = prm_ano_fechamento
  return {"upperbound": datetime.strptime("01/{}/{} 00:00".format(prm_mes_fechamento_up, prm_ano_fechamento_up), "%d/%m/%Y %H:%M"),
          "lowerbound": datetime.strptime("01/01/{} 00:00".format(prm_ano_fechamento), "%d/%m/%Y %H:%M")}

# COMMAND ----------

df_rec_sab = df_mess\
.filter((col("cd_tipo_situacao_matricula") == 24) &
        (col("dh_inicio_vigencia") < upper_lower_bound_date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"])["upperbound"]) &\
        (col("dh_primeira_atualizacao_oltp").cast("date") <= var_parameters["prm_data_corte"]) &\
        ((col("fl_excluido")==0) |\
         ((col("fl_excluido")==1) &\
          (col("dh_ultima_atualizacao_oltp").cast("date") > var_parameters ["prm_data_corte"]))))\
.drop("dh_fim_vigencia", "dh_inicio_vigencia", "id_matricula_educacao_sesi_situacao_oltp","cd_tipo_situacao_matricula", "dh_ultima_atualizacao_oltp",
      "dh_primeira_atualizacao_oltp", "fl_excluido", "dt_situacao_saida", "dt_situacao_entrada")\
.distinct()\
.join(df_mat.select("cd_matricula_educacao_sesi"),
      ["cd_matricula_educacao_sesi"],
      "inner")\
.withColumnRenamed("cd_matricula_educacao_sesi", "cd_matricula_educacao_sesi_recon_sab")\
.coalesce(1)\
.cache()

# Activate cache
df_rec_sab.count()

# COMMAND ----------

df_mess = df_mess.unpersist()

# COMMAND ----------

#df_rec_sab.count(), df_rec_sab.dropDuplicates().count() #(17175, 17175)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC     /* ========================================================================================================
# MAGIC   PASSO 5 (ANTIGO PASSO 4)
# MAGIC   Sumarizar e calcular os indicadores a cruzando as informações colhidas nos passos anteriores e aplicando
# MAGIC   o filtro final
# MAGIC   ======================================================================================================== */   
# MAGIC   SELECT
# MAGIC   #prm_ano_fechamento AS cd_ano_fechamento,
# MAGIC   #prm_mes_fechamento AS cd_mes_fechamento, 
# MAGIC   #prm_data_corte     AS dt_fechamento,
# MAGIC   m.cd_entidade_regional,
# MAGIC   m.cd_centro_responsabilidade,
# MAGIC   m.cd_matricula_educacao_sesi, -- incluída em 12/08/2020
# MAGIC   m.cd_matricula_educacao_sesi_dr, -- incluída em 12/08/2020
# MAGIC   NVL(v.cd_tipo_vinculo_matricula, -98)              AS cd_tipo_vinculo_matricula, -- incluída em 16/12/2020
# MAGIC   
# MAGIC   SUM(m.qt_matricula_educacao_sesi)                  AS qt_matricula_educacao_sesi,
# MAGIC   SUM(m.qt_matricula_educacao_sesi_foco_industria)   AS qt_matricula_educacao_sesi_foco_industria,
# MAGIC   SUM(se.qt_matricula_educacao_sesi_remanescente)    AS qt_matricula_educacao_sesi_remanescente,    -- alterado para se. em 23/03/2021
# MAGIC   SUM(se.qt_matricula_educacao_sesi_nova)            AS qt_matricula_educacao_sesi_nova,            -- alterado para se. em 23/03/2021
# MAGIC   SUM(mc.qt_matricula_educacao_sesi_gratuidade_reg)  AS qt_matricula_educacao_sesi_gratuidade_reg,  -- alterado para mc. em 03/09/2020
# MAGIC   SUM(mc.qt_matricula_educacao_sesi_gratuidade_nreg) AS qt_matricula_educacao_sesi_gratuidade_nreg, -- alterado para mc. em 03/09/2020
# MAGIC   SUM(mc.qt_matricula_educacao_sesi_paga)            AS qt_matricula_educacao_sesi_paga,            -- alterado para mc. em 03/09/2020
# MAGIC   SUM(mc.qt_matricula_educacao_sesi_ebep)            AS qt_matricula_educacao_sesi_ebep,            -- alterado para mc. em 03/09/2020
# MAGIC   SUM(s.qt_matricula_educacao_sesi_concluinte)       AS qt_matricula_educacao_sesi_concluinte,      -- incluido em s. em 23/03/2021
# MAGIC   SUM(s.qt_matricula_educacao_sesi_transf_turma)     AS qt_matricula_educacao_sesi_transf_turma,    -- incluido em s. em 23/03/2021
# MAGIC   SUM(s.qt_matricula_educacao_sesi_transf_interna)   AS qt_matricula_educacao_sesi_transf_interna,  -- incluido em s. em 23/03/2021
# MAGIC   SUM(CASE WHEN rs.cd_matricula_educacao_sesi_recon_sab IS NOT NULL THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_reconhec_saberes, 
# MAGIC   
# MAGIC     --incluído em 18/12/20202
# MAGIC   SUM(CASE WHEN s.cd_tipo_situacao_matricula NOT IN (19, 20) -- pendente de revisão em 2021
# MAGIC       THEN m.qt_matricula_educacao_sesi_turma_concluida ELSE 0 END) AS qt_matricula_educacao_sesi_turma_concluida
# MAGIC       
# MAGIC     --movidos em 23/03/2021 para passo 2.1
# MAGIC   --SUM(CASE WHEN YEAR(m.dt_saida) = #prm_ano_fechamento AND s.cd_tipo_situacao_matricula IN (11,12) THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_concluinte, 
# MAGIC   --SUM(CASE WHEN YEAR(m.dt_saida) = #prm_ano_fechamento AND s.cd_tipo_situacao_matricula = 20 THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_transf_turma,
# MAGIC   --SUM(CASE WHEN YEAR(m.dt_saida) = #prm_ano_fechamento AND s.cd_tipo_situacao_matricula = 19 THEN 1 ELSE 0 END) AS qt_matricula_educacao_sesi_transf_interna,
# MAGIC   
# MAGIC   SUM(ch.qt_hr_aula)                    AS qt_hr_aula,
# MAGIC   SUM(ch.qt_hr_aula_gratuidade_reg)     AS qt_hr_aula_gratuidade_reg,
# MAGIC   SUM(ch.qt_hr_aula_gratuidade_nreg)    AS qt_hr_aula_gratuidade_nreg,
# MAGIC   SUM(ch.qt_hr_aula_paga)               AS qt_hr_aula_paga,
# MAGIC   SUM(ch.qt_hr_reconhec_saberes)        AS qt_hr_reconhec_saberes
# MAGIC   SUM(ch.qt_hr_recon_sab_gratuidade_reg) AS qt_hr_recon_sab_gratuidade_reg
# MAGIC   
# MAGIC   FROM "MATRICULA" m    -- PASSO 1
# MAGIC   
# MAGIC   -- incluído em 03/09/2020
# MAGIC   INNER JOIN "MAT_CARAC" mc    -- PASSO 1.1
# MAGIC   ON mc.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi  
# MAGIC   
# MAGIC   INNER JOIN "STATUS" s -- PASSO 2
# MAGIC   ON s.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi
# MAGIC   
# MAGIC   -- incluído em 23/03/2021
# MAGIC   INNER JOIN "SITUACAO_ENTRADA" se -- PASSO 2.1
# MAGIC   ON se.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi
# MAGIC 
# MAGIC   INNER JOIN "CARGA_HORARIA" ch -- PASSO 3 / alterado de LEFT para INNER em 23/06/2020
# MAGIC   ON ch.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi
# MAGIC 
# MAGIC   LEFT JOIN "VINCULO" v -- PASSO 3A incluído em 16/12/2020
# MAGIC   ON v.cd_matricula_educacao_sesi = m.cd_matricula_educacao_sesi
# MAGIC 
# MAGIC   LEFT JOIN "RECON_SAB" rs -- PASSO 4
# MAGIC   ON rs.cd_matricula_educacao_sesi_recon_sab = m.cd_matricula_educacao_sesi
# MAGIC 
# MAGIC   -- incluída em 08/07/2020: 
# MAGIC   -- se trancamento, que seja posterior ao primeiro dia do ano de fechamento  
# MAGIC   WHERE s.cd_tipo_situacao_matricula <> 5 
# MAGIC     OR (s.cd_tipo_situacao_matricula = 5 AND CAST(s.dh_inicio_vigencia AS DATE) >= TO_DATE(#prm_ano_fechamento+01+01))
# MAGIC  
# MAGIC   GROUP BY 
# MAGIC   m.cd_entidade_regional, 
# MAGIC   m.cd_centro_responsabilidade,
# MAGIC   m.cd_matricula_educacao_sesi,   -- incluída em 12/08/2020
# MAGIC   m.cd_matricula_educacao_sesi_dr -- incluída em 12/08/2020
# MAGIC   NVL(v.cd_tipo_vinculo_matricula, -98) -- incluída em 16/12/2020
# MAGIC   
# MAGIC /* ========================================================================================================
# MAGIC   CONCLUSÃO - Registro único por cd_entidade_regional, cd_centro_responsabilidade:
# MAGIC   cd_ano_fechamento,
# MAGIC   cd_mes_fechamento, 
# MAGIC   dt_fechamento,
# MAGIC   cd_entidade_regional,
# MAGIC   cd_centro_responsabilidade, 
# MAGIC   cd_matricula_educacao_sesi,
# MAGIC   cd_matricula_educacao_sesi_dr,
# MAGIC   qt_matricula_educacao_sesi,
# MAGIC   qt_matricula_educacao_sesi_remanescente,
# MAGIC   qt_matricula_educacao_sesi_nova,
# MAGIC   qt_matricula_educacao_sesi_gratuidade_reg,
# MAGIC   qt_matricula_educacao_sesi_gratuidade_nreg,
# MAGIC   qt_matricula_educacao_sesi_paga,
# MAGIC   qt_matricula_educacao_sesi_ebep,
# MAGIC   qt_matricula_educacao_sesi_foco_industria
# MAGIC   qt_matricula_educacao_sesi_reconhec_saberes,
# MAGIC   qt_matricula_educacao_sesi_turma_concluida, -- em 18/12/2020
# MAGIC   qt_matricula_educacao_sesi_concluinte,
# MAGIC   qt_matricula_educacao_sesi_transf_turma,
# MAGIC   qt_matricula_educacao_sesi_transf_interna,
# MAGIC   qt_hr_aula,
# MAGIC   qt_hr_aula_gratuidade_reg
# MAGIC   qt_hr_aula_gratuidade_nreg
# MAGIC   qt_hr_aula_paga
# MAGIC   qt_hr_reconhec_saberes,
# MAGIC   qt_hr_recon_sab_gratuidade_reg
# MAGIC   ======================================================================================================== */
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Joining all tables

# COMMAND ----------

df_final = df_mat\
.join(df_sit_saida.filter((col("cd_tipo_situacao_matricula") != 5) | \
                          ((col("cd_tipo_situacao_matricula") == 5) & \
                           (col("dh_inicio_vigencia").cast("date") >= date(var_parameters["prm_ano_fechamento"], 1, 1)))),
      ["cd_matricula_educacao_sesi"], "inner")\
.join(df_sit_entrada,["cd_matricula_educacao_sesi"], "inner")\
.join(df_mat_carac,["cd_matricula_educacao_sesi"], "inner")\
.join(df_car,["cd_matricula_educacao_sesi"], "inner")\
.join(df_tpv, ["cd_matricula_educacao_sesi"], "left")\
.join(df_rec_sab, df_mat.cd_matricula_educacao_sesi == df_rec_sab.cd_matricula_educacao_sesi_recon_sab, "left")\
.drop("cd_tipo_financiamento", "dh_inicio_vigencia", "cd_produto_servico_educacao_sesi")\
.coalesce(4)\
.cache()

# Activate cache
df_final.count()

# COMMAND ----------

#df.count(), df.dropDuplicates().count() #(355336, 355336)

# COMMAND ----------

df_mat = df_mat.unpersist()
df_sit_saida = df_sit_saida.unpersist()
df_sit_entrada = df_sit_entrada.unpersist()
df_car = df_car.unpersist()
df_tpv = df_tpv.unpersist()
df_rec_sab = df_rec_sab.unpersist()

# COMMAND ----------

df = df_final

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
# MAGIC Columns transformations before grouping

# COMMAND ----------

var_qt_list =[i for i in df_final.columns if i.startswith("qt_") is True]
    
df = df.fillna(0, subset=var_qt_list)

# COMMAND ----------

df = df.fillna(-98, subset=["cd_tipo_vinculo_matricula"])

# COMMAND ----------

var_mapping = {"qt_matricula_educacao_sesi_reconhec_saberes": (col("cd_matricula_educacao_sesi_recon_sab").isNull() == False)}

for cl in var_mapping:
  df = df.withColumn(cl, when(var_mapping[cl], lit(1)).otherwise(lit(0)))

# COMMAND ----------

df = df.withColumn("qt_matricula_educacao_sesi_turma_concluida", when(col("cd_tipo_situacao_matricula").isin(19, 20), col("qt_matricula_educacao_sesi_turma_concluida")).otherwise(lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy cd_atendimento_matricula_ensino_prof

# COMMAND ----------

df = df.groupBy("cd_entidade_regional", "cd_centro_responsabilidade", "cd_matricula_educacao_sesi", "cd_matricula_educacao_sesi_dr", "cd_tipo_vinculo_matricula")\
.agg(sum("qt_matricula_educacao_sesi").cast("int").alias("qt_matricula_educacao_sesi"),
     sum("qt_matricula_educacao_sesi_remanescente").cast("int").alias("qt_matricula_educacao_sesi_remanescente"),
     sum("qt_matricula_educacao_sesi_nova").cast("int").alias("qt_matricula_educacao_sesi_nova"),
     sum("qt_matricula_educacao_sesi_gratuidade_reg").cast("int").alias("qt_matricula_educacao_sesi_gratuidade_reg"),
     sum("qt_matricula_educacao_sesi_gratuidade_nreg").cast("int").alias("qt_matricula_educacao_sesi_gratuidade_nreg"),
     sum("qt_matricula_educacao_sesi_paga").cast("int").alias("qt_matricula_educacao_sesi_paga"),
     sum("qt_matricula_educacao_sesi_ebep").cast("int").alias("qt_matricula_educacao_sesi_ebep"),
     sum("qt_matricula_educacao_sesi_foco_industria").cast("int").alias("qt_matricula_educacao_sesi_foco_industria"),
     sum("qt_matricula_educacao_sesi_reconhec_saberes").cast("int").alias("qt_matricula_educacao_sesi_reconhec_saberes"),
     sum("qt_matricula_educacao_sesi_turma_concluida").cast("int").alias("qt_matricula_educacao_sesi_turma_concluida"),
     sum("qt_matricula_educacao_sesi_concluinte").cast("int").alias("qt_matricula_educacao_sesi_concluinte"),
     sum("qt_matricula_educacao_sesi_transf_turma").cast("int").alias("qt_matricula_educacao_sesi_transf_turma"),
     sum("qt_matricula_educacao_sesi_transf_interna").cast("int").alias("qt_matricula_educacao_sesi_transf_interna"),
     sum("qt_hr_aula").cast("int").alias("qt_hr_aula"),
     sum("qt_hr_aula_gratuidade_reg").cast("int").alias("qt_hr_aula_gratuidade_reg"),   
     sum("qt_hr_aula_gratuidade_nreg").cast("int").alias("qt_hr_aula_gratuidade_nreg"),   
     sum("qt_hr_aula_paga").cast("int").alias("qt_hr_aula_paga"),   
     sum("qt_hr_reconhec_saberes").cast("int").alias("qt_hr_reconhec_saberes"),
     sum("qt_hr_recon_sab_gratuidade_reg").cast("int").alias("qt_hr_recon_sab_gratuidade_reg"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Add columns

# COMMAND ----------

var_mapping = {"cd_ano_fechamento": lit(var_parameters["prm_ano_fechamento"]).cast("int"),
               "cd_mes_fechamento": lit(var_parameters["prm_mes_fechamento"]).cast("int"),
               "dt_fechamento": lit(var_parameters ["prm_data_corte"]).cast("date")}

for cl in var_mapping:
  df = df.withColumn(cl, var_mapping[cl])

# COMMAND ----------

#Add control fields from trusted_control_field egg. Define layer="biz" to insert dh_insercao_biz
df = tcf.add_control_fields(df, var_adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write on ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Dynamic overwrite will guarantee that only this view will be updated by cd_ano_fechamento and cd_mes_fechamento
# MAGIC We use coalesce equals to 1 to avoid large files
# MAGIC </pre>

# COMMAND ----------

df.coalesce(4).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_final.unpersist()

# COMMAND ----------

