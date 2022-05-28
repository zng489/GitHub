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
# MAGIC Processo	trs_biz_fte_producao_tecnologia_inovacao (versao 02.0)
# MAGIC Tabela/Arquivo Origem	"/trs/evt/atendimento_tecnologia_informacao
# MAGIC /trs/evt/atendimento_tecnologia_inovacao_producao
# MAGIC /trs/evt/atendimento_tecnologia_inovacao_situacao
# MAGIC /trs/mtd/senai/pessoa_juridica"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fte_producao_tecnologia_inovacao
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção SENAI de atendimentos de serviços tecnológicos e inovação
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
var_tables = {"origins": ["/evt/atendimento_tecnologia_inovacao",
                         "/evt/atendimento_tecnologia_inovacao_situacao",
                         "/mtd/senai/pessoa_juridica",
                         "/evt/atendimento_tecnologia_inovacao_producao"],
              "destination": "/producao/fte_producao_tecnologia_inovacao",
              "databricks": {
                "notebook": "/biz/sti_senai/trs_biz_fte_producao_tecnologia_inovacao"
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
       'adf_pipeline_name': 'trs_biz_fte_producao_tecnologia_inovacao',
       'adf_pipeline_run_id': 'p1',
       'adf_trigger_id': 't1',
       'adf_trigger_name': 'author_dev',
       'adf_trigger_time': '2020-10-09T11:35:06.0829994Z',
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

src_ati, src_atis, src_pj, src_atip = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]
print(src_ati, src_atis, src_pj, src_atip)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, first, desc, asc, count, coalesce
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
# MAGIC Obter os parâmetros informados pela UNIGEST para fechamento da Produção de Serviços de Tecnologia e Inovação
# MAGIC do SENAI do mês: #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou 
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
# MAGIC   Obter os atendimentos que estão ou estiveram ativos ao longo do ano:
# MAGIC   - cuja data de inicio seja anterior ao último dia do mês de parâmetro #prm_ano_fechamento + #prm_mes_fechamento
# MAGIC   - cuja data de fim seja posterior ao primeiro dia do ano de parâmetro #prm_ano_fechamento ou a saída ainda não ocorreu
# MAGIC    ======================================================================================================== */
# MAGIC  ---- ALTERADO DIA 08/10  - INICIO
# MAGIC  ( 
# MAGIC      SELECT
# MAGIC      cd_atendimento_sti,
# MAGIC      cd_tipo_situacao_atendimento_sti,
# MAGIC      dt_termino_atendimento,   
# MAGIC      ROW_NUMBER() OVER(PARTITION BY cd_atendimento_sti ORDER BY dh_inicio_vigencia DESC) as SQ
# MAGIC      FROM atendimento_tecnologia_inovacao_situacao 
# MAGIC      WHERE 
# MAGIC          CAST(dh_inicio_vigencia AS DATE) <= #prm_data_corte   
# MAGIC 	AND
# MAGIC          (CAST(dh_fim_vigencia AS DATE) is null 
# MAGIC 	OR
# MAGIC          CAST(dh_fim_vigencia AS DATE) >= #prm_data_corte)      
# MAGIC 	 -- não foi excluído ou foi excluído após a data de corte
# MAGIC 	 AND((fl_excluido_oltp = 0 OR (fl_excluido_oltp = 1 AND CAST(dh_inicio_vigencia AS DATE) > #prm_data_corte) ))  
# MAGIC      -- inicio anterior ao último dia do ANO/MÊS de fechamento
# MAGIC         AND dt_inicio <= TO_DATE(#prm_ano_fechamento+#prm_mes_fechamento+<último dia do mês>)
# MAGIC         AND nvl(dt_termino_atendimento,'2090-12-31') >= TO_DATE(#prm_ano_fechamento+01+01) 
# MAGIC   ) "STATUS"
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Defining function that will be used in processing

# COMMAND ----------

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  
    return next_month - timedelta(days=next_month.day)

# COMMAND ----------

# MAGIC %md
# MAGIC Read and filter atendimento tecnologia inovacao situacao

# COMMAND ----------

var_useful_columns_atendimento_tecnologia_inovacao_situacao = ["cd_atendimento_sti", "cd_tipo_situacao_atendimento_sti", "dh_inicio_vigencia", "dh_fim_vigencia", "dt_termino_atendimento", "dt_inicio_atendimento", "fl_excluido_oltp"]

# COMMAND ----------

w = Window.partitionBy("cd_atendimento_sti").orderBy(col("dh_inicio_vigencia").desc())

df_status = spark.read.parquet(src_atis)\
.select(*var_useful_columns_atendimento_tecnologia_inovacao_situacao)\
.filter((col("dh_inicio_vigencia").cast("date") <= var_parameters["prm_data_corte"]) &\
         ((col("dh_fim_vigencia").cast("date") >= var_parameters["prm_data_corte"]) |\
          (col("dh_fim_vigencia").isNull())) &\
        ((col("fl_excluido_oltp") == 0) |\
         ((col("fl_excluido_oltp") == 1) &\
          (col("dh_inicio_vigencia").cast("date") > var_parameters["prm_data_corte"]))) &\
        (col("dt_inicio_atendimento") <= last_day_of_month(date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"], 1))) &\
        (coalesce("dt_termino_atendimento",lit('2090-12-31').cast("date")) >= date(var_parameters["prm_ano_fechamento"], 1, 1)))\
.withColumn("row_number", row_number().over(w))\
.drop("dh_fim_vigencia", "dh_inicio_vigencia", "fl_excluido_oltp", "dt_inicio_atendimento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC <b> FROM DOCUMENTATION </b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 2
# MAGIC   Para os atendimentos encontrados no PASSO 1, obter a última versão de seu status, isto é, do registro 
# MAGIC   de atendimento_tecnologia_inovacao_situacao cuja vigência seja anterior ao parâmetro de data de corte
# MAGIC   #prm_data_corte
# MAGIC   ***** Considerar apenas o SQ = 1 ******
# MAGIC   ======================================================================================================== */
# MAGIC (
# MAGIC  SELECT
# MAGIC      s.cd_atendimento_sti,
# MAGIC      aten.cd_atendimento_sti_dr, 
# MAGIC      aten.cd_entidade_regional,
# MAGIC      aten.cd_centro_responsabilidade,
# MAGIC      aten.cd_pessoa_atendida_sti
# MAGIC  FROM "STATUS" s
# MAGIC  INNER JOIN 
# MAGIC       atendimento_tecnologia_inovacao aten ON s.cd_atendimento_sti = aten.cd_atendimento_sti AND s.SQ = 1
# MAGIC )"ATEND"
# MAGIC  ---- ALTERADO DIA 08/10  ---- FIM
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Read and filter atendimento tecnologia inovacao

# COMMAND ----------

var_useful_columns_atendimento_tecnologia_inovacao = ["cd_atendimento_sti", "cd_atendimento_sti_dr", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_pessoa_atendida_sti"]

# COMMAND ----------

df_atend = spark.read.parquet(src_ati)\
.select(*var_useful_columns_atendimento_tecnologia_inovacao)\
.withColumnRenamed("cd_atendimento_sti", "cd_atendimento_sti_atend")


# COMMAND ----------

# MAGIC %md
# MAGIC Join atendimento_tecnologia_inovacao_situacao with atend from step 1

# COMMAND ----------

df_atend = df_atend\
.join(df_status.select("cd_atendimento_sti", "row_number"), 
      (df_atend.cd_atendimento_sti_atend == df_status.cd_atendimento_sti) & (df_status.row_number == 1),
      "inner")\
.drop("cd_atendimento_sti_atend", "row_number")\
.coalesce(1)\
.cache()

df_atend.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 3 - ALTERADO EM 19-05-2020
# MAGIC   Para os atendimentos encontrados no PASSO 1, obter as empresas atendidas que tenha cnpj ou doc estrangeiro
# MAGIC   ======================================================================================================== */
# MAGIC (
# MAGIC SELECT
# MAGIC      DISTINCT  ---- INCLUIDO 14/07
# MAGIC      a.cd_atendimento_sti,
# MAGIC      NVL(pj.cd_cnpj, NVL(pj.cd_documento_estrangeiro, 'N/I') AS cd_empresa_atendida_calc --- alterado 27/05
# MAGIC      FROM pessoa_juridica pj 
# MAGIC      INNER JOIN "ATEND" a (PASSO 2) ON a.cd_pessoa_atendida_sti = pj.cd_pessoa_juridica 
# MAGIC ) "EMPRESA_ATENDIDA"
# MAGIC 
# MAGIC ```

# COMMAND ----------

var_useful_columns_pessoa_juridica = ["cd_pessoa_juridica", "cd_cnpj", "cd_documento_estrangeiro"]

# COMMAND ----------

df_emp_atend = spark.read.parquet(src_pj)\
.select(*var_useful_columns_pessoa_juridica)\
.withColumn("cd_documento_empresa_atendida_calc", coalesce("cd_cnpj", "cd_documento_estrangeiro", lit("N/I")))\
.drop("cd_cnpj", "cd_documento_estrangeiro")\
.withColumnRenamed("cd_pessoa_juridica", "cd_pessoa_atendida_sti")

# COMMAND ----------

#df_emp_atend.count() #491045

# COMMAND ----------

# MAGIC %md
# MAGIC Join atend with atend from step 1

# COMMAND ----------

df_emp_atend = df_emp_atend.join(df_atend.select("cd_atendimento_sti", "cd_pessoa_atendida_sti"),
                                 ["cd_pessoa_atendida_sti"], "inner")\
.drop("cd_pessoa_atendida_sti")\
.dropDuplicates()

# COMMAND ----------

#df_emp_atend.count() #82925

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4

# COMMAND ----------

# MAGIC %md
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 4 - ALTERADO EM 19-05-2020 (trata somente carga horária)
# MAGIC   Usar os atendimentos do PASSO 1, para obter as métricas, somando as ocorrências de cada atendimento/mês
# MAGIC   cujas atualizações sejam anteriores ao parâmetro de data de corte #prm_data_corte
# MAGIC   ======================================================================================================== */
# MAGIC   (
# MAGIC    SELECT 
# MAGIC    c.cd_atendimento_sti,
# MAGIC    SUM(c.qt_hora_realizada) AS qt_hora_realizada,
# MAGIC    SUM(c.qt_ensaio_realizado) AS qt_ensaio_realizado,
# MAGIC    SUM(c.qt_calibracao_realizada) AS qt_calibracao_realizada,
# MAGIC    SUM(c.qt_material_realizado) AS qt_material_realizado,
# MAGIC    SUM(c.qt_relatorio_realizado) AS qt_relatorio_realizado,
# MAGIC    SUM(c.qt_certificado_realizado) AS qt_certificado_realizado,
# MAGIC    SUM(c.vl_producao) AS vl_producao
# MAGIC ​
# MAGIC    FROM atendimento_tecnologia_inovacao_producao c
# MAGIC    --INNER JOIN "STATUS" a (PASSO 2)   ----- excluído
# MAGIC    INNER JOIN "ATEND" a (PASSO 1) ----- incluído
# MAGIC    ON c.cd_atendimento_sti = a.cd_atendimento_sti
# MAGIC  
# MAGIC    -- FILTROS REGISTROS CARGA HORARIA
# MAGIC    WHERE c.dt_producao <= #prm_ano_fechamento + #prm_mes_fechamento + ultimodiames  --- alterado 27/05
# MAGIC    AND   c.dt_producao >= #prm_ano_fechamento + 01 + 01 -- 01 de Janeiro do ano de fechamento  --- alterado 27/05
# MAGIC    AND   DATE(c.dh_referencia) <= #prm_data_corte
# MAGIC    
# MAGIC    GROUP BY c.cd_atendimento_sti
# MAGIC  ) "CARGA_HORARIA"
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC Defining function that transforms year and month in one concatenated format

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

# MAGIC %md
# MAGIC Load relevant columns from atendimento tecnologia informacao producao and filter by dt_producao and dh_ultima_atualizacao_oltp. Filtering at this point will optimize reading

# COMMAND ----------

var_useful_columns_atendimento_tecnologia_informacao_producao = ["dt_producao", "cd_atendimento_sti", "qt_hora_realizada", "qt_ensaio_realizado", "qt_calibracao_realizada", "qt_material_realizado", "qt_relatorio_realizado", "qt_certificado_realizado", "vl_producao", "dh_referencia"]

# COMMAND ----------

df_carg_hor = spark.read.parquet(src_atip)\
.select(*var_useful_columns_atendimento_tecnologia_informacao_producao)\
.filter((col("dt_producao") < upper_lower_bound_date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"])["upperbound"]) &\
        (col("dt_producao") >= upper_lower_bound_date(var_parameters["prm_ano_fechamento"], var_parameters["prm_mes_fechamento"])["lowerbound"]) &\
        (col("dh_referencia").cast("date") <= var_parameters["prm_data_corte"]))\
.drop("dh_referencia", "dt_producao")\
.groupBy("cd_atendimento_sti")\
.agg(sum("qt_hora_realizada").cast("int").alias("qt_hora_realizada"),
     sum("qt_ensaio_realizado").cast("int").alias("qt_ensaio_realizado"),
     sum("qt_calibracao_realizada").cast("int").alias("qt_calibracao_realizada"),
     sum("qt_material_realizado").cast("int").alias("qt_material_realizado"),
     sum("qt_relatorio_realizado").cast("int").alias("qt_relatorio_realizado"),
     sum("qt_certificado_realizado").cast("int").alias("qt_certificado_realizado"),
     sum("vl_producao").cast("decimal(18,4)").alias("vl_producao")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Join atendimento_tecnologia_inovacao_producao with atend from step 1

# COMMAND ----------

df_carg_hor = df_carg_hor.join(df_atend.select("cd_atendimento_sti"),
                                 ["cd_atendimento_sti"], "inner")

# COMMAND ----------

#df_carg_hor.count() #54284

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5

# COMMAND ----------

# MAGIC %md
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ========================================================================================================
# MAGIC   PASSO 5 Unificar as informações - ALTERADO EM 19-05-2020 (criado a partir da quebra do PASSO 4 da versão 2.1
# MAGIC   ======================================================================================================== */
# MAGIC    /* ========================================================================================================
# MAGIC    PASSO 5 Unificar as informações - ALTERADO EM 19-05-2020 (criado a partir da quebra do PASSO 4 da versão 2.1
# MAGIC   ======================================================================================================== */
# MAGIC    SELECT 
# MAGIC    #prm_ano_fechamento AS cd_ano_fechamento,     
# MAGIC    #prm_mes_fechamento AS cd_mes_fechamento,    
# MAGIC    #prm_data_corte AS dt_fechamento, 
# MAGIC    a.cd_entidade_regional,
# MAGIC    a.cd_centro_responsabilidade, 
# MAGIC    NVL(e.cd_empresa_atendida_calc, 'N/A') as cd_empresa_atendida_calc,
# MAGIC    a.cd_atendimento_sti,   ---- incluído 12/08/2020
# MAGIC    a.cd_atendimento_sti_dr, -- incluido em 26/08/2020
# MAGIC    SUM(NVL(c.qt_hora_realizada,0)) AS qt_hora_realizada,
# MAGIC    SUM(NVL(c.qt_ensaio_realizado,0)) AS qt_ensaio_realizado,
# MAGIC    SUM(NVL(c.qt_calibracao_realizada,0)) AS qt_calibracao_realizada,
# MAGIC    SUM(NVL(c.qt_material_realizado,0)) AS qt_material_realizado,
# MAGIC    SUM(NVL(c.qt_relatorio_realizado,0)) AS qt_relatorio_realizado,
# MAGIC    SUM(NVL(c.qt_certificado_realizado,0)) AS qt_certificado_realizado,
# MAGIC    SUM(NVL(c.vl_producao,0)) AS vl_producao
# MAGIC 
# MAGIC    FROM ATEND a
# MAGIC    
# MAGIC    --INNER JOIN STATUS e
# MAGIC    --ON a.cd_atendimento_sti = e.cd_atendimento_sti
# MAGIC    
# MAGIC    LEFT JOIN EMPRESA_ATENDIDA e
# MAGIC    ON a.cd_atendimento_sti = e.cd_atendimento_sti
# MAGIC    
# MAGIC    LEFT JOIN CARGA_HORARIA c
# MAGIC    ON a.cd_atendimento_sti = c.cd_atendimento_sti
# MAGIC    
# MAGIC    GROUP BY a.cd_entidade_regional, a.cd_centro_responsabilidade, e.cd_empresa_atendida_calc, 
# MAGIC    a.cd_atendimento_sti, --- alterado 12/08
# MAGIC    a.cd_atendimento_sti_dr, -- incluido em 26/08/2020
# MAGIC  
# MAGIC  ```

# COMMAND ----------

df = df_atend.select("cd_entidade_regional", "cd_centro_responsabilidade", "cd_atendimento_sti", "cd_atendimento_sti_dr")\
.join(df_emp_atend, 
      ["cd_atendimento_sti"], 
      "left")\
.join(df_carg_hor, 
      ["cd_atendimento_sti"],
      "left")

# COMMAND ----------

#df.count() #83850

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
    repartition(1). \
    write. \
    save(path=sink, format="parquet", mode="overwrite")  
  
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC Create new columns

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"cd_ano_fechamento": lit(var_parameters["prm_ano_fechamento"]).cast("int"),
                  "cd_mes_fechamento": lit(var_parameters["prm_mes_fechamento"]).cast("int"), 
                  "dt_fechamento": lit(var_parameters["prm_data_corte"]).cast("date")}

#Apply transformations defined in column_map
for c in var_column_map:
  df = df.withColumn(c, lit(var_column_map[c]))

# COMMAND ----------

df = df.withColumn("cd_documento_empresa_atendida_calc", coalesce("cd_documento_empresa_atendida_calc", lit("N/A")))

# COMMAND ----------

#Create dictionary with column transformations required
var_type_map = {"cd_ano_fechamento": "int",
                "cd_mes_fechamento": "int",
                "dt_fechamento": "date",
                "cd_atendimento_sti": "int",
                "cd_entidade_regional": "int",
                "cd_centro_responsabilidade": "string"}

#Apply transformations defined in column_map
for c in var_type_map:
  df = df.withColumn(c, col("{}".format(c)).cast(var_type_map[c]))

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Now, group by keys specified in var_columns_to_groupby and sum the indicators
# MAGIC </pre>

# COMMAND ----------

var_columns_to_groupby = ["cd_ano_fechamento", "cd_mes_fechamento", "dt_fechamento", "cd_entidade_regional", "cd_centro_responsabilidade", "cd_documento_empresa_atendida_calc", "cd_atendimento_sti", "cd_atendimento_sti_dr"]

# COMMAND ----------

df = df.groupBy(var_columns_to_groupby)\
.agg(sum("qt_hora_realizada").cast("int").alias("qt_hora_realizada"),
     sum("qt_ensaio_realizado").cast("int").alias("qt_ensaio_realizado"),
     sum("qt_calibracao_realizada").cast("int").alias("qt_calibracao_realizada"),
     sum("qt_material_realizado").cast("int").alias("qt_material_realizado"),
     sum("qt_relatorio_realizado").cast("int").alias("qt_relatorio_realizado"),
     sum("qt_certificado_realizado").cast("int").alias("qt_certificado_realizado"),
     sum("vl_producao").cast("decimal(18,4)").alias("vl_producao")
    )

# COMMAND ----------

#display(df)

# COMMAND ----------

#df.count() #1611

# COMMAND ----------

# MAGIC %md
# MAGIC Finally create load timestamp as dh_insercao_biz

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
# MAGIC We can coalesce to 1 file to avoid keeping many small files
# MAGIC </pre>

# COMMAND ----------

df.coalesce(1).write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_atend.unpersist()


# COMMAND ----------

