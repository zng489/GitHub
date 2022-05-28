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
# MAGIC Processo	trs_biz_fte_producao_saude_seguranca_industria
# MAGIC Tabela/Arquivo Origem	"/trs/evt/lancamento_servico_saude_seguranca
# MAGIC /trs/evt/lancamento_saude_seguranca_metrica
# MAGIC /trs/evt/valor_lancamento_ssi_pessoa_beneficiada
# MAGIC /trs/evt/estabelecimento_atendido
# MAGIC /trs/evt/estabelecimento_atendido_caracteristica
# MAGIC /trs/mtd/corp/unidade_atendimento"
# MAGIC Tabela/Arquivo Destino	/biz/producao/fte_producao_saude_seguranca_industria
# MAGIC Particionamento Tabela/Arquivo Destino	cd_ano_fechamento / cd_mes_fechamento
# MAGIC Descrição Tabela/Arquivo Destino	Indicadores básicos de produção de pessoas atendidas pelos serviços de saúde e segurança do trabalhador.
# MAGIC Tipo Atualização	P = substituição parcial (delete/insert)
# MAGIC Detalhe Atualização	trunca a partição correspondente à  cd_ano_fechamento / cd_mes_fechamento correspondente à parâmetros informados para este processo ( #prm_ano_fechamento e #prm_mes_fechamento)
# MAGIC Periodicidade/Horario Execução	Diária, a partir do recebimento de parâmetros de fechamento  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte informados pelo usuário responsável da UNIGEST ou se não informado, setar como default  #prm_ano_fechamento,  #prm_mes_fechamento e #prm_data_corte pela data atual do processamento
# MAGIC 
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
var_tables = {"origins": ["/evt/lancamento_servico_saude_seguranca",
                         "/evt/lancamento_servico_saude_seguranca_metrica",
                         "/mtd/sesi/valor_lancamento_ssi_pessoa_beneficiada",
                         "/mtd/sesi/estabelecimento_atendido",
                         "/mtd/sesi/estabelecimento_atendido_caracteristica",
                         "/mtd/corp/entidade_regional",
                         "/mtd/corp/unidade_atendimento"],
              "destination": "/producao/fte_producao_saude_seguranca_industria",
              "databricks": {
                "notebook": "/biz/saude_seguranca_industria/trs_biz_fte_producao_saude_seguranca_industria"
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
       'adf_pipeline_name': 'trs_biz_fta_producao_saude_seguranca_trabalhador',
       'adf_pipeline_run_id': 'development',
       'adf_trigger_id': 'development',
       'adf_trigger_name': 'thomaz',
       'adf_trigger_time': '2020-07-31T13:15:00.0000000Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"closing": {"year": 2020, "month": 12, "dt_closing": "2021-01-27"}}
"""

# COMMAND ----------

print("var_tables: ", var_tables)
print("var_dls: ", var_dls)
print("var_adf: ", var_adf)
print("var_user_parameters: ", var_user_parameters)

# COMMAND ----------

src_seguranca, src_seguranca_metrica, src_pessoa_beneficiada, src_estabelecimento, src_estabelecimento_caracteristica, src_entidade_regional, src_unidade_atendimento = ["{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"],t)  for t in var_tables["origins"]]

print(src_seguranca, src_seguranca_metrica, src_pessoa_beneficiada, src_estabelecimento, src_estabelecimento_caracteristica, src_entidade_regional, src_unidade_atendimento)

# COMMAND ----------

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["business"], var_tables["destination"])
print(sink)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Section

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, from_utc_timestamp, current_timestamp, sum, year, month, max, row_number, first, desc, asc, count, coalesce, lower
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
# MAGIC Obter os parâmetros informados pela UNIGEST para fechamento do Orçamento Produção dos lançamentos de serviços de saúde e segurança do trabalhador do SESI do mês: 
# MAGIC #prm_ano_fechamento, #prm_mes_fechamento, #prm_data_corte ou obter #prm_ano_fechamento, #prm_mes_fechamento e
# MAGIC #prm_data_corte da data atual YEAR(SYSDATE), MONTH(SYSDATE) e DATE(SYSDATE)
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
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 1: Obter Lançamentos Desejados que estão ou estiveram ativos ao longo do ano:
# MAGIC - cuja data de inicio seja anterior ao último dia do mês de parâmetro #prm_ano_fechamento + #prm_mes_fechamenteo
# MAGIC - cuja data de fim seja posterior ao primeiro dia do ano de parâmetro #prm_ano_fechamento 
# MAGIC */ ====================================================================================================================
# MAGIC (   
# MAGIC     SELECT 
# MAGIC     	       l.id_lancamento_evento_oltp
# MAGIC 	      ,l.id_filtro_lancamento_oltp
# MAGIC               ,u.cd_entidade_regional      
# MAGIC               ,l.cd_centro_responsabilidade
# MAGIC               ,l.cd_clientela_oltp
# MAGIC               ,es.cd_porte_empresa      ---- incluido 30/06
# MAGIC               /*,CASE WHEN es.cd_cnpj IS NOT NULL
# MAGIC                     THEN 0
# MAGIC                     ELSE 1 END AS  fl_empresa_cadastro_especifico_inss   */ --- Excluído
# MAGIC               ,CASE WHEN es.cd_cnpj IS NULL
# MAGIC                     THEN es.cd_cei
# MAGIC                     ELSE es.cd_cnpj END AS cd_documento_empresa_atendida_calc    ---- alterado 03/08
# MAGIC 	      ,CASE WHEN  (es.cd_indicador_industria_fpas IN ('N', 'X') AND es.cd_indicador_industria_cnae = 'N') ---- incluído 29/07/2020
# MAGIC                    THEN 0
# MAGIC                    ELSE CASE WHEN es.fl_empresa_dn = 0 
# MAGIC                              THEN 1 
# MAGIC                              ELSE 0 
# MAGIC                              END 
# MAGIC                    END AS fl_industria
# MAGIC 			,CASE WHEN es.cd_cei IS NOT NULL       ---- Incluido no dia 06/05 --- exclusão de estabelecimentos CEI
# MAGIC 			      THEN 1
# MAGIC 				  ELSE 0
# MAGIC 				  END AS fl_empresa_cadastro_especifico_inss  --- fl_cei   --- Alterado 13/05/2021
# MAGIC      FROM lancamento_servico_saude_seguranca l
# MAGIC      INNER JOIN unidade_atendimento u on l.cd_unidade_atendimento_oba = u.cd_unidade_atendimento_dr   ----- incluido 08/09/2020
# MAGIC      LEFT JOIN 
# MAGIC  	(SELECT ea.cd_estabelecimento_atendido_oltp, ea.cd_cnpj, ea.cd_cei, ec.cd_porte_empresa, ea.fl_empresa_dn, ec.cd_indicador_industria_fpas, ec.cd_indicador_industria_cnae
# MAGIC 	    FROM estabelecimento_atendido ea 
# MAGIC 	    INNER JOIN estabelecimento_atendido_caracteristica ec ON ea.cd_estabelecimento_atendido_oltp = ec.cd_estabelecimento_atendido_oltp 
# MAGIC 	    WHERE ec.dt_inicio_vigencia <= #data_corte 
# MAGIC 	      AND (ec.dt_fim_vigencia >= #data_corte or ec.dt_fim_vigencia is null)
# MAGIC 	      AND (ea.cd_cnpj IS NOT NULL OR ea.cd_cei IS NOT NULL)   --- alterado 03/08
# MAGIC 		 
# MAGIC 	) es ON es.cd_estabelecimento_atendido_oltp = l.cd_estabelecimento_atendido_oltp
# MAGIC 
# MAGIC         WHERE 
# MAGIC           l.cd_ano_referencia = #prm_ano_fechamento
# MAGIC     AND   l.cd_mes_referencia <= #prm_mes_fechamento
# MAGIC     AND   l.cd_tipo_producao = 0   --- Normal - todos os indicadores são para Normal e não para Eventos
# MAGIC ) "LANCAMENTO"
# MAGIC 
# MAGIC ```

# COMMAND ----------

var_useful_columns_seguranca = ["id_lancamento_evento_oltp",
                                "id_filtro_lancamento_oltp",
                                "cd_unidade_atendimento_oba",
                                "cd_centro_responsabilidade",
                                "cd_clientela_oltp",
                                "cd_tipo_producao",
                                "cd_estabelecimento_atendido_oltp",
                                "dh_ultima_atualizacao_oltp",
                                "cd_ano_referencia",
                                "cd_mes_referencia"
                               ]

#In specification, this is: l
df_lancamento = spark.read.parquet(src_seguranca)\
.select(*var_useful_columns_seguranca)\
.filter((col("cd_ano_referencia") == var_parameters["prm_ano_fechamento"]) &
        (col("cd_mes_referencia") <= var_parameters["prm_mes_fechamento"]) &
        (col("cd_tipo_producao") == 0))\
.drop("cd_ano_referencia", "cd_mes_referencia", "cd_tipo_producao", "dh_ultima_atualizacao_oltp")

# COMMAND ----------

# When using 2020-05 and year, month
# df_lancamento is about 250k records and 2.9GB when cache in memory
#df_lancamento.count(), df_lancamento.dropDuplicates().count() #(228222, 228222)

# COMMAND ----------

var_useful_columns_unidade_atendimento = ["cd_entidade_regional","cd_unidade_atendimento_dr"]

#In specification, this is: u
df_unidade_atendimento = spark.read.parquet(src_unidade_atendimento)\
.select(*var_useful_columns_unidade_atendimento)\
.withColumnRenamed("cd_unidade_atendimento_dr", "cd_unidade_atendimento_oba")

# COMMAND ----------

#df_unidade_atendimento.count(), df_unidade_atendimento.dropDuplicates().count() #(5352, 5352)

# COMMAND ----------

var_useful_columns_estabelecimento = ["cd_cnpj", "cd_cei", "cd_estabelecimento_atendido_oltp", "fl_empresa_dn"]

#In specification, this is: ea
df_estabelecimento = spark.read.parquet(src_estabelecimento)\
.select(*var_useful_columns_estabelecimento)\
.filter((col("cd_cnpj").isNotNull()) | 
        (col("cd_cei").isNotNull()))\
.withColumn("cd_documento_empresa_atendida_calc", coalesce("cd_cnpj", "cd_cei"))\
.drop("cd_cnpj")

# COMMAND ----------

#df_estabelecimento.count(), df_estabelecimento.dropDuplicates().count() #(221616, 221616)

# COMMAND ----------

var_useful_columns_estabelecimento_caracteristica = ["cd_porte_empresa", 
                                                     "cd_estabelecimento_atendido_oltp", 
                                                     "cd_indicador_industria_fpas", 
                                                     "cd_indicador_industria_cnae", 
                                                     "dt_inicio_vigencia", 
                                                     "dt_fim_vigencia"
                                                    ]

#In specification, this is: ec
df_estabelecimento_caracteristica = spark.read.parquet(src_estabelecimento_caracteristica)\
.select(*var_useful_columns_estabelecimento_caracteristica)\
.filter((col("dt_inicio_vigencia") <= var_parameters["prm_data_corte"]) &
        ((col("dt_fim_vigencia") >= var_parameters["prm_data_corte"]) | (col("dt_fim_vigencia").isNull()))
       )\
.drop("dt_inicio_vigencia", "dt_fim_vigencia")

# COMMAND ----------

#df_estabelecimento_caracteristica.count(), df_estabelecimento_caracteristica.dropDuplicates().count() #(224101, 224101)

# COMMAND ----------

df_estabelecimento = df_estabelecimento.join(df_estabelecimento_caracteristica, ["cd_estabelecimento_atendido_oltp"], "inner").\
withColumn("fl_industria",\
           when((col("cd_indicador_industria_fpas").isin("N", "X")) & (col("cd_indicador_industria_cnae") == "N"), 0)\
           .otherwise(when(col("fl_empresa_dn") == 0, 1).otherwise(0))
          )\
.drop("cd_indicador_industria_fpas", "cd_indicador_industria_cnae", "fl_empresa_dn")

# COMMAND ----------

#df_estabelecimento.count(), df_estabelecimento.dropDuplicates().count() #(221425, 221425)

# COMMAND ----------

df_lancamento = df_lancamento.join(df_unidade_atendimento, ["cd_unidade_atendimento_oba"], "inner")\
.join(df_estabelecimento, ["cd_estabelecimento_atendido_oltp"], "left")\
.withColumn("fl_empresa_cadastro_especifico_inss", when(col("cd_cei").isNull(), lit(0)).otherwise(lit(1)))\
.drop("cd_unidade_atendimento_oba", "cd_cei")

# COMMAND ----------

#df_lancamento.count(), df_lancamento.dropDuplicates().count() #(224586, 224586)

# COMMAND ----------

df_lancamento = df_lancamento.coalesce(1).cache()
"""
This is just for activating df_lancamento. Count is LAZY!
This item is less than 10MB in memory cache, it surely can be kept in just one partition.
"""
df_lancamento.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 2: Todos os indicadores de metricas existentes acumulada do ano, cujas atualizações sejam anteriores ao parâmetro de
# MAGIC          data de corte #prm_data_corte 
# MAGIC */ ====================================================================================================================
# MAGIC 
# MAGIC select *   ---- inclusão 03/07
# MAGIC from
# MAGIC (
# MAGIC SELECT l.id_lancamento_evento_oltp
# MAGIC       ,l.id_filtro_lancamento_oltp
# MAGIC       ,m.id_valor_lancamento_oltp
# MAGIC       ,m.cd_clientela_oltp  --- Incluído 04/12
# MAGIC       ,NVL(m.cd_metrica, 'N/I') as cd_metrica
# MAGIC       ,sum(m.vl_metrica) as vl_metrica
# MAGIC FROM lancamento_servico_saude_seguranca_metrica m
# MAGIC INNER JOIN lancamento l on m.id_filtro_lancamento_oltp = l.id_filtro_lancamento_oltp
# MAGIC WHERE 
# MAGIC     m.cd_ano_referencia = #prm_ano_fechamento
# MAGIC AND m.cd_mes_referencia <= #prm_mes_fechamento
# MAGIC AND cast(m.dh_referencia as date) <= #prm_data_corte  --- alteração 03/07
# MAGIC GROUP BY l.id_lancamento_evento_oltp
# MAGIC       ,l.id_filtro_lancamento_oltp
# MAGIC       ,m.id_valor_lancamento_oltp
# MAGIC       ,m.cd_clientela_oltp  --- Incluído 04/12
# MAGIC       ,m.cd_metrica
# MAGIC  ) "VALOR_TOTAL"
# MAGIC WHERE vl_metrica > 0
# MAGIC ) "VALOR"
# MAGIC 
# MAGIC ```

# COMMAND ----------

var_useful_columns_seguranca_metrica = ["id_valor_lancamento_oltp", 
                                        "cd_clientela_oltp",
                                        "cd_metrica", 
                                        "vl_metrica", 
                                        "id_filtro_lancamento_oltp", 
                                        "cd_ano_referencia", 
                                        "cd_mes_referencia", 
                                        "dh_referencia"
                                       ]
"""
In specification, this is: m
The object is ~20 partitions
"""
df_valor = spark.read.parquet(src_seguranca_metrica)\
.select(*var_useful_columns_seguranca_metrica)\
.filter((col("dh_referencia").cast("date") <= var_parameters["prm_data_corte"]) &
        (col("cd_ano_referencia") == var_parameters["prm_ano_fechamento"]) &
        (col("cd_mes_referencia") <= var_parameters["prm_mes_fechamento"]))\
.withColumn("cd_metrica", coalesce(col("cd_metrica"), lit("N/I")))\
.drop("cd_ano_referencia", "cd_mes_referencia", "dh_referencia")

# COMMAND ----------

#df_valor.count(), df_valor.dropDuplicates().count() #(331192, 331192)

# COMMAND ----------

"""
The result is kept in just one partition.
Processing here is fast due to cache of df_lancamento.Most stages are skipped.
"""

df_valor = df_valor\
.join(df_lancamento.select("id_lancamento_evento_oltp", "id_filtro_lancamento_oltp"), ["id_filtro_lancamento_oltp"], "inner")\
.groupBy("id_lancamento_evento_oltp", "id_filtro_lancamento_oltp", "id_valor_lancamento_oltp", "cd_clientela_oltp", "cd_metrica")\
.agg(sum("vl_metrica").alias("vl_metrica"))\
.filter(col("vl_metrica")>0)

# COMMAND ----------

"""
The 1 partition object will be kept in memory. Sike is about 8MB. this will speed things up.
Row cont < 500K.
Count is to activate the cache().
"""
df_valor = df_valor.cache()
df_valor.count()

# COMMAND ----------

#df_valor.count(), df_valor.dropDuplicates().count() #(321763, 321763)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 3: quantidade de trabalhadores acumulada do ano, cujas atualizações sejam anteriores ao parâmetro de
# MAGIC          data de corte #prm_data_corte 
# MAGIC */ ====================================================================================================================
# MAGIC (
# MAGIC SELECT distinct
# MAGIC        vl.id_lancamento_evento_oltp
# MAGIC       ,vl.id_filtro_lancamento_oltp
# MAGIC       ,cd_cpf_pessoa as cd_cpf_pessoa_atendida
# MAGIC       ,l.cd_clientela_oltp   ---- incluído 04/12
# MAGIC FROM valor_lancamento_ssi_pessoa_beneficiada v
# MAGIC INNER JOIN valor vl ON vl.id_valor_lancamento_oltp = v.id_valor_lancamento_oltp 
# MAGIC INNER JOIN lancamento l ON l.id_filtro_lancamento_oltp = vl.id_filtro_lancamento_oltp 
# MAGIC WHERE 
# MAGIC       v.cd_cpf_pessoa IS NOT NULL
# MAGIC   --AND l.cd_clientela_oltp = 50 
# MAGIC   AND ( (cast (v.dh_inclusao_valor_pessoa as date) <= #prm_data_corte ---- incluído 04/08
# MAGIC   AND    v.fl_excluido_oltp = 0)   ---- alterado 31/07
# MAGIC    OR   (v.fl_excluido_oltp = 1 AND cast( v.dh_ultima_atualizacao_oltp as date) > #prm_data_corte) )  --- alteração 31/07
# MAGIC 
# MAGIC )  "TRABALHADOR"
# MAGIC 
# MAGIC ```

# COMMAND ----------

var_useful_columns_pessoa_beneficiada = ["cd_cpf_pessoa",
                                         "id_valor_lancamento_oltp",
                                         "dh_inclusao_valor_pessoa",
                                         "dh_ultima_atualizacao_oltp",
                                         "fl_excluido_oltp"
                                        ]
# In specification, this is: v
df_trabalhador = spark.read.parquet(src_pessoa_beneficiada)\
.select(*var_useful_columns_pessoa_beneficiada)\
.filter((col("cd_cpf_pessoa").isNotNull()) &
        (
         ((col("fl_excluido_oltp") == 0)
          & (col("dh_inclusao_valor_pessoa").cast("date") <= var_parameters["prm_data_corte"])
         ) | 
         ((col("fl_excluido_oltp") == 1) 
          & (col("dh_ultima_atualizacao_oltp").cast("date") > var_parameters["prm_data_corte"])
         )
        )
       )\
.drop("dh_ultima_atualizacao_oltp", "fl_excluido_oltp", "dh_inclusao_valor_pessoa")\
.dropDuplicates()\
.coalesce(16)

# COMMAND ----------

#df_trabalhador.count(), df_trabalhador.dropDuplicates().count() #(3837923, 3837923)

# COMMAND ----------

df_trabalhador = df_trabalhador\
.join(
  df_valor.select("id_valor_lancamento_oltp", "id_lancamento_evento_oltp", "id_filtro_lancamento_oltp"), 
  ["id_valor_lancamento_oltp"], 
  "inner"
)\
.join(
  df_lancamento.select("id_filtro_lancamento_oltp", "cd_clientela_oltp"), 
  ["id_filtro_lancamento_oltp"], 
  "inner"
)\
.withColumnRenamed("cd_cpf_pessoa","cd_cpf_pessoa_atendida")\
.drop("id_valor_lancamento_oltp")\
.dropDuplicates()\
.coalesce(2)

# COMMAND ----------

#df_trabalhador.count()

# COMMAND ----------

"""
The object is ~80MB in memory cache. 2 partitions will do fine.
We can keep it in cache to speed things up later.
count() is just to activate the cache().

"""

df_trabalhador = df_trabalhador.cache()
df_trabalhador.count()

# COMMAND ----------

#df_trabalhador.count(), df_trabalhador.dropDuplicates().count() #(3404506, 3404506)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4

# COMMAND ----------

# MAGIC %md
# MAGIC <b>FROM DOCUMENTATION</b>
# MAGIC ```
# MAGIC /* ====================================================================================================================
# MAGIC PASSO 4: Unificar as informações
# MAGIC */ ====================================================================================================================
# MAGIC (
# MAGIC SELECT 
# MAGIC        #prm_ano_fechamento AS cd_ano_fechamento
# MAGIC       ,#prm_mes_fechamento AS cd_mes_fechamento    
# MAGIC       ,#prm_data_corte AS dt_fechamento
# MAGIC       ,l.cd_entidade_regional
# MAGIC       ,l.cd_centro_responsabilidade
# MAGIC       ,l.id_filtro_lancamento_oltp    ---- incluido 02/09
# MAGIC       ,l.cd_documento_empresa_atendida_calc    ---- alterado 03/08
# MAGIC       ,l.fl_empresa_cadastro_especifico_inss   --- incluido 04/08
# MAGIC       ,l.cd_porte_empresa
# MAGIC       ,l.fl_industria
# MAGIC 	  -- ,l.fl_cei      ----  Excluído 13/05/2021
# MAGIC 	  ,l.fl_empresa_cadastro_especifico_inss  
# MAGIC       ,v.cd_clientela_oltp  Incluido 04/12
# MAGIC       ,-98 as cd_cpf_pessoa_atendida
# MAGIC       ,v.cd_metrica
# MAGIC       ,sum(v.vl_metrica) as vl_metrica
# MAGIC FROM  lancamento l
# MAGIC INNER JOIN valor v ON l.id_lancamento_evento_oltp = v.id_lancamento_evento_oltp and l.id_filtro_lancamento_oltp = v.id_filtro_lancamento_oltp  --- alterado 29/07
# MAGIC GROUP BY 
# MAGIC        l.cd_entidade_regional
# MAGIC       ,l.cd_centro_responsabilidade
# MAGIC       ,l.id_filtro_lancamento_oltp   ---- incluido 02/09
# MAGIC       ,cd_documento_empresa_atendida_calc    --- alterado 03/08
# MAGIC       ,l.fl_empresa_cadastro_especifico_inss    --- incluido 04/08
# MAGIC       ,l.cd_porte_empresa
# MAGIC       ,l.fl_industria
# MAGIC 	  -- ,l.fl_cei      ----  Excluído 13/05/2021
# MAGIC 	  ,v.cd_clientela_oltp  --- Incluido 04/12
# MAGIC       ,v.cd_metrica
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC SELECT 
# MAGIC        #prm_ano_fechamento AS cd_ano_fechamento
# MAGIC       ,#prm_mes_fechamento AS cd_mes_fechamento    
# MAGIC       ,#prm_data_corte AS dt_fechamento
# MAGIC       ,l.cd_entidade_regional
# MAGIC       ,l.cd_centro_responsabilidade
# MAGIC       ,l.id_filtro_lancamento_oltp     ---- incluido 02/09
# MAGIC       ,l.cd_documento_empresa_atendida_calc    ---- alterado 03/08
# MAGIC       , 0 as fl_empresa_cadastro_especifico_inss --- incluido 04/08  
# MAGIC       ,l.cd_porte_empresa
# MAGIC       ,l.fl_industria
# MAGIC 	  -- ,l.fl_cei      ----  Excluído 13/05/2021
# MAGIC 	  ,l.fl_empresa_cadastro_especifico_inss    --- incluido 04/08
# MAGIC       ,t.cd_clientela_oltp  ---- Incluido 04/12
# MAGIC       ,t.cd_cpf_pessoa_atendida
# MAGIC       ,'N/A' as cd_metrica
# MAGIC       ,0 as vl_metrica
# MAGIC FROM  lancamento l
# MAGIC LEFT JOIN trabalhador t ON l.id_lancamento_evento_oltp = t.id_lancamento_evento_oltp and l.id_filtro_lancamento_oltp = t.id_filtro_lancamento_oltp  --- alterado 29/07
# MAGIC )
# MAGIC  ```

# COMMAND ----------

df_lncmt_valor = df_lancamento\
.drop("cd_clientela_oltp")\
.join(df_valor, ["id_lancamento_evento_oltp", "id_filtro_lancamento_oltp"], "inner")\
.drop("id_lancamento_evento_oltp", "id_valor_lancamento_oltp", "cd_estabelecimento_atendido_oltp")\
.groupBy("cd_entidade_regional", "cd_centro_responsabilidade", "id_filtro_lancamento_oltp", "cd_documento_empresa_atendida_calc", 
         "fl_empresa_cadastro_especifico_inss", "cd_porte_empresa", "fl_industria", "cd_clientela_oltp", "cd_metrica")\
.agg(sum("vl_metrica").cast("long").alias("vl_metrica"))\
.withColumn("cd_cpf_pessoa_atendida", lit(-99).cast("long"))

# COMMAND ----------

#df_lncmt_valor.count(), df_lncmt_valor.dropDuplicates().count() #(135004, 135004)

# COMMAND ----------

"""
This object is ~60 MB in memory, 2.5M records. 2 partitions will do fine.
"""


df_lcmnt_trabl = df_lancamento\
.drop("cd_clientela_oltp")\
.join(df_trabalhador, ["id_lancamento_evento_oltp", "id_filtro_lancamento_oltp"], "inner")\
.drop("id_lancamento_evento_oltp", "id_valor_lancamento_oltp", "cd_estabelecimento_atendido_oltp")\
.dropDuplicates()\
.withColumn("cd_metrica", lit("N/A"))\
.withColumn("vl_metrica", lit(0).cast("long"))\
.coalesce(2)

# COMMAND ----------

#df_lcmnt_trabl.count(), df_lcmnt_trabl.dropDuplicates().count() #(2587102, 2587102)

# COMMAND ----------

"""
This final object is ~80MB in cached memory,  ~3M records
Can be held by 1 partition. 
All older caches can be dropped.
"""

df = df_lncmt_valor\
.union(df_lcmnt_trabl.select(df_lncmt_valor.columns))\
.coalesce(1)

"""
count() is just to activate the cache()
We'll use this one later =D
"""
df = df.cache()
var_df_count = df.count()

# COMMAND ----------

"""
Time to drop all older caches
"""
df_trabalhador = df_trabalhador.unpersist()
df_valor = df_valor.unpersist()
df_lancamento = df_lancamento.unpersist()

# COMMAND ----------

#df.count(), df.dropDuplicates().count() #(2722106, 2722106)

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

#Create dictionary with column transformations required
var_type_map = {"cd_ano_fechamento": "int",
                "cd_mes_fechamento": "int",
                "dt_fechamento": "date",
                "cd_entidade_regional": "int",
                "cd_centro_responsabilidade": "string",   
                "id_filtro_lancamento_oltp": "long",
                "cd_documento_empresa_atendida_calc": "string",
                "cd_porte_empresa": "int",
                "fl_industria": "int",                
                "fl_empresa_cadastro_especifico_inss": "int",
                "cd_clientela_oltp": "int",
                "cd_cpf_pessoa_atendida": "long",
                "cd_metrica": "string",
                "vl_metrica": "long"}

#Apply transformations defined in column_map
for c in var_type_map:
  df = df.withColumn(c, col("{}".format(c)).cast(var_type_map[c]))

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
# MAGIC *df is already coalesced in 1 partition!*
# MAGIC 
# MAGIC <pre>
# MAGIC If there's no data and it is the first load, we must save the data frame without partitioning, because saving an empty partitioned dataframe does not save the metadata.
# MAGIC When there is no new data in the business and you already have other data from other loads, nothing happens.
# MAGIC And when we have new data, it is normally saved with partitioning.
# MAGIC </pre>

# COMMAND ----------

"""
To do this trick, we use the already created var_df_count, as all other following operations are row/narrow operations.
"""
if var_df_count == 0:
  try:
    spark.read.parquet(sink)
  except AnalysisException:
    df.write.save(path=sink, format="parquet", mode="overwrite")
  dbutils.notebook.exit('{"data_count_is_zero": 1}')

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Dynamic overwrite will guarantee that only this view will be updated by cd_ano_fechamento and cd_mes_fechamento
# MAGIC We can coalesce to 1 file to avoid keeping many small files
# MAGIC </pre>

# COMMAND ----------

df.write.partitionBy("cd_ano_fechamento", "cd_mes_fechamento").save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df = df.unpersist()