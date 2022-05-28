# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type append/incremental insert
# MAGIC 
# MAGIC <pre>
# MAGIC Processo	raw_trs_orcamento_nacional_realizado
# MAGIC Tabela/Arquivo Origem	/raw/bdo/protheus11/akd010
# MAGIC Tabela/Arquivo Destino	/trs/evt/orcamento_nacional_realizado
# MAGIC Particionamento Tabela/Arquivo Destino	YEAR(dt_lancamento) / MONTH(dt_lancamento) / dt_ultima_atualizacao_oltp
# MAGIC Descrição Tabela/Arquivo Destino	Lançamentos financeiros de receitas e despesas realizados, informados pelas regionais e departamento nacional SESI e SENAI, consolidadoras. Podem ser eventos do tipo "E", entrada quando um novo lançamento chega, ou do tipo "S", saída, quando um lançamento é deletado, então um registro idêntico ao da entrada correspondente é lançado com sinal invertido para a data de referncia de sua exclusão, mantendo assim , um saldo ao somar valores e quantidades. 
# MAGIC Tipo Atualização	A = append (insert)
# MAGIC Detalhe Atuaização	carga incremental, dando a oportunidade de recuperar registros ignoraddos na carga anterior
# MAGIC Periodicidade/Horario Execução	Diária, após carga raw akd010
# MAGIC 
# MAGIC 
# MAGIC Dev: Marcela
# MAGIC Manutenção:
# MAGIC   2020/06/10 - Thomaz Moreira: adicionada claúsula de avaliação var_ano = 0 -> ano corrente
# MAGIC   2020/11/05 - Patricia Costa: adicionada cláusula de filtro LEN(TRIM(SUBSTRING(AKD_ITCTB,3,25)) >= 9 para não permitir lançamentos que não sejam no nível 5 do CR
# MAGIC   2020/12/08 - Marcela Crozara: removida cláusula de filtro LEN(TRIM(SUBSTRING(AKD_ITCTB,3,25)) >= 9
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
var_tables =  {"origins": ["/bdo/protheus11/akd010"], "destination": "/evt/orcamento_nacional_realizado"}

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
       'adf_trigger_time': '2020-10-21T15:00:06.0829994Z',
       'adf_trigger_type': 'Manual'
      }

var_user_parameters = {"var_ano": 2020}
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

sink = "{}{}{}".format(var_adls_uri, var_dls["folders"]["trusted"], var_tables["destination"])
print(sink)

# COMMAND ----------

var_ano = var_user_parameters["var_ano"]

"""
Evaluation for var_ano to consider the actual year when its value is 0
"""
var_ano = datetime.datetime.now().year if var_ano == 0 else var_ano
print(var_ano)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation section

# COMMAND ----------

from pyspark.sql.functions import trim, col, current_timestamp, from_utc_timestamp, substring, max, lit, when, to_date, year, current_date, lead, udf, array, dense_rank, desc, date_format, row_number, month, length, min, count
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql.window import Window
from trs_control_field import trs_control_field as tcf
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Obter a maior data de referencia carregada #var_max_dt_ultima_atualizacao_oltp = SELECT MAX(dt_ultima_atualizacao_oltp) from orcamento_nacional_realizado

# COMMAND ----------

var_max_dt_ultima_atualizacao_oltp=None
var_primeira_carga=False
try:
  df_sink = spark.read.parquet(sink)
  var_max_dt_ultima_atualizacao_oltp = df_sink.select(max(date_format(col("dt_ultima_atualizacao_oltp"), "yyyyMMdd").cast(IntegerType()))).collect()[0][0]  
except:
  var_max_dt_ultima_atualizacao_oltp=0
  var_primeira_carga=True

# COMMAND ----------

print(var_primeira_carga, var_max_dt_ultima_atualizacao_oltp)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Entradas

# COMMAND ----------

# MAGIC %md
# MAGIC <b>From documentation:</b>
# MAGIC ```
# MAGIC --=================================================================
# MAGIC -- REGISTRA LANÇAMENTOS NOVOS (ARQUIVOS NOVOS)
# MAGIC --=================================================================
# MAGIC /* 
# MAGIC Obter o parâmetro de execução da carga para o ano = #var_ano
# MAGIC Obter a maior data de referencia carregada #var_max_dt_referencia = SELECT MAX(dt_referencia) from orcamento_nacional_realizado
# MAGIC Ler AKD010 com as regras para carga incremental na tabela orcamento_nacional_realizado, execucao diária, query abaixo:
# MAGIC */ 
# MAGIC 
# MAGIC SELECT
# MAGIC 'E' as cd_movimento,
# MAGIC TRIM(AKD_OPER) as cd_entidade_regional_erp_oltp,
# MAGIC SUBSTRING(AKD_OPER,2,1) as cd_entidade_nacional,
# MAGIC TRIM(AKD_LOTE) as cd_lote_lancamento,
# MAGIC AKD_ID as cd_item_lote_lancamento,
# MAGIC AKD_DATA as dt_lancamento,
# MAGIC TRIM(AKD_HIST) as ds_lancamento,
# MAGIC TRIM(AKD_CC) as cd_unidade_organizacional,
# MAGIC TRIM(SUBSTRING(AKD_ITCTB,3,25)) as cd_centro_responsabilidade,
# MAGIC TRIM(AKD_CO) as cd_conta_contabil,
# MAGIC 1 as qt_lancamento,
# MAGIC AKD_VALOR1 * (CASE WHEN AKD_TIPO <> '1' THEN -1 ELSE 1 END) as vl_lancamento,
# MAGIC R_E_C_N_O_ as id_registro_oltp,
# MAGIC AKD_XDTIMP as dt_ultima_atualizacao_oltp,
# MAGIC TRIM(AKD_XFILE) as nm_arq_referencia
# MAGIC FROM PROTHEUS11_AKD010
# MAGIC WHERE TRIM(AKD_TPSALD) = 'RN'			                 -- Realizado, traz somente receitas e despesas (AKD_CO iniciando com 3-despesas ou 4-receitas)
# MAGIC AND   (AKD_CO like '4%' OR AKD_CO like '3%')			 -- somente Receita e Despesa, para reforçar regra
# MAGIC AND   CAST(SUBSTRING(AKD_OPER,2,1) AS INT) in (2,3)		 -- 02-SESI e 03-SENAI
# MAGIC AND   TRIM(AKD_OPER) like '%0000'				         -- Todas as entidades regionais e as entidades nacionais SESI e SENAI que representam consolidadoras)
# MAGIC AND   AKD_STATUS = 1						             -- somente registros válidos
# MAGIC AND   SUBSTRING(AKD_FILIAL,1,2) = SUBSTRING(AKD_OPER,1,2)-- Data quality, contorno para ignorar registros inconsistentes na tabela
# MAGIC --incluída em 05/11/2020 e excluida em 08/12/2020
# MAGIC --AND LEN(TRIM(SUBSTRING(AKD_ITCTB,3,25)) >= 9            -- Data quality, não permitir lançamentos que não sejam no nível 5 do CR
# MAGIC AND   AKD_XDTIMP >= #var_max_dt_referencia			     -- carga incremental, dando a oportunidade de recuperar registros ignoraddos na carga anterior
# MAGIC AND   YEAR(AKD_DATA) = #var_ano					         -- ano selecionado para processamento
# MAGIC ```

# COMMAND ----------

useful_columns = ["AKD_OPER", "AKD_LOTE", "AKD_ID", "AKD_DATA", "AKD_HIST", "AKD_CC", "AKD_ITCTB", "AKD_CO", "AKD_VALOR1", "AKD_TIPO", "R_E_C_N_O_", "AKD_XDTIMP", "AKD_XFILE", "AKD_TPSALD", "AKD_STATUS", "AKD_FILIAL", "D_E_L_E_T_"]

# COMMAND ----------

df_akd010 = spark.read.parquet(src_akd).select(*useful_columns)\
.filter((trim(col("AKD_TPSALD")) == "RN") &\
        (col("AKD_CO").like("4%") | col("AKD_CO").like("3%")) &\
        (substring(col("AKD_OPER"), 2, 1).cast(IntegerType()).isin(2,3)) &\
        (trim(col("AKD_OPER")).like("%0000")) & (trim(col("AKD_STATUS")) == 1) &\
        (substring(col("AKD_FILIAL"), 2, 1) == substring(col("AKD_OPER"), 2, 1)) &\
        (to_date(col("AKD_DATA"), 'yyyyMMdd') >= lit(datetime.strptime("01/01/2017", "%d/%m/%Y")).cast("date"))
       )\
.drop("AKD_TPSALD", "AKD_STATUS", "AKD_FILIAL")\
.coalesce(24)

# COMMAND ----------

df_entrada = df_akd010.filter(trim(col("AKD_XDTIMP")) >= var_max_dt_ultima_atualizacao_oltp)\
.drop("D_E_L_E_T_")

# COMMAND ----------

if var_primeira_carga == True:
  df_entrada = df_entrada.withColumn("AKD_DATA", when((trim(col("AKD_DATA")).isNull()) | (trim(col("AKD_DATA")) == ''), "19000101").otherwise(col("AKD_DATA")))

# COMMAND ----------

var_column_name_map = {"AKD_LOTE": "cd_lote_lancamento",
                       "AKD_ID": "cd_item_lote_lancamento",
                       "AKD_DATA": "dt_lancamento",
                       "AKD_HIST": "ds_lancamento",
                       "AKD_CC": "cd_unidade_organizacional",
                       "AKD_ITCTB": "cd_centro_responsabilidade",
                       "AKD_CO": "cd_conta_contabil",
                       "R_E_C_N_O_": "id_registro_oltp",
                       "AKD_XDTIMP": "dt_ultima_atualizacao_oltp",
                       "AKD_XFILE": "nm_arq_referencia"}
  
for key in var_column_name_map:
  df_entrada = df_entrada.withColumnRenamed(key, var_column_name_map[key])

# COMMAND ----------

#Create dictionary with column transformations required
var_column_map = {"cd_movimento": lit("E"),
                  "cd_entidade_regional_erp_oltp": trim(col("AKD_OPER")),
                  "cd_entidade_nacional": substring(col("AKD_OPER"), 2, 1).cast(IntegerType()),
                  "cd_lote_lancamento": trim(col("cd_lote_lancamento")),
                  "cd_item_lote_lancamento": col("cd_item_lote_lancamento").cast(IntegerType()),
                  "dt_lancamento": to_date(col("dt_lancamento"), 'yyyyMMdd'),                  
                  "ds_lancamento": trim(col("ds_lancamento")),
                  "cd_unidade_organizacional": trim(col("cd_unidade_organizacional")),
                  "cd_centro_responsabilidade": trim(substring(col("cd_centro_responsabilidade"), 3, 25)),
                  "cd_conta_contabil": trim(col("cd_conta_contabil")),
                  "qt_lancamento": lit("1").cast(IntegerType()),
                  "vl_lancamento": when(col("AKD_TIPO") != "1", - col("AKD_VALOR1")).otherwise(col("AKD_VALOR1")),
                  "id_registro_oltp": col("id_registro_oltp").cast(LongType()),
                  "dt_ultima_atualizacao_oltp": to_date(col("dt_ultima_atualizacao_oltp"), 'yyyyMMdd'),
                  "nm_arq_referencia": trim(col("nm_arq_referencia"))}

#Apply transformations defined in column_map
for c in var_column_map:
  df_entrada = df_entrada.withColumn(c, var_column_map[c])


# COMMAND ----------

df_entrada = df_entrada.withColumn("cd_ano_lancamento", year(col("dt_lancamento")))\
.drop("AKD_OPER", "AKD_VALOR1", "AKD_TIPO")

# COMMAND ----------

#add control fields from trusted_control_field egg
df_entrada = tcf.add_control_fields(df_entrada, var_adf)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Serifica se já existe registro cd_lote_lancamento, cd_item_lote_lancamento, 'E'
# MAGIC Se existe, IGNORA
# MAGIC Senão, INSERE o registro
# MAGIC </pre>

# COMMAND ----------

concat_udf = udf(lambda cols: "".join([str(x).strip() if x is not None else "" for x in cols]), StringType())

# COMMAND ----------

if var_primeira_carga:
  df = df_entrada.filter(year(col("dt_lancamento")) <= var_ano)
  
else:
  id_column =  ["cd_lote_lancamento", "cd_item_lote_lancamento", "id_registro_oltp", "cd_movimento"]
  df_entrada = df_entrada.filter(year(col("dt_lancamento")) == var_ano)  
  df = df_sink\
  .union(df_entrada.select(df_sink.columns))\
  .withColumn("unique_id", concat_udf(array(*id_column)))\
  .withColumn("rank", row_number().over(Window.partitionBy("unique_id").orderBy("dh_insercao_trs")))\
  .filter(col("rank") == 1)\
  .drop("rank", "unique_id")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>From documentation:</b>
# MAGIC ```
# MAGIC --=================================================================
# MAGIC -- LISTA ARQUIVOS DO ANO
# MAGIC --=================================================================
# MAGIC /* 
# MAGIC listar todos os arquivos referentes ao ano de lançamento desejado, ordenados por aquivo e data de importação do arquivo,
# MAGIC query abaixo:
# MAGIC */
# MAGIC SELECT DISTINCT AKD_XFILE, AKD_XDTIMP, D_E_L_E_T_, AKD_LOTE
# MAGIC FROM PROTHEUS11_AKD010
# MAGIC WHERE TRIM(AKD_TPSALD) = 'RN'			                 -- Realizado, traz somente receitas e despesas (AKD_CO iniciando com 3-despesas ou 4-receitas)
# MAGIC AND   (AKD_CO like '4%' OR AKD_CO like '3%')			 -- somente Receita e Despesa, para reforçar regra
# MAGIC AND   CAST(SUBSTRING(AKD_OPER,2,1) AS INT) in (2,3)		 -- 02-SESI e 03-SENAI
# MAGIC AND   TRIM(AKD_OPER) like '%0000'				         -- Todas as entidades regionais e as entidades nacionais SESI e SENAI que representam consolidadoras)
# MAGIC AND   AKD_STATUS = 1						             -- somente registros válidos
# MAGIC AND   SUBSTRING(AKD_FILIAL,1,2) = SUBSTRING(AKD_OPER,1,2)-- Data quality, contorno para ignorar registros inconsistentes na tabela
# MAGIC --incluída em 05/11/2020
# MAGIC AND LEN(TRIM(SUBSTRING(AKD_ITCTB,3,25)) >= 9            -- Data quality, não permitir lançamentos que não sejam no nível 5 do CR
# MAGIC AND   YEAR(AKD_DATA) = #var_ano					         -- ano selecionado para processamento
# MAGIC ORDER BY AKD_XFILE, AKD_XDTIMP, AKD_LOTE			     -- IMPORTANTE: viabiliza encontrar a data de substituição do arquivo
# MAGIC 
# MAGIC ```

# COMMAND ----------

if var_primeira_carga:
  df_lista = df_akd010.filter(year(to_date(col("AKD_DATA"), 'yyyyMMdd')) <= var_ano)
  
else:
  df_lista = df_akd010.filter(year(to_date(col("AKD_DATA"), 'yyyyMMdd')) == var_ano)

# COMMAND ----------

df_lista = df_lista.select("AKD_XFILE", "AKD_XDTIMP", "D_E_L_E_T_", "AKD_LOTE")\
.withColumn("AKD_XFILE", trim(col("AKD_XFILE")))\
.withColumn("AKD_LOTE", trim(col("AKD_LOTE")))\
.withColumn("AKD_XDTIMP", trim(col("AKD_XDTIMP")))\
.distinct()\
.orderBy(["AKD_XFILE", "AKD_XDTIMP", "AKD_LOTE"])\
.coalesce(1)\
.cache()

df_lista.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>From documentation</b>
# MAGIC ```
# MAGIC --=================================================================
# MAGIC -- IDENTIFICA SAÍDAS (ARQUIVOS DELETADOS COM ARQUIVO NOVO EM SUBSTITUIÇÃO)
# MAGIC --=================================================================
# MAGIC /* 
# MAGIC LISTAR APENAS OD DELETADOS: Com a lista de ARQUIVOS lidos ordenados por AKD_XFILE, AKD_XDTIMP, obter daqueles com D_E_L_E_T_ = '*', 
# MAGIC seu lote, #var_cd_lote_lancamento_saida = AKD_LOTE
# MAGIC 
# MAGIC Encontrar o ARQUIVO DE MESMO NOME subsequente ao deletado (LEAD), isto é, o ARQUIVO que o substituiu e obter sua data que será considerada a data de saída
# MAGIC do registro deletado, #var_dt_referencia_saida = AKD_XDTIMP (do registro de substituição) 
# MAGIC 
# MAGIC AKD_XFILE					AKD_LOTE	AKD_XDTIMP	D_E_L_E_T_
# MAGIC 
# MAGIC REA03SP202001-ORC.TXT    	O203SC1OSQ	20200219	*
# MAGIC REA03SP202001-ORC.TXT    	O203SC21XW	20201014	NULL
# MAGIC 
# MAGIC REA03SP202002-ORC.TXT    	O203SC1QR7	20200318	NULL
# MAGIC REA03SP202003-ORC.TXT    	O203SC1SO4	20200504	*
# MAGIC REA03SP202003-ORC.TXT    	O203SC1SWZ	20200507	NULL
# MAGIC REA03SP202004-ORC.TXT    	O203SC1TSM	20200525	NULL
# MAGIC REA03SP202005-ORC.TXT    	O203SC1VG4	20200630	NULL
# MAGIC REA03SP202006-ORC.TXT    	O203SC1WHS	20200720	NULL
# MAGIC REA03SP202007-ORC.TXT    	O203SC1Y7U	20200820	NULL
# MAGIC REA03SP202008-ORC.TXT    	O203SC2198	20201009	NULL
# MAGIC REA03SP202009-ORC.TXT    	O203SC225K	20201016	NULL
# MAGIC 
# MAGIC Neste exemplo do aquivo "REA03SP202001-ORC.TXT" registro deletado (linha 1) obteríamos o seu lote = O203SC1OSQ
# MAGIC e a data do registro (mesmo arquivo) subsequente (linha2), lembrando que estão ordenados por AKD_XFILE e AKD_XDTIMP
# MAGIC 
# MAGIC Resultado "LISTA_DELETADOS":
# MAGIC AKD_XFILE					AKD_LOTE	AKD_XDTIMP	D_E_L_E_T_  AKD_XDTIMP_SAIDA
# MAGIC REA03SP202001-ORC.TXT    	O203SC1OSQ	20200219	*			20201014
# MAGIC REA03SP202003-ORC.TXT    	O203SC1SO4	20200504	*           20200507
# MAGIC 
# MAGIC OBS: arquivo com delete sem registro subsequente não será incuído, AKD_XDTIMP_SAIDA = NULL (aguardará a entrada deste arquivo substituto no futuro)
# MAGIC */
# MAGIC 
# MAGIC ```

# COMMAND ----------

leadCol = lead(col("AKD_XDTIMP"), 1).over(Window.partitionBy("AKD_XFILE").orderBy(col("AKD_XFILE").asc(), col("AKD_XDTIMP").asc(), col("D_E_L_E_T_").desc()))

# COMMAND ----------

df_lista_del = df_lista.withColumn("var_dt_ultima_atualizacao_oltp_saida", leadCol)\
.withColumnRenamed("AKD_LOTE", "var_cd_lote_lancamento_saida")\
.withColumnRenamed("AKD_XFILE", "var_nm_arquivo_saida")\
.withColumnRenamed("AKD_XDTIMP", "var_dt_ultima_atualizacao_oltp")\
.filter((col("D_E_L_E_T_") == "*") &\
        (col("var_dt_ultima_atualizacao_oltp_saida").isNotNull()))\
.drop("D_E_L_E_T_")\
.coalesce(1)\
.cache()

df_lista_del.count()

# COMMAND ----------

df_lista = df_lista.unpersist()

# COMMAND ----------

df_registros_del = df_akd010\
.select("AKD_XFILE", "AKD_LOTE", "AKD_XDTIMP", "R_E_C_N_O_", "D_E_L_E_T_")\
.join(df_lista_del, 
     (df_lista_del.var_nm_arquivo_saida == trim(df_akd010.AKD_XFILE)) &\
     (df_lista_del.var_cd_lote_lancamento_saida == trim(df_akd010.AKD_LOTE)) &\
     (df_lista_del.var_dt_ultima_atualizacao_oltp == trim(df_akd010.AKD_XDTIMP)),
     "inner")\
.withColumnRenamed("R_E_C_N_O_", "var_id_registro_oltp_saida")\
.withColumn("var_dt_ultima_atualizacao_oltp_saida", to_date(col("var_dt_ultima_atualizacao_oltp_saida"), 'yyyyMMdd'))\
.withColumn("var_dt_ultima_atualizacao_oltp", to_date(col("var_dt_ultima_atualizacao_oltp"), 'yyyyMMdd'))\
.filter(col("D_E_L_E_T_") == "*")\
.drop("AKD_XFILE", "AKD_LOTE", "AKD_XDTIMP", "D_E_L_E_T_")\
.coalesce(8)\
.cache()

df_registros_del.count()

# COMMAND ----------

df_lista_del = df_lista_del.unpersist()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Saidas

# COMMAND ----------

# MAGIC %md
# MAGIC <b>From documentation</b>
# MAGIC ```
# MAGIC --=================================================================
# MAGIC -- REGISTRA LANÇAMENTOS SAÍDA (ARQUIVOS EXISTENTE SUBSTITUI DOS POR NOVOS)
# MAGIC --=================================================================
# MAGIC --Para cada lote deletado na "LISTA_DELETADOS" gerar um registro de saída com a data do novo lote
# MAGIC 
# MAGIC SELECT
# MAGIC 'S' as cd_movimento,
# MAGIC o.cd_entidade_regional_erp_oltp,
# MAGIC o.cd_entidade_nacional,
# MAGIC o.cd_lote_lancamento,
# MAGIC o.cd_item_lote_lancamento,
# MAGIC o.dt_lancamento,
# MAGIC o.ds_lancamento,
# MAGIC o.cd_unidade_organizacional,
# MAGIC o.cd_centro_responsabilidade,
# MAGIC o.cd_conta_contabil,
# MAGIC -1 as qt_lancamento,
# MAGIC o.vl_lancamento *-1 as vl_lancamento,
# MAGIC id_registro_oltp,
# MAGIC d.AKD_XDTIMP_SAIDA as dt_ultima_atualizacao_oltp,
# MAGIC o.nm_arq_referencia
# MAGIC FROM orcamento_nacional_realizado o
# MAGIC INNER JOIN  "LISTA_DELETADOS" d
# MAGIC ON o.cd_lote_lancamento = d.AKD_LOTE
# MAGIC 
# MAGIC ```

# COMMAND ----------

df_e = df.filter(col("cd_movimento") == "E")\
.drop("dh_insercao_trs", "kv_process_control")

# COMMAND ----------

df_s = df.filter(col("cd_movimento") == "S")\
.select("id_registro_oltp")

# COMMAND ----------

df_saida = df_e\
.join(df_s, ["id_registro_oltp"], "leftanti")

# COMMAND ----------

#add control fields from trusted_control_field egg
df_saida = tcf.add_control_fields(df_saida, var_adf)

# COMMAND ----------

df_saida = df_saida\
.join(df_registros_del,
      (df_saida.nm_arq_referencia == df_registros_del.var_nm_arquivo_saida) &\
      (df_saida.cd_lote_lancamento == df_registros_del.var_cd_lote_lancamento_saida) &\
      (df_saida.dt_ultima_atualizacao_oltp == df_registros_del.var_dt_ultima_atualizacao_oltp) &\
      (df_saida.id_registro_oltp == df_registros_del.var_id_registro_oltp_saida))\
.withColumn("cd_movimento", lit("S"))\
.withColumn("qt_lancamento", lit("-1").cast(IntegerType()))\
.withColumn("vl_lancamento", -1 * col("vl_lancamento"))\
.withColumn("dt_ultima_atualizacao_oltp", to_date(col("var_dt_ultima_atualizacao_oltp_saida"), 'yyyyMMdd'))\
.drop("var_nm_arquivo_saida","var_cd_lote_lancamento_saida","var_dt_ultima_atualizacao_oltp_saida", "var_dt_ultima_atualizacao_oltp", "var_id_registro_oltp_saida")

# COMMAND ----------

df = df.union(df_saida.select(df.columns))

# COMMAND ----------

if var_primeira_carga == False:
  df = df.filter(col("cd_ano_lancamento") == var_ano)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Salvando no destino

# COMMAND ----------

df.coalesce(24).write.partitionBy(["cd_ano_lancamento"]).save(path=sink, format="parquet", mode="overwrite")

# COMMAND ----------

df_lista_del = df_lista_del.unpersist()

# COMMAND ----------

