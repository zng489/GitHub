# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	org_raw_microdados_turmas				
# MAGIC Tabela/Arquivo Origem	\lnd\crw\inep_censo_escolar__turmas			
# MAGIC Tabela/Arquivo Destino	\raw\crw\inep_censo_escolar\turmas				
# MAGIC Particionamento Tabela/Arquivo Destino	Ano do Censo (campo NU_ANO_CENSO)				
# MAGIC Descrição Tabela/Arquivo Destino	Dados do Censo Escolar turmas unificado a com base no layout de 2019				
# MAGIC Tipo Atualização	A = append (insert)				
# MAGIC Detalhe Atuaização	N/A				
# MAGIC Periodicidade/Horario Execução	Anual depois da disponibilização dos dados na landing zone				
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

try:
  table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
  dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
  adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
except:
  from datetime import datetime
  
  table = {
    'schema': 'inep_censo_escolar',
    'table': 'turmas',
    'partition_column_raw': 'NU_ANO_CENSO',
    'prm_path': '/prm/usr/inep_censo_escolar_turma/KC2332_Censo_Escolar_Turma_mapeamento_unificado_raw.xlsx'
  }

  adf = {
    "adf_factory_name": "cnibigdatafactory",
    "adf_pipeline_name": "org_raw_microdados_escola",
    "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
    "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
    "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
    "adf_trigger_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    "adf_trigger_type": "PipelineActivity"
  }

  dls = {"folders":{"landing":"/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

headers = {'name_header':'Campo Origem', 'pos_header':'C', 'pos_org':'C', 'pos_dst':'E', 'pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get years present in BA documentation 

# COMMAND ----------

years = list(map(lambda path: path.split('/')[-1], cf.list_subdirectory(dbutils, '{lnd}/crw/{schema}__{table}/'.format(lnd=lnd, schema=table['schema'], table=table['table']))))
prm_years = sorted(var_prm_dict.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

adl_sink = "{adl_path}{raw}/crw/{schema}/{table}".format(adl_path=var_adls_uri, raw=raw, schema=table["schema"], table=table["table"])
adl_sink

# COMMAND ----------

for year in years:
  adl_origin = '{adl_path}{lnd}/crw/{schema}__{table}/{year}/'.format(adl_path=var_adls_uri, lnd=lnd, schema=table['schema'], table=table['table'], year=year)

  df = spark.read.csv(adl_origin, sep='|', header=True)
  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  
  prm_year = year if year in prm_years else prm_years[-1]
  cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=str(prm_year))

  for origin, rename, _type in var_prm_dict[str(prm_year)]:
    if origin != 'N/A':
      df = df.withColumn(origin, f.col(origin).cast(_type))
      df = df.withColumnRenamed(origin, rename)
    else:
      df = df.withColumn(rename, f.lit(None).cast(_type))
      
  df = df.withColumn('IN_MANT_ESCOLA_PRIV_ONG_OSCIP', f.lit('IN_MANT_ESCOLA_PRIVADA_ONG'))
  
  adl_list_dir = '{lnd}/crw/{schema}__{table}/{year}/'.format(lnd=lnd, schema=table['schema'], table=table['table'], year=year)
  df_adl_files = cf.list_adl_files(spark, dbutils, adl_list_dir)
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')
  
  df.coalesce(1).write.save(path=adl_sink, partitionBy=table["partition_column_raw"], format="parquet", mode="overwrite")