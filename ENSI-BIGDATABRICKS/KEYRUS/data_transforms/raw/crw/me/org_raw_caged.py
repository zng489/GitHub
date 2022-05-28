# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	org_raw_ibge_caged
# MAGIC Tabela/Arquivo Origem -> lnd - ibge - caged
# MAGIC Tabela/Arquivo Destino ->	raw - crw - ibge - caged
# MAGIC Particionamento Tabela/Arquivo Destino	Ano da qual é a movimentação (campocd_ano_movimentacao)
# MAGIC Descrição Tabela/Arquivo Destino	
# MAGIC Tipo Atualização	F = substituição full (truncate/insert)
# MAGIC Detalhe Atuaização	N/A
# MAGIC Periodicidade/Horario Execução	Anual depois da disponibilização dos dados na landing zone
# MAGIC </pre>

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw specific parameter section

# COMMAND ----------

from unicodedata import normalize

import crawler.functions as cf
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

# MAGIC %md
# MAGIC This cell is for implementing widgets.get and json convertion
# MAGIC Provided that it is still not implemented, i'll mock it up by setting the necessary stuff for the table I'm working with.
# MAGIC 
# MAGIC Remember that when parsing any json, we must handle any possibility of strange char, escapes ans whatever comes dirt from Data Factory!

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=table["schema"], table=table["table"])

# COMMAND ----------

raw_path = "{raw}/crw/{schema}/{table}/".format(raw=raw, schema=table["schema"], table=table["table"])

adl_sink = "{adl_path}{raw_path}".format(adl_path=var_adls_uri, raw_path=raw_path)
adl_sink

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

cf.delete_files(dbutils, raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

def rename_columns(df):
  regex = re.compile(r'[.,;{}()\n\t=]')
  for col in df.columns:
      col_renamed = regex.sub('', normalize('NFKD', col.strip())
                             .encode('ASCII', 'ignore')        
                             .decode('ASCII')                 
                             .replace(' ', '_')                    
                             .replace('-', '_')
                             .replace('/', '_')
                             .upper())
      df = df.withColumnRenamed(col, col_renamed)
  return df

# COMMAND ----------

def select_columns(year):
  for org, dst, _type in var_prm_dict[year]:
      if org == 'N/A':
        yield f.lit(None).cast(_type).alias(dst)
      else:
        col = f.col(org)
        if _type.lower() == 'double':
          col = f.regexp_replace(org, ',', '.')
        
        yield col.cast(_type).alias(dst)

# COMMAND ----------

for lnd_year_path in sorted(cf.list_subdirectory(dbutils, lnd_path), key=lambda p: int(p.split('/')[-1])):
  year = lnd_year_path.split('/')[-1]
  
  df = spark.read.csv(var_adls_uri + '/' + lnd_year_path, sep=';', encoding='UTF-8', header=True)
  df = rename_columns(df)

  cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=year)
  df = df.select(*select_columns(year))
  
  df = cf.append_control_columns(df, dh_insercao_raw=adf["adf_trigger_time"].split(".")[0])
  df_adl_files = cf.list_adl_files(spark, dbutils, lnd_year_path)
  df = df.join(f.broadcast(df_adl_files), on='nm_arq_in', how='inner')
  
  (df
   .repartition(5)
   .write
   .partitionBy('CD_ANO_COMPETENCIA', 'CD_ANO_MES_COMPETENCIA_DECLARADA')
   .parquet(path=adl_sink, mode='append'))
