# Databricks notebook source
# MAGIC %md
# MAGIC # About unified mapping objects:
# MAGIC * There are only schema conformation transformations, so there are no other treatments
# MAGIC * This is a RAW that reads from landing and writes in raw for crawler processes
# MAGIC * This notebook is very specific to each of the tasks they are performing

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC Processo	org_raw_microdados_enem
# MAGIC Tabela/Arquivo Origem	\lnd\crw\inep_enem\microdados_enem
# MAGIC Tabela/Arquivo Destino	\raw\crw\inep_enem\microdados_enem
# MAGIC Particionamento Tabela/Arquivo Destino	Ano do ENEM (campo nu_ano)
# MAGIC Descrição Tabela/Arquivo Destino	Dados do ENEM em layout unificado a partir de 2009
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

from pyspark.sql.functions import col, lit, max, from_utc_timestamp, current_timestamp, input_file_name, monotonically_increasing_id, substring_index, row_number, split, lit, struct, substring, lag, trim
from pyspark.sql.window import Window 

import crawler.functions as cf
import datetime
import json
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

# table = {
#   'schema': 'inep_enem', 
#   'table': 'dados',
#   'table_cmpt': 'dados_complementares',
#   'table_dict': 'dicionario',
#   'table_sink': 'microdados_enem',
#   'partition_column_raw': 'NU_ANO', 
#   'join_column': 'NU_INSCRICAO', 
#   'join_type': 'inner', 
#   'prm_path': '/prm/usr/inep_enem/KC2332_ENEM_mapeamento_unificado_raw_V2.xlsx'
# }

# adf = {
#   "adf_factory_name": "cnibigdatafactory",
#   "adf_pipeline_name": "org_raw_microdados_enem",
#   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
#   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
#   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
#   "adf_trigger_time": "2020-06-22T16:00:00.0000000Z",
#   "adf_trigger_type": "PipelineActivity"
# }

# dls = {"folders":{"landing":"/lnd", "error":"/tmp/dev/err", "staging":"/tmp/dev/stg", "log":"/tmp/dev/log", "raw":"/raw"}}

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']

# COMMAND ----------

options = {"adls_uri": var_adls_uri, "control_fields": {"nm_arq_in": substring_index(input_file_name(), "/", -1), "nr_reg": lit(monotonically_increasing_id()), "dh_arq_in": lit(None), "dh_insercao_raw": lit(adf["adf_trigger_time"].split(".")[0])}}

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_json = cf.parse_ba_doc(dbutils, table["prm_path"], headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get years present in BA documentation 

# COMMAND ----------

var_years = [int(year) for year in list(var_prm_json.keys())]
var_years.sort()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate over the years, apply transformations and save dataframe

# COMMAND ----------

def read_enem_with_control_fields(var_year:int, var_table:str, options:dict, dls:dict, table:dict):
  """
  It's a specific implementation for inep_enem that reads that with particular settings of delimiter, header and parsing. There's a case where all the columns come together concatenated, so it's needed to get the positions that belongs to each column from the dictionary.
  Also adds control fields nm_arq_in, dh_arq_in, dh_arq_in and dh_insercao_raw to the dataframe.
  In order to get the timestamp of file upload, we need this information that comes using the egg library crawler.functions.list_adl_files().
  """
  var_delimiter = ',' if var_year in [2012, 2014, 2015] else ';'
  var_years_need_parse = [2011]
  var_header = False if var_year in var_years_need_parse else True
  var_encoding = "UTF-8"
  var_source_table = "{}{}/{}/{}__{}/{}".format(options["adls_uri"], lnd, "crw", table["schema"], var_table, var_year).strip().lower()
  var_source_dicionario = "{}{}/{}/{}__{}/{}".format(options["adls_uri"], lnd, "crw", table["schema"], table["table_dict"], var_year).strip().lower()
  var_list_columns_dicionario = ["NOME_VARIAVEL", "INICIO", "TAMANHO"]
  
  df = spark.read.csv(var_source_table, sep=var_delimiter, header=var_header, mode="FAILFAST", encoding=var_encoding)
  df = (df
        .withColumn("nm_arq_in_{}".format(var_table), options["control_fields"]["nm_arq_in"].cast("string"))
        .withColumn("nr_reg_{}".format(var_table), row_number().over(Window.partitionBy("nm_arq_in_{}".format(var_table))
                                                                     .orderBy(options["control_fields"]["nr_reg"])).cast("integer")))
  df = df.repartition(200)
  
  if var_year in var_years_need_parse:
    try:
      dicionario = spark.read.parquet(var_source_dicionario).select(*var_list_columns_dicionario)
    except:
      raise Exception('Invalid or not found file at: {}'.format(var_source_dicionario))
    dicionario_dados_complementares = dicionario.filter((col("NOME_VARIAVEL").like("Q%")) | (col("NOME_VARIAVEL").isin("IN_QSE", "NU_INSCRICAO")))
    dicionario_dados = dicionario.subtract(dicionario_dados_complementares.filter(col("NOME_VARIAVEL") != "NU_INSCRICAO"))
    var_list_dict = dicionario_dados.collect() if var_table == "dados" else dicionario_dados_complementares.collect()
    var_dicionario = [{"column": row[0].strip("\n").strip(), 
                       "start": int(row[1]), 
                       "length": int(row[2])} for row in var_list_dict]
    for i in range(0, len(var_dicionario)):
      df = df.withColumn(var_dicionario[i]["column"], substring("_c0", var_dicionario[i]["start"], var_dicionario[i]["length"]))
    df = df.drop("_c0")
      
  # Get file names and timestamps
  adl_files_dir = "{}/{}/{}__{}/{}".format(lnd, "crw", table["schema"], var_table, var_year)
  adl_file_time = cf.list_adl_files(spark, dbutils, adl_files_dir)
  adl_file_time = adl_file_time.withColumnRenamed("nm_arq_in", "nm_arq_in_{}".format(var_table))
  adl_file_time = adl_file_time.withColumnRenamed("dh_arq_in", "dh_arq_in_{}".format(var_table))

  # Add control fields
  df = df.join(adl_file_time, ["nm_arq_in_{}".format(var_table)], "inner")
  return df

# COMMAND ----------

def read_and_unify_tables(var_year:int, table:dict, options:dict, dls:dict, var_dados:str, var_dados_complementares:str):
  """
  Read data saved by the crawler on landing and save it on a dataframe. It returns the dataframe with data from dados and, if it's the case, dados_complementares with the respective control fields in struct type.
  Params:
    - var_year: year in integer, corresponding to the key in prm_json and to the folder name on level after schema.
    - table: Dictionary with information about table name and schema
    - options: Dictionary of options that contains specific information about control field columns and adls path.
    - dls: Dictionary of adls folders.
    - var_dados: Name of directory that contains data corresponding to main data.
    - var_dados_complementares: Name of directory that contains data corresponding to complementary data.
  """
  var_source_dados = "{}{}/{}/{}__{}".format(options["adls_uri"], lnd, "crw", table["schema"], var_dados).strip().lower()
  var_source_dados_complementares = "{}{}/{}/{}__{}".format(options["adls_uri"], lnd, "crw", table["schema"], var_dados_complementares).strip().lower()
  
  #Check if the data table exists for the current year and read if it's true.
  if var_year in [int(path.name.strip('/')) for path in dbutils.fs.ls(var_source_dados)]:
    df = read_enem_with_control_fields(var_year, var_dados, options, dls, table)

    #Check if the dados_complementares table exists for the current year and, if it's make the join of the two tables.
    if var_year in [int(path.name.strip('/')) for path in dbutils.fs.ls(var_source_dados_complementares)]:
      df_ = read_enem_with_control_fields(var_year, var_dados_complementares, options, dls, table)
 
      #Join and create structfields for control fields
      df = df.join(df_, table["join_column"], table["join_type"])\
      .withColumn("nm_arq_in", struct(col("nm_arq_in_{}".format(var_dados)).alias(var_dados),
                                                 col("nm_arq_in_{}".format(var_dados_complementares)).alias(var_dados_complementares)))\
      .withColumn("nr_reg", struct(col("nr_reg_{}".format(var_dados)).alias(var_dados), 
                                   col("nr_reg_{}".format(var_dados_complementares)).alias(var_dados_complementares)))\
      .withColumn("dh_arq_in", struct(col("dh_arq_in_{}".format(var_dados)).alias(var_dados), 
                                      col("dh_arq_in_{}".format(var_dados_complementares)).alias(var_dados_complementares)))\
      .withColumn("dh_insercao_raw", options["control_fields"]["dh_insercao_raw"].cast("timestamp"))\
      .drop("nm_arq_in_{}".format(var_dados), "nm_arq_in_{}".format(var_dados_complementares), 
            "nr_reg_{}".format(var_dados), "nr_reg_{}".format(var_dados_complementares), 
            "dh_arq_in_{}".format(var_dados), "dh_arq_in_{}".format(var_dados_complementares))
    else:
      #If there is only one data source, just create StructFields with the corresponding column name for the control fields. There's no other way to do it before because the control fields must be added for a specific data source.
      df = df.withColumn("nm_arq_in", struct(col("nm_arq_in_{}".format(var_dados)).alias(var_dados)))\
      .withColumn("nr_reg", struct(col("nr_reg_{}".format(var_dados)).alias(var_dados)))\
      .withColumn("dh_arq_in", struct(col("dh_arq_in_{}".format(var_dados)).alias(var_dados)))\
      .withColumn("dh_insercao_raw", options["control_fields"]["dh_insercao_raw"].cast("timestamp"))\
      .drop("nm_arq_in_{}".format(var_dados), "nr_reg_{}".format(var_dados), "dh_arq_in_{}".format(var_dados))
  else:
    raise Exception("Sorry there's no file for year {}".format(var_year))
  
  return df

# COMMAND ----------

def enem_schema_unified(var_year:int, options:dict, table:dict, dls:dict, df, var_prm_json:dict, list_of_destination_columns:list):
  """
     This functions aims to copy ENEM data from landing to raw for a specific year specified on parameters, applying transformations such as renaming columns, changing types and allow schema changes with parameters specified on the json extracted from the BA's excel specification. 
     Params:
       - var_year: year in integer, corresponding to the key in prm_json and to the folder name on level after schema.
       - options: Dictionary of options that contains specific information about control field columns and adls path.
       - table: Dictionary with information about table name, schema and intermediate tables.
       - dls: Dictionary of adls folders.
       - df: dataframe of data extracted from landing on "read_enem_from_landing" function.
       - var_prm_json: Dictionary extracted from parse_ba_doc function. Should contain year as keys and lists of 3 string values each as content, that corresponds to the origin column, destination column and type of the column.
       - list_of_destination_columns: list of the destination columns from "check_documentation" function.
       - var_get_dh_insercao_raw: boolean that indicates the need to return the dh_insercao_raw of the load.
    """
  #Here we first get the origin columns that are not "N/A", so in these cases we can create or replace a column, naming it the corresponding destination column and casting it to the corresponding type column. 
  #If the origin column is "N/A", so we can simply create a new Null column with the corresponding destination column name and type.
  for row in (var_prm_json[str(var_year)]):
    if row[0] != "N/A":  
      df = df.withColumn("{}".format(row[1]), col("{}".format(row[0])).cast("{}".format(row[2])))
    elif row[0] == "N/A":
      df = df.withColumn("{}".format(row[1]), lit(None).cast("{}".format(row[2])))
  
  #Conforming schema
  #Note that the original column will not necessarily be replaced in the transformations above. So it's needed to select only the columns that matter
  list_of_destination_columns.extend(list(options["control_fields"].keys()))
  df = df.select(*list_of_destination_columns)
  
  # No dia 02/06/2020 o cliente solicitou que fosse removido os campos, para que não fosse gravado na tabela de destino.
  df = df.drop("Q046_ENSINO_MEDIO_CONCLUIDO", "Q047_TIPO_ESCOLA_ENSINO_MEDIO")
  
  # Writing in sink
  # Finally we write in sink. As we're using dynamic writing, we'll overwrite only partitions. If the schema changes, read with "mergeSchema" option.
  return df

# COMMAND ----------

#Iterate over the years only for the years that are greater to the years we have already written on raw so this will ensure an incremental year overwrite load. On the last year, get var_dh_insercao_raw.
for var_year in var_years:
  df = read_and_unify_tables(var_year, table, options, dls, table["table"], table["table_cmpt"])
  
  exclude_cols = ['nm_arq_in', 'nr_reg', 'dh_arq_in', 'dh_insercao_raw']
  list_of_destination_columns = cf.check_ba_doc(df, parse_ba=var_prm_json, sheet=str(var_year), exclude_cols=exclude_cols, return_dst_cols=True)
  
  df = enem_schema_unified(var_year, options, table, dls, df, var_prm_json, list_of_destination_columns)
  var_sink = "{}{}/{}/{}/{}".format(options["adls_uri"], raw, "crw", table["schema"], table["table_sink"])
  
  print(var_year, var_sink)
  df.repartition(20).write.partitionBy(table["partition_column_raw"]).save(path=var_sink, format="parquet", mode="overwrite")