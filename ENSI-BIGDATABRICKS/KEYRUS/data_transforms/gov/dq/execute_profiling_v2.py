# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, dynamic_overwrite="dynamic")

# COMMAND ----------

import json
import datetime
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql import *
from decimal import Decimal
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

# COMMAND ----------

# O item marca vai ser definido exatamente aqui, já que não há mais o notebook loop_profile (ainda bem)
#var_marca = datetime.date.today().strftime("%Y-%m-%d")
var_marca = sqlContext.createDataFrame([(dbutils.widgets.get("marca") if dbutils.widgets.get("marca") != 'None' else None,)], ['s',])
var_marca = var_marca.select(substring(var_marca.s, 1, 10).alias('s'))
var_marca = var_marca.first().s
print(var_marca)

# COMMAND ----------

# Trazendo os intens em varíaveis distintas permite desacoplar o ADF do Databricks.
var_source = dbutils.widgets.get("source") if dbutils.widgets.get("source") != 'None' else None
var_schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") != 'None' else None
var_table = dbutils.widgets.get("table") if dbutils.widgets.get("table") != 'None' else None
var_chave = dbutils.widgets.get("chave") if dbutils.widgets.get("chave") != 'None' else None
var_col_version = dbutils.widgets.get("col_version") if dbutils.widgets.get("col_version") != 'None' else None
var_path_table = dbutils.widgets.get("path_table") if dbutils.widgets.get("path_table") != 'None' else None
var_tipo_carga = dbutils.widgets.get("tipo_carga") if dbutils.widgets.get("tipo_carga") != 'None' else None

var_path_profiling = dbutils.widgets.get("path_profiling")

# COMMAND ----------

# função de retorno da tabela
# Esses itens que essa função precisa do gov_table, podemos trazer diretamente do ADF, já que lá realizamos o lookup na tabela e teremos o custo de fazer isso uma única vez para cada item, numa porção mais fácil de gerenciar. 

def gera_tabela_raw(path):
  key = var_chave
  col_version = var_col_version
  tipo_carga = var_tipo_carga
  print("Tipo carga: {}".format(tipo_carga))
  
  if tipo_carga == None:
    tipo_carga = 'na'  
    key = 'na'
    col_version = 'na'
  
  # Localiza e lê a tabela
  tb = spark.read.format("parquet").load(var_adls_uri + path)

  # Colunas a desconsiderar no profiling, são as colunas de controle inseridas peloas processos do Datalake.
  var_columns_to_drop = ["nm_arq_in", "nr_reg", 
                     "dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                     "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                     "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_exclusao_valor_pessoa",\
                     "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                     "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                     "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp","D_E_L_E_T_","R_E_C_D_E_L_"]
    
  df_fim = tb    
  if tipo_carga in ['incremental','full_balance']:
    # gera o dataframe final com a última versão de cada chave
    key = key.replace(" ", "").split(',')
    chave = key.copy()
    chave.append(col_version)
    
    tb_last_version = tb.groupBy(key).agg(max(col_version).alias(col_version))
    df_fim = tb.join(tb_last_version, chave, 'inner')    
    
  df_fim = df_fim.drop(*var_columns_to_drop)
  return df_fim

# COMMAND ----------

df_lake = gera_tabela_raw(path=var_path_table)
names = df_lake.schema.names

# COMMAND ----------

def get_dtype(df,colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]

# COMMAND ----------

# cria a lista inicial = dados do dataframe
var_data = []

def assemble_row(campo, name, metric):
  return (var_source, var_schema, var_table, campo, name, metric, var_tipo)

# varre todos os nomes de campos da tabela em questão
for c in names:
  
    alvo = df_lake.select(c)
    var_tipo = get_dtype(alvo, c)

    var_distinct_count = Decimal(alvo.distinct().count())
    var_data.append(assemble_row(c, 'distinct_count', var_distinct_count))    
        
    var_null_count = Decimal(alvo.filter(c + ' is null').count())
    var_data.append(assemble_row(c, 'null_count', var_null_count))    
    
    var_fill_count = Decimal(alvo.count() - var_null_count)
    var_data.append(assemble_row(c, 'fill_count', var_fill_count))   
    
    var_more_than_once = Decimal(alvo.groupBy(c).count().where('count > 1').count())
    var_data.append(assemble_row(c, 'more_than_once', var_more_than_once))    
    
    var_only_once= Decimal(alvo.groupBy(c).count().where('count = 1').count())
    var_data.append(assemble_row(c, 'only_once', var_only_once))
    
    var_max_str_len = alvo.withColumn('tamanho', length(c)).agg({'tamanho': "max"}).collect()[0][0]
    if var_max_str_len is None:
      var_max_str_len = 0
    var_max_str_len = Decimal(var_max_str_len)
    var_data.append(assemble_row(c, 'max_str_len', var_max_str_len))
    
    if var_tipo=="tinyint" or var_tipo=="smallint" or var_tipo=="int" or var_tipo=="bigint" or var_tipo=="float" or var_tipo=="double" or "decimal" in var_tipo:
      var_max_value = alvo.select(max(c)).collect()[0][0]
      if str(var_max_value) == 'None':
        var_max_value = 0
      var_max_value = Decimal(var_max_value)
      var_data.append(assemble_row(c, 'max_value', var_max_value))      

      var_min_value = alvo.select(min(c)).collect()[0][0]
      if str(var_min_value) == 'None':
        var_min_value = 0
      var_min_value = Decimal(var_min_value)   
      var_data.append(assemble_row(c, 'min_value', var_min_value))   
      
# column names of dataframe
var_columns = ['source_name','schema_name','table_name','field_name','metric','value','tipo']
  
# creating a dataframe 
dataframe = spark.createDataFrame(var_data, var_columns)

# adding 'created_at' to dataframe
dataframe = dataframe.withColumn("created_at", to_timestamp(lit(var_marca + " 00:00:00.0000"), 'yyyy-MM-dd HH:mm:ss.SSSS')).drop(dataframe.tipo)

# COMMAND ----------

#display(dataframe)

# COMMAND ----------

dataframe.write.format("delta").mode("append").save(var_adls_uri + var_path_profiling)
