# Databricks notebook source
#from cni_connectors_fornecedor import adls_gen1_connector as adls_conn
from cni_connectors import adls_gen1_connector as adls_conn

import json
import re
import os
import crawler.functions as cf
import pyspark.sql.functions as f

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

'''
table = {
   'schema': 'inep_saeb',
   'table': 'aluno',
   'prm_path': '/tmp/dev/prm/usr/inep_saeb/KC2332_Prova_Brasil_mapeamento_unificado_raw_V2.1.xlsx',
   'idp_tables': ['TS_QUEST_ALUNO', 'TS_RESULTADO_ALUNO', 'TS_RESPOSTA_ALUNO'],
   'grp_tables': {
     '2013': ['2013_TS_ALUNO_5EF', '2013_TS_ALUNO_9EF', '2013_TS_ALUNO_3EM'],
     '2015': ['2015_TS_ALUNO_5EF', '2015_TS_ALUNO_9EF', '2015_TS_ALUNO_3EM'],
     '2017': ['2017_TS_ALUNO_5EF', '2017_TS_ALUNO_9EF', '2017_TS_ALUNO_3EM_ESC', '2017_TS_ALUNO_3EM_AG'],
     '2019': ['2019_TS_ALUNO_5EF', '2019_TS_ALUNO_9EF', '2019_TS_ALUNO_34EM', '2019_TS_ALUNO_2EF']
   },
   'year_tables': {
     'partition': 'ID_PROVA_BRASIL',
     'sheets': ['Prova_Brasil_2013', 'Prova_Brasil_2015', 'Prova_Brasil_2017',"Prova_Brasil_2019"]
   }
 }

adf = {
   "adf_factory_name": "cnibigdatafactory",
   "adf_pipeline_name": "org_raw_estrutura_territorial",
   "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
   "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
   "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
   "adf_trigger_time": "2020-06-08T01:42:41.5507749Z",
   "adf_trigger_type": "PipelineActivity"
 }
dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw"}}
'''

# COMMAND ----------

def remove_ba_layer(folder):
  val = 2 if folder.startswith('/') else 1
  return '/'.join(folder.split('/')[val:])

# COMMAND ----------

def fill_column(df, org, dst, _type, file_type=None):
  if org != dst and dst in df.columns:
    dst = dst + '_tmp'
  
  if org != dst and org != 'N/A':
    df = df.withColumnRenamed(org, dst)
  
  _type = _type.lower()
  if org == 'N/A':
    dft = None
    if dst == 'TP_ARQUIVO':
      dft = file_type
    elif dst == 'TP_FONTE':
      dft = 'SAEB'
    
    df = df.withColumn(dst, f.lit(dft).cast(_type))
  else:
    df = df.withColumn(dst, f.col(dst).cast(_type))
  
  return df

# COMMAND ----------

def execute(parse_ba_doc, parse_ba_doc_metadata, mode='overwrite', file_sep=';', dt_insertion_raw=None):
  append_mode = mode == 'append'
  
  for sheet in parse_ba_doc:
    mtd = parse_ba_doc_metadata[sheet]
    
    lnd_path = '{adl_path}{lnd}/{dst}'.format(adl_path=var_adls_uri, lnd=dls['folders']['landing'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_ORIGEM']))
    df = spark.read.csv(path=lnd_path, sep=file_sep, header=True)
    cf.check_ba_doc(df, parse_ba=parse_ba_doc, sheet=sheet)
    
    file_type = sheet.split('ALUNO_')
    file_type = file_type[-1] if len(file_type) > 1 else None
    
    if file_type is not None and file_type == '3EM':
      file_type = '3EM_ESC'
    
    for org, dst, _type in parse_ba_doc[sheet]:
      df = fill_column(df, org, dst, _type, file_type)
      
    for col_with_tmp_name in [col for col in df.columns if '_tmp' in col]:
      dst = col_with_tmp_name.split('_tmp')[0]
      df = df.withColumnRenamed(col_with_tmp_name, dst)
    
    df = cf.append_control_columns(df, dt_insertion_raw)
    df_adl_files = cf.list_adl_files(spark, dbutils,  os.path.dirname(mtd['TABELA_ARQUIVO_ORIGEM']))
    df = df.join(df_adl_files, on='nm_arq_in', how='inner')
    df = df.repartition(10)
    
    uri = '{adl_path}{raw}/{dst}'.format(adl_path=var_adls_uri, raw=dls['folders']['raw'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_DESTINO']))
    if append_mode:
      append_mode = False
      
      print(sheet, uri, 'overwrite')
      df.write.option('mergeSchema', 'true').parquet(uri, mode='overwrite')
    else:
      print(sheet, uri, mode)
      df.write.option('mergeSchema', 'true').parquet(uri, mode=mode)

# COMMAND ----------

def execute_unification(partition, parse_ba_doc, parse_ba_doc_metadata):
  first_overwrite = True
  
  for sheet in parse_ba_doc:
    mtd = parse_ba_doc_metadata[sheet]
    
    lnd_path = '{adl_path}{raw}/{dst}'.format(adl_path=var_adls_uri, raw=dls['folders']['raw'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_ORIGEM']))
    df = spark.read.parquet(lnd_path)
    cf.check_ba_doc(df, parse_ba=parse_ba_doc, sheet=sheet)
    
    for org, dst, _type in parse_ba_doc[sheet]:
      df = fill_column(df, org, dst, _type)
      
    for col_with_tmp_name in [col for col in df.columns if '_tmp' in col]:
      dst = col_with_tmp_name.split('_tmp')[0]
      df = df.withColumnRenamed(col_with_tmp_name, dst)
    
    if first_overwrite:
      first_overwrite = False
      mode = 'overwrite'
    else:
      mode = 'append'
    
    uri = '{adl_path}{raw}/{dst}'.format(adl_path=var_adls_uri, raw=dls['folders']['raw'], dst=remove_ba_layer(mtd['TABELA_ARQUIVO_DESTINO']))
    print(sheet, uri, mode)
    
    df.write.partitionBy(partition).parquet(uri, mode=mode)

# COMMAND ----------

headers = {
  'name_header': 'Tabela Origem',
  'pos_header': 'B',
  'pos_org': 'C',
  'pos_dst': 'E',
  'pos_type': 'F',
}

metadata = {
  'name_header': 'Processo',
  'pos_header': 'B'
}

# COMMAND ----------

dt_insertion_raw = adf["adf_trigger_time"].split(".")[0]

# COMMAND ----------

cf.delete_files(dbutils, '/raw/crw/inep_saeb/ts_resposta_aluno')
cf.delete_files(dbutils, '/raw/crw/inep_saeb/ts_resultado_aluno')
cf.delete_files(dbutils, '/raw/crw/inep_saeb/ts_quest_aluno')

idp_parse, idp_metadata = cf.parse_ba_doc(dbutils, table['prm_path'], headers, metadata, sheet_names=table['idp_tables'])
execute(parse_ba_doc=idp_parse, parse_ba_doc_metadata=idp_metadata, dt_insertion_raw=dt_insertion_raw)

# COMMAND ----------

cf.delete_files(dbutils, '/raw/crw/inep_saeb/prova_brasil_2013')
cf.delete_files(dbutils, '/raw/crw/inep_saeb/prova_brasil_2015')
cf.delete_files(dbutils, '/raw/crw/inep_saeb/prova_brasil_2017')
cf.delete_files(dbutils, '/raw/crw/inep_saeb/prova_brasil_2019')

for year in table['grp_tables']:
  sheets = table['grp_tables'][year]
  
  grp_parse, grp_metadata = cf.parse_ba_doc(dbutils, table['prm_path'], headers, metadata, sheet_names=sheets)
  execute(parse_ba_doc=grp_parse, parse_ba_doc_metadata=grp_metadata, mode='append', file_sep=',', dt_insertion_raw=dt_insertion_raw)

# COMMAND ----------

cf.delete_files(dbutils, '/raw/crw/inep_saeb/saeb_aluno_unificado')

year_parse, year_metadata = cf.parse_ba_doc(dbutils, table['prm_path'], headers, metadata, sheet_names=table['year_tables']['sheets'])
execute_unification(partition=table['year_tables']['partition'], parse_ba_doc=year_parse, parse_ba_doc_metadata=year_metadata)