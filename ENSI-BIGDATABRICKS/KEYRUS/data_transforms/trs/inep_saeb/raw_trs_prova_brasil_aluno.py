# Databricks notebook source
import crawler.functions as cf

import cni_connectors.adls_gen1_connector as connector
import pyspark.sql.functions as f

import json
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'pb_qt_aluno': '/crw/inep_prova_brasil/ts_quest_aluno/',
#   'pb_rs_aluno': '/crw/inep_prova_brasil/ts_resultado_aluno/',
#   'pb_rp_aluno': '/crw/inep_prova_brasil/ts_resposta_aluno/',
#   'sb_qt_aluno': '/crw/inep_saeb/ts_quest_aluno/',
#   'sb_rs_aluno': '/crw/inep_saeb/ts_resultado_aluno/',
#   'sb_rp_aluno': '/crw/inep_saeb/ts_resposta_aluno/',
#   'raw_sb_table': '/crw/inep_saeb/saeb_aluno_unificado/',
#   'trs_pb_2011_table': '/inep_saeb/prova_brasil_2011/',
#   'trs_pb_unified_table': '/inep_saeb/prova_brasil_saeb_aluno_unificado/',
#   'partition_col': 'ID_PROVA_BRASIL',
#   'prm_path': '/prm/usr/inep_prova_brasil/KC2332_SAEB_Prova_Brasil_mapeamento_trusted.xlsx'
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_convenio_ensino_prof_carga_horaria',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-10T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

def prepare_raw_df(qt_aluno, rs_aluno, rp_aluno):
  df_qt_aluno = spark.read.parquet(var_adls_uri + raw + qt_aluno).alias('qt')
  df_rs_aluno = spark.read.parquet(var_adls_uri + raw + rs_aluno).alias('rs')
  df_rp_aluno = spark.read.parquet(var_adls_uri + raw + rp_aluno).alias('rp')
  
  df = df_qt_aluno.join(df_rs_aluno, on='ID_ALUNO', how='inner')
  df = df.join(df_rp_aluno, on='ID_ALUNO', how='inner')
  
  return df

# COMMAND ----------

def fill_column(df, org, dst, _type):
  if org == 'PESO':
    df = df.withColumn(dst, f.col(org).cast(_type))
  else:
    if org != dst and dst in df.columns:
      dst = dst + '_tmp'

    if org != dst and org != 'N/A':
      df = df.withColumnRenamed(org, dst)

    _type = _type.lower()
    if org == 'N/A':
      if dst not in ['TP_ARQUIVO', 'TP_FONTE']:
        df = df.withColumn(dst, f.lit(None).cast(_type))
    else:
      df = df.withColumn(dst, f.col(dst).cast(_type))
  
  return df

# COMMAND ----------

def normalize_columns(df, sheet, file_type=None, org_type=None, exclude_columns=None):
  if exclude_columns is None:
    exclude_columns = []
  
  headers = {'name_header': 'Tabela Origem', 'pos_header': 'B', 'pos_org': 'C', 'pos_dst': 'E', 'pos_type': 'F'}
  metadata = {'name_header': 'Processo', 'pos_header': 'B'}
  
  parse_ba_doc, parse_ba_mtd = cf.parse_ba_doc(dbutils, table['prm_path'], headers, metadata, sheet_names=[sheet])
  cf.check_ba_doc(df, parse_ba=parse_ba_doc, sheet=sheet)
  
  for org, dst, _type in parse_ba_doc[sheet]:
    if org not in exclude_columns:
      df = fill_column(df, org, dst, _type)

  for col_with_tmp_name in [col for col in df.columns if '_tmp' in col]:
    dst = col_with_tmp_name.split('_tmp')[0]
    df = df.withColumnRenamed(col_with_tmp_name, dst)
  
  if file_type is not None:
    df = df.withColumn('TP_ARQUIVO', f.lit(file_type))
  if org_type is not None:
    df = df.withColumn('TP_FONTE', f.lit(org_type))
    
  df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))
  
  reorder_columns = [cols[1] for cols in parse_ba_doc[sheet]]
  reorder_columns += ['dh_insercao_trs']
  
  if len(exclude_columns) == 0:
    return df.select(*reorder_columns)
  else:
    return df, reorder_columns

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Prova Brasil

# COMMAND ----------

df = prepare_raw_df(table['pb_qt_aluno'], table['pb_rs_aluno'], table['pb_rp_aluno'])

# COMMAND ----------

cf.delete_files(dbutils, trs + table['trs_pb_2011_table'])

# COMMAND ----------

df_pb_5ef = df.where(f.col('qt.ID_SERIE') == f.lit(5))
cols_pb_5ef = ['qt.ID_PROVA_BRASIL', 'qt.ID_UF', 'qt.ID_MUNICIPIO', 'qt.ID_ESCOLA', 'qt.ID_DEPENDENCIA_ADM', 'qt.ID_LOCALIZACAO', 'qt.ID_TURMA', 'qt.ID_TURNO', 'qt.ID_SERIE', 'qt.ID_ALUNO', 'qt.IN_SITUACAO_CENSO', 'rs.IN_PREENCHIMENTO', 'rp.ID_CADERNO', 'rp.ID_BLOCO_1', 'rp.ID_BLOCO_2', 'rp.TX_RESP_BLOCO_1_LP2', 'rp.TX_RESP_BLOCO_2_LP2', 'rp.TX_RESP_BLOCO_1_MT2', 'rp.TX_RESP_BLOCO_2_MT2', 'rs.IN_PROFICIENCIA', 'rs.PESO', 'rs.PROFICIENCIA_LP', 'rs.DESVIO_PADRAO_LP', 'rs.PROFICIENCIA_LP_SAEB', 'rs.DESVIO_PADRAO_LP_SAEB', 'rs.PROFICIENCIA_MT', 'rs.DESVIO_PADRAO_MT', 'rs.PROFICIENCIA_MT_SAEB', 'rs.DESVIO_PADRAO_MT_SAEB', 'qt.IN_PREENCHIMENTO_QUESTIONARIO', 'qt.TX_RESP_Q001', 'qt.TX_RESP_Q002', 'qt.TX_RESP_Q003', 'qt.TX_RESP_Q004', 'qt.TX_RESP_Q005', 'qt.TX_RESP_Q006', 'qt.TX_RESP_Q007', 'qt.TX_RESP_Q008', 'qt.TX_RESP_Q009', 'qt.TX_RESP_Q010', 'qt.TX_RESP_Q011', 'qt.TX_RESP_Q012', 'qt.TX_RESP_Q013', 'qt.TX_RESP_Q014', 'qt.TX_RESP_Q015', 'qt.TX_RESP_Q016', 'qt.TX_RESP_Q017', 'qt.TX_RESP_Q018', 'qt.TX_RESP_Q019', 'qt.TX_RESP_Q020', 'qt.TX_RESP_Q021', 'qt.TX_RESP_Q022', 'qt.TX_RESP_Q023', 'qt.TX_RESP_Q024', 'qt.TX_RESP_Q025', 'qt.TX_RESP_Q027', 'qt.TX_RESP_Q026', 'qt.TX_RESP_Q028', 'qt.TX_RESP_Q029', 'qt.TX_RESP_Q030', 'qt.TX_RESP_Q031', 'qt.TX_RESP_Q032', 'qt.TX_RESP_Q035', 'qt.TX_RESP_Q036', 'qt.TX_RESP_Q034', 'qt.TX_RESP_Q033', 'qt.TX_RESP_Q037', 'qt.TX_RESP_Q043', 'qt.TX_RESP_Q038', 'qt.TX_RESP_Q044', 'qt.TX_RESP_Q039', 'qt.TX_RESP_Q045', 'qt.TX_RESP_Q040', 'qt.TX_RESP_Q046', 'qt.TX_RESP_Q041', 'qt.TX_RESP_Q047', 'qt.TX_RESP_Q042', 'qt.TX_RESP_Q048', 'qt.TX_RESP_Q049', 'qt.TX_RESP_Q050', 'qt.TX_RESP_Q051', 'qt.TX_RESP_Q052', 'qt.TX_RESP_Q053', 'qt.TX_RESP_Q054']

df_pb_5ef = df_pb_5ef.select(*cols_pb_5ef)
df_pb_5ef = normalize_columns(df_pb_5ef, file_type='5EF', org_type='PROVA BRASIL', sheet='raw_trs_2011_TS_ALUNO_5EF_PB')

target = var_adls_uri + trs + table['trs_pb_2011_table']
df_pb_5ef.coalesce(10).write.parquet(target, mode='overwrite')

target

# COMMAND ----------

df_pb_9ef = df.where(f.col('qt.ID_SERIE') == f.lit(9))
cols_pb_9ef = ['qt.ID_PROVA_BRASIL', 'qt.ID_UF', 'qt.ID_MUNICIPIO', 'qt.ID_ESCOLA', 'qt.ID_DEPENDENCIA_ADM', 'qt.ID_LOCALIZACAO', 'qt.ID_TURMA', 'qt.ID_TURNO', 'qt.ID_SERIE', 'qt.ID_ALUNO', 'qt.IN_SITUACAO_CENSO', 'rs.IN_PREENCHIMENTO', 'rp.ID_CADERNO', 'rp.ID_BLOCO_1', 'rp.ID_BLOCO_2', 'rp.TX_RESP_BLOCO_1_LP2', 'rp.TX_RESP_BLOCO_2_LP2', 'rp.TX_RESP_BLOCO_1_MT2', 'rp.TX_RESP_BLOCO_2_MT2', 'rs.IN_PROFICIENCIA', 'rs.PESO', 'rs.PROFICIENCIA_LP', 'rs.DESVIO_PADRAO_LP', 'rs.PROFICIENCIA_LP_SAEB', 'rs.DESVIO_PADRAO_LP_SAEB', 'rs.PROFICIENCIA_MT', 'rs.DESVIO_PADRAO_MT', 'rs.PROFICIENCIA_MT_SAEB', 'rs.DESVIO_PADRAO_MT_SAEB', 'qt.IN_PREENCHIMENTO_QUESTIONARIO', 'qt.TX_RESP_Q001', 'qt.TX_RESP_Q002', 'qt.TX_RESP_Q003', 'qt.TX_RESP_Q004', 'qt.TX_RESP_Q005', 'qt.TX_RESP_Q006', 'qt.TX_RESP_Q007', 'qt.TX_RESP_Q008', 'qt.TX_RESP_Q009', 'qt.TX_RESP_Q010', 'qt.TX_RESP_Q011', 'qt.TX_RESP_Q012', 'qt.TX_RESP_Q013', 'qt.TX_RESP_Q014', 'qt.TX_RESP_Q015', 'qt.TX_RESP_Q016', 'qt.TX_RESP_Q017', 'qt.TX_RESP_Q018', 'qt.TX_RESP_Q019', 'qt.TX_RESP_Q020', 'qt.TX_RESP_Q021', 'qt.TX_RESP_Q022', 'qt.TX_RESP_Q023', 'qt.TX_RESP_Q024', 'qt.TX_RESP_Q025', 'qt.TX_RESP_Q027', 'qt.TX_RESP_Q026', 'qt.TX_RESP_Q028', 'qt.TX_RESP_Q029', 'qt.TX_RESP_Q030', 'qt.TX_RESP_Q031', 'qt.TX_RESP_Q032', 'qt.TX_RESP_Q035', 'qt.TX_RESP_Q036', 'qt.TX_RESP_Q037', 'qt.TX_RESP_Q034', 'qt.TX_RESP_Q033', 'qt.TX_RESP_Q038', 'qt.TX_RESP_Q044', 'qt.TX_RESP_Q039', 'qt.TX_RESP_Q045', 'qt.TX_RESP_Q040', 'qt.TX_RESP_Q046', 'qt.TX_RESP_Q047', 'qt.TX_RESP_Q041', 'qt.TX_RESP_Q048', 'qt.TX_RESP_Q042', 'qt.TX_RESP_Q049', 'qt.TX_RESP_Q043', 'qt.TX_RESP_Q050', 'qt.TX_RESP_Q051', 'qt.TX_RESP_Q052', 'qt.TX_RESP_Q053', 'qt.TX_RESP_Q054', 'qt.TX_RESP_Q055', 'qt.TX_RESP_Q058', 'qt.TX_RESP_Q056', 'qt.TX_RESP_Q057']

df_pb_9ef = df_pb_9ef.select(*cols_pb_9ef)
df_pb_9ef = normalize_columns(df_pb_9ef, file_type='9EF', org_type='PROVA BRASIL', sheet='raw_trs_2011_TS_ALUNO_9EF_PB')

target = var_adls_uri + trs + table['trs_pb_2011_table']
df_pb_9ef.coalesce(10).write.parquet(target, mode='append')

target

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # SAEB

# COMMAND ----------

df = prepare_raw_df(table['sb_qt_aluno'], table['sb_rs_aluno'], table['sb_rp_aluno'])

# COMMAND ----------

df_sb_5ef = df.where(f.col('qt.ID_SERIE') == f.lit(5))
cols_sb_5ef = ['qt.ID_SAEB', 'qt.ID_REGIAO', 'qt.ID_UF', 'qt.ID_MUNICIPIO', 'qt.ID_ESCOLA', 'qt.ID_DEPENDENCIA_ADM', 'qt.ID_LOCALIZACAO', 'qt.ID_TURMA', 'qt.ID_TURNO', 'qt.ID_SERIE', 'qt.ID_ALUNO', 'qt.IN_SITUACAO_CENSO', 'rs.IN_PREENCHIMENTO', 'rp.ID_CADERNO', 'rp.ID_BLOCO_1', 'rp.ID_BLOCO_2', 'rp.TX_RESP_BLOCO_1_LP', 'rp.TX_RESP_BLOCO_2_LP', 'rp.TX_RESP_BLOCO_1_MT', 'rp.TX_RESP_BLOCO_2_MT', 'rs.IN_PROFICIENCIA', 'rs.IN_PROVA_BRASIL', 'rs.PESO', 'rs.PROFICIENCIA_LP', 'rs.DESVIO_PADRAO_LP', 'rs.PROFICIENCIA_LP_SAEB', 'rs.DESVIO_PADRAO_LP_SAEB', 'rs.PROFICIENCIA_MT', 'rs.DESVIO_PADRAO_MT', 'rs.PROFICIENCIA_MT_SAEB', 'rs.DESVIO_PADRAO_MT_SAEB', 'qt.IN_PREENCHIMENTO', 'qt.TX_RESP_Q001', 'qt.TX_RESP_Q002', 'qt.TX_RESP_Q003', 'qt.TX_RESP_Q004', 'qt.TX_RESP_Q005', 'qt.TX_RESP_Q006', 'qt.TX_RESP_Q007', 'qt.TX_RESP_Q008', 'qt.TX_RESP_Q009', 'qt.TX_RESP_Q010', 'qt.TX_RESP_Q011', 'qt.TX_RESP_Q012', 'qt.TX_RESP_Q013', 'qt.TX_RESP_Q014', 'qt.TX_RESP_Q015', 'qt.TX_RESP_Q016', 'qt.TX_RESP_Q017', 'qt.TX_RESP_Q018', 'qt.TX_RESP_Q019', 'qt.TX_RESP_Q020', 'qt.TX_RESP_Q021', 'qt.TX_RESP_Q022', 'qt.TX_RESP_Q023', 'qt.TX_RESP_Q024', 'qt.TX_RESP_Q025', 'qt.TX_RESP_Q027', 'qt.TX_RESP_Q026', 'qt.TX_RESP_Q028', 'qt.TX_RESP_Q029', 'qt.TX_RESP_Q030', 'qt.TX_RESP_Q031', 'qt.TX_RESP_Q032', 'qt.TX_RESP_Q035', 'qt.TX_RESP_Q036', 'qt.TX_RESP_Q034', 'qt.TX_RESP_Q033', 'qt.TX_RESP_Q037', 'qt.TX_RESP_Q043', 'qt.TX_RESP_Q038', 'qt.TX_RESP_Q044', 'qt.TX_RESP_Q039', 'qt.TX_RESP_Q045', 'qt.TX_RESP_Q040', 'qt.TX_RESP_Q046', 'qt.TX_RESP_Q041', 'qt.TX_RESP_Q047', 'qt.TX_RESP_Q042', 'qt.TX_RESP_Q048', 'qt.TX_RESP_Q049', 'qt.TX_RESP_Q050', 'qt.TX_RESP_Q051', 'qt.TX_RESP_Q052', 'qt.TX_RESP_Q053', 'qt.TX_RESP_Q054']

exclude_columns = ['IN_PREENCHIMENTO']
df_sb_5ef = df_sb_5ef.select(*cols_sb_5ef)
df_sb_5ef, reorder_columns = normalize_columns(df_sb_5ef, file_type='5EF', org_type='SAEB', sheet='raw_trs_2011_TS_ALUNO_5EF', exclude_columns=exclude_columns)

include_columns = list(set(df_sb_5ef.columns) - set(exclude_columns))
repeated_names = [
  ['rs.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_PROVA', 'Int'],
  ['qt.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_QUESTIONARIO', 'Int'],
]
alias_cols = [f.col(org).alias(dst).cast(_type) for org, dst, _type in repeated_names]
include_columns = include_columns + alias_cols

df_sb_5ef = df_sb_5ef.select(*include_columns)
df_sb_5ef = df_sb_5ef.select(*reorder_columns)

target = var_adls_uri + trs + table['trs_pb_2011_table']
df_sb_5ef.coalesce(10).write.parquet(target, mode='append')

target

# COMMAND ----------

df_sb_9ef = df.where(f.col('qt.ID_SERIE') == f.lit(9))
cols_sb_9ef = ['qt.ID_SAEB', 'qt.ID_REGIAO', 'qt.ID_UF', 'qt.ID_MUNICIPIO', 'qt.ID_ESCOLA', 'qt.ID_DEPENDENCIA_ADM', 'qt.ID_LOCALIZACAO', 'qt.ID_TURMA', 'qt.ID_TURNO', 'qt.ID_SERIE', 'qt.ID_ALUNO', 'qt.IN_SITUACAO_CENSO', 'rs.IN_PREENCHIMENTO', 'rp.ID_CADERNO', 'rp.ID_BLOCO_1', 'rp.ID_BLOCO_2', 'rp.TX_RESP_BLOCO_1_LP', 'rp.TX_RESP_BLOCO_2_LP', 'rp.TX_RESP_BLOCO_1_MT', 'rp.TX_RESP_BLOCO_2_MT', 'rs.IN_PROFICIENCIA', 'rs.IN_PROVA_BRASIL', 'rs.PESO', 'rs.PESO', 'rs.PROFICIENCIA_LP', 'rs.DESVIO_PADRAO_LP', 'rs.PROFICIENCIA_LP_SAEB', 'rs.DESVIO_PADRAO_LP_SAEB', 'rs.PROFICIENCIA_MT', 'rs.DESVIO_PADRAO_MT', 'rs.PROFICIENCIA_MT_SAEB', 'rs.DESVIO_PADRAO_MT_SAEB', 'qt.IN_PREENCHIMENTO', 'qt.TX_RESP_Q001', 'qt.TX_RESP_Q002', 'qt.TX_RESP_Q003', 'qt.TX_RESP_Q004', 'qt.TX_RESP_Q005', 'qt.TX_RESP_Q006', 'qt.TX_RESP_Q007', 'qt.TX_RESP_Q008', 'qt.TX_RESP_Q009', 'qt.TX_RESP_Q010', 'qt.TX_RESP_Q011', 'qt.TX_RESP_Q012', 'qt.TX_RESP_Q013', 'qt.TX_RESP_Q014', 'qt.TX_RESP_Q015', 'qt.TX_RESP_Q016', 'qt.TX_RESP_Q017', 'qt.TX_RESP_Q018', 'qt.TX_RESP_Q019', 'qt.TX_RESP_Q020', 'qt.TX_RESP_Q021', 'qt.TX_RESP_Q022', 'qt.TX_RESP_Q023', 'qt.TX_RESP_Q024', 'qt.TX_RESP_Q025', 'qt.TX_RESP_Q027', 'qt.TX_RESP_Q026', 'qt.TX_RESP_Q028', 'qt.TX_RESP_Q029', 'qt.TX_RESP_Q030', 'qt.TX_RESP_Q031', 'qt.TX_RESP_Q032', 'qt.TX_RESP_Q035', 'qt.TX_RESP_Q036', 'qt.TX_RESP_Q037', 'qt.TX_RESP_Q034', 'qt.TX_RESP_Q033', 'qt.TX_RESP_Q038', 'qt.TX_RESP_Q044', 'qt.TX_RESP_Q039', 'qt.TX_RESP_Q045', 'qt.TX_RESP_Q040', 'qt.TX_RESP_Q046', 'qt.TX_RESP_Q047', 'qt.TX_RESP_Q041', 'qt.TX_RESP_Q048', 'qt.TX_RESP_Q042', 'qt.TX_RESP_Q049', 'qt.TX_RESP_Q043', 'qt.TX_RESP_Q050', 'qt.TX_RESP_Q051', 'qt.TX_RESP_Q052', 'qt.TX_RESP_Q053', 'qt.TX_RESP_Q054', 'qt.TX_RESP_Q055', 'qt.TX_RESP_Q058', 'qt.TX_RESP_Q056', 'qt.TX_RESP_Q057']

exclude_columns = ['IN_PREENCHIMENTO']
df_sb_9ef = df_sb_9ef.select(*cols_sb_9ef)
df_sb_9ef, reorder_columns = normalize_columns(df_sb_9ef, file_type='9EF', org_type='SAEB', sheet='raw_trs_2011_TS_ALUNO_9EF', exclude_columns=exclude_columns)

include_columns = list(set(df_sb_9ef.columns) - set(exclude_columns))
repeated_names = [
  ['rs.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_PROVA', 'Int'],
  ['qt.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_QUESTIONARIO', 'Int'],
]
alias_cols = [f.col(org).alias(dst).cast(_type) for org, dst, _type in repeated_names]
include_columns = include_columns + alias_cols

df_sb_9ef = df_sb_9ef.select(*include_columns)
df_sb_9ef = df_sb_9ef.select(*reorder_columns)

target = var_adls_uri + trs + table['trs_pb_2011_table']
df_sb_9ef.coalesce(10).write.parquet(target, mode='append')

target

# COMMAND ----------

df_sb_3em = df.where(f.col('qt.ID_SERIE').isin([12, 13]))
cols_sb_3em = ['qt.ID_SAEB', 'qt.ID_REGIAO', 'qt.ID_UF', 'qt.ID_MUNICIPIO', 'qt.ID_ESCOLA', 'qt.ID_DEPENDENCIA_ADM', 'qt.ID_LOCALIZACAO', 'qt.ID_TURMA', 'qt.ID_TURNO', 'qt.ID_SERIE', 'qt.ID_ALUNO', 'qt.IN_SITUACAO_CENSO', 'rs.IN_PREENCHIMENTO', 'rp.ID_CADERNO', 'rp.ID_BLOCO_1', 'rp.ID_BLOCO_2', 'rp.TX_RESP_BLOCO_1_LP', 'rp.TX_RESP_BLOCO_2_LP', 'rp.TX_RESP_BLOCO_1_MT', 'rp.TX_RESP_BLOCO_2_MT', 'rs.IN_PROFICIENCIA', 'rs.IN_PROVA_BRASIL', 'rs.PESO', 'rs.PESO', 'rs.PROFICIENCIA_LP', 'rs.DESVIO_PADRAO_LP', 'rs.PROFICIENCIA_LP_SAEB', 'rs.DESVIO_PADRAO_LP_SAEB', 'rs.PROFICIENCIA_MT', 'rs.DESVIO_PADRAO_MT', 'rs.PROFICIENCIA_MT_SAEB', 'rs.DESVIO_PADRAO_MT_SAEB', 'qt.IN_PREENCHIMENTO', 'qt.TX_RESP_Q001', 'qt.TX_RESP_Q002', 'qt.TX_RESP_Q003', 'qt.TX_RESP_Q004', 'qt.TX_RESP_Q005', 'qt.TX_RESP_Q006', 'qt.TX_RESP_Q007', 'qt.TX_RESP_Q008', 'qt.TX_RESP_Q009', 'qt.TX_RESP_Q010', 'qt.TX_RESP_Q011', 'qt.TX_RESP_Q012', 'qt.TX_RESP_Q013', 'qt.TX_RESP_Q014', 'qt.TX_RESP_Q015', 'qt.TX_RESP_Q016', 'qt.TX_RESP_Q017', 'qt.TX_RESP_Q018', 'qt.TX_RESP_Q019', 'qt.TX_RESP_Q020', 'qt.TX_RESP_Q021', 'qt.TX_RESP_Q022', 'qt.TX_RESP_Q023', 'qt.TX_RESP_Q024', 'qt.TX_RESP_Q025', 'qt.TX_RESP_Q026', 'qt.TX_RESP_Q027', 'qt.TX_RESP_Q028', 'qt.TX_RESP_Q029', 'qt.TX_RESP_Q030', 'qt.TX_RESP_Q031', 'qt.TX_RESP_Q032', 'qt.TX_RESP_Q033', 'qt.TX_RESP_Q034', 'qt.TX_RESP_Q035', 'qt.TX_RESP_Q036', 'qt.TX_RESP_Q037', 'qt.TX_RESP_Q038', 'qt.TX_RESP_Q039', 'qt.TX_RESP_Q040', 'qt.TX_RESP_Q041', 'qt.TX_RESP_Q042', 'qt.TX_RESP_Q043', 'qt.TX_RESP_Q044', 'qt.TX_RESP_Q045', 'qt.TX_RESP_Q046', 'qt.TX_RESP_Q047', 'qt.TX_RESP_Q048', 'qt.TX_RESP_Q049', 'qt.TX_RESP_Q050', 'qt.TX_RESP_Q051', 'qt.TX_RESP_Q052', 'qt.TX_RESP_Q053', 'qt.TX_RESP_Q054', 'qt.TX_RESP_Q055', 'qt.TX_RESP_Q056', 'qt.TX_RESP_Q057', 'qt.TX_RESP_Q058', 'qt.TX_RESP_Q059', 'qt.TX_RESP_Q060', 'qt.TX_RESP_Q061', 'qt.TX_RESP_Q062']

exclude_columns = ['IN_PREENCHIMENTO']
df_sb_3em = df_sb_3em.select(*cols_sb_3em)
df_sb_3em, reorder_columns = normalize_columns(df_sb_3em, file_type='3EM_ESC', org_type='SAEB', sheet='raw_trs_2011_TS_ALUNO_3EM', exclude_columns=exclude_columns)

include_columns = list(set(df_sb_3em.columns) - set(exclude_columns))
repeated_names = [
  ['rs.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_PROVA', 'Int'],
  ['qt.IN_PREENCHIMENTO', 'IN_PREENCHIMENTO_QUESTIONARIO', 'Int'],
]
alias_cols = [f.col(org).alias(dst).cast(_type) for org, dst, _type in repeated_names]
include_columns = include_columns + alias_cols

df_sb_3em = df_sb_3em.select(*include_columns)
df_sb_3em = df_sb_3em.select(*reorder_columns)

target = var_adls_uri + trs + table['trs_pb_2011_table']
df_sb_3em.coalesce(10).write.parquet(target, mode='append')

target

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## /trs/inep_saeb/prova_brasil_2011 > /trs/inep_saeb/PROVA_BRASIL_SAEB_UNIFICADO

# COMMAND ----------

cf.delete_files(dbutils, trs + table['trs_pb_unified_table'])

# COMMAND ----------

df_pb_2011 = spark.read.parquet(var_adls_uri + trs + table['trs_pb_2011_table'])
df_pb_2011 = normalize_columns(df_pb_2011, sheet='raw_trs_prova_brasil_2011')

target = var_adls_uri + trs + table['trs_pb_unified_table']
df_pb_2011.coalesce(10).write.partitionBy(table['partition_col']).parquet(target, mode='overwrite')

target

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## /raw/inep_saeb/SAEB_UNIFICADO > /trs/inep_saeb/PROVA_BRASIL_SAEB_UNIFICADO

# COMMAND ----------

df_raw_sb = spark.read.parquet(var_adls_uri + raw + table['raw_sb_table'])
df_raw_sb = normalize_columns(df_raw_sb, sheet='raw_trs_SAEB_UNIFICADO')

target = var_adls_uri + trs + table['trs_pb_unified_table']
df_raw_sb.coalesce(10).write.partitionBy(table['partition_col']).parquet(target, mode='append')

target