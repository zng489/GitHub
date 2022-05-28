# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn
var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")
dbutils.widgets.text("gov_table","source")
gov_table = dbutils.widgets.get("gov_table")
dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

tipo = 3
# 1 = para o curador    (campos do curador + tabelas com réplicas 1         + description null)
# 2 = para a governança (todos os campos   + tabelas com réplicas liberadas + description liberados)
# 3 = para a governança (todos os campos   + tabelas com réplicas 1         + description liberados)
# 4 = para o curador    (campos do curador + tabelas com réplicas 1         + description liberados)


# COMMAND ----------

filtro = '1 = 1'
filtro_table = filtro
filtro_field = filtro

drop_column_list = ['created_at','updated_at']

if tipo in [1, 4]:
  drop_column_list = drop_column_list + ['path','chave','col_version','tipo_carga','data_type','data_steward','login','is_derivative']
  
if gov_table == 'field':
  if tipo == 1:
    filtro_field = 'description is null'
  filtro_table = 'replica = 1'
  filtro = filtro_field                    
                                         
if gov_table == 'table':                                      
  if tipo != 2:         
    filtro_table = '(replica = 1) or (replica is null)'   
  filtro = filtro_table


# COMMAND ----------

gov_table, tipo, filtro, drop_column_list

# COMMAND ----------

if env == 'dev': default_dir = "/tmp/dev/raw/gov"
elif env == 'prod': default_dir = ""
path_gov = "{adl_path}{default_dir}/gov/tables/{gov_table}".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)
path_csv_gov = "{adl_path}{default_dir}/gov/csv_desc/{gov_table}/".format(adl_path=var_adls_uri,default_dir=default_dir,gov_table=gov_table)

# COMMAND ----------

df_csv = spark.read.format("delta")\
                   .load(path_gov)\
                   .filter(filtro)

# COMMAND ----------

# acha as tabelas 
if gov_table == 'field':
   path_table = path_gov.replace('field','table')
   df_table = spark.read.format("delta").load(path_table).filter(filtro_table).select(['source_name','schema_name','table_name'])
   cond = [df_csv.source_name == df_table.source_name, df_csv.schema_name == df_table.schema_name, df_csv.table_name == df_table.table_name]
   columns_to_drop = ['df_table.source_name','df_table.schema_name','df_table.table_name']
   df_field_table = df_csv.join(df_table, cond, 'inner').drop(df_table.source_name).drop(df_table.schema_name).drop(df_table.table_name)

   # colunas a desconsiderar
   columns_to_drop = ["nm_arq_in", "nr_reg", 
                      "dh_inicio_vigencia","dh_ultima_atualizacao_oltp","dh_insercao_trs","kv_process_control",\
                      "dh_insercao_raw","dh_inclusao_lancamento_evento","dh_atualizacao_entrada_oltp","dh_fim_vigencia",\
                      "dh_referencia","dh_atualizacao_saida_oltp","dh_insercao_biz","dh_arq_in","dh_exclusao_valor_pessoa",\
                      "dh_ultima_atualizacao_oltp","dh_fim_vigencia","dh_insercao_trs","dh_inicio_vigencia","dh_referencia",\
                      "dh_atualizacao_entrada_oltp","dh_atualizacao_saida_oltp","dh_inclusao_lancamento_evento","dh_atualizacao_oltp",\
                      "dh_inclusao_valor_pessoa","dh_primeira_atualizacao_oltp","D_E_L_E_T_","R_E_C_D_E_L_"]  
   
   df_field_table = df_field_table.filter(~df_field_table.field_name.isin(columns_to_drop))

   df_csv = df_field_table
    

# COMMAND ----------

df_csv.select([column for column in df_csv.columns if column not in drop_column_list])\
      .repartition(1)\
      .write\
      .format("csv")\
      .option("delimiter",";")\
      .option("header","true")\
      .option("encoding", "ISO-8859-1")\
      .mode("overwrite")\
      .save(path_csv_gov)

# COMMAND ----------

file_list = dbutils.fs.ls(path_csv_gov)

# COMMAND ----------

file_list = dbutils.fs.ls(path_csv_gov)
for fileinfo in file_list:
  if "csv" in fileinfo.name:
    file_path = fileinfo.path.split("/",1)[1].split("/",1)[1].split("/",1)[1]
    print(file_path)
    dbutils.notebook.exit(file_path)

# COMMAND ----------

