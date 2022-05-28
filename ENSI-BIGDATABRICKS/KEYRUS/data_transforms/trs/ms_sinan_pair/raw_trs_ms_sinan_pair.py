# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen2", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC # Trusted specific parameter section

# COMMAND ----------

import re
import json
import datetime

import crawler.functions as cf
import pyspark.sql.functions as f

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

raw_path = f'{raw}/crw/{table["schema"]}/{table["table"]}'
trs_path = f'{trs}/{table["schema"]}/{table["table"]}'

adl_path = var_adls_uri
adl_raw = f"{adl_path}{raw_path}"
adl_trs = f"{adl_path}{trs_path}"

# COMMAND ----------

prm = dls['folders']['prm']
prm_file = table["prm_path"].split('/')[-1]
prm_path = f'{prm}/usr/{table["schema"]}_{table["table"]}/{prm_file}'

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'F'}
var_prm_dict = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply transformations and save dataframe

# COMMAND ----------

def __transform_columns(sheet_name):
  for org, dst, _type in var_prm_dict[sheet_name]:
    if org == 'N/A':
      yield f.lit(None).cast(_type).alias(dst)
    else:
      yield f.col(org).cast(_type).alias(dst)

# COMMAND ----------

df = spark.read.parquet(adl_raw)

sheet_name = table["table"].upper()
cf.check_ba_doc(df, parse_ba=var_prm_dict, sheet=sheet_name)
df = df.select(*__transform_columns(sheet_name))

# COMMAND ----------

df = df.withColumn('DS_IDADE_N', f.when(
                                        (f.length(f.col('CD_IDADE_N'))==4)&(f.substring(f.col('CD_IDADE_N'), 0, 1)==4),
                                        f.substring(f.col('CD_IDADE_N'), 2, 4)).otherwise(f.lit('< 1 ano')))

data = [
        {'CD_SEXO': 'M', 'DS_SEXO': 'Masculino'},
        {'CD_SEXO': 'F', 'DS_SEXO': 'Feminino'},
        {'CD_SEXO': 'I', 'DS_SEXO': 'Ignorado'}
]
_df = spark.createDataFrame(data)
df = df.join(_df, on='CD_SEXO', how='left').drop(df.DS_SEXO)

# COMMAND ----------

headers = {'name_header':'Campo Origem','pos_header':'C','pos_org':'C','pos_dst':'E','pos_type':'D'}
var_prm_dict_desc = cf.parse_ba_doc(dbutils, prm_path, headers=headers)

map_list = [row for row in var_prm_dict_desc[sheet_name] if row[2] != 'Mapeamento direto']

# COMMAND ----------

remove_list = ['DS_IDADE_N','DS_SEXO']

for item in map_list:
  column = item[1]
  if column in remove_list:
    continue

  map_ = re.sub('^se ','\"Cod_', item[2])
  map_ = re.sub('\nse ','\n\"Cod_', map_)
  map_ = (map_.replace(' então ',', \"')
              .replace(' =','\":')
              .replace(';\n','\n')
  )
  map_ = re.sub('"(Cod_\w+)"',f'"Cod_{column}"', map_).split('\n')

  try:
    data = [json.loads('{'+re.sub(';$|,$|\.$','',i.strip())+'}') for i in map_]
    _df = spark.createDataFrame(data)
    df = (df.join(_df, df[column]==_df['Cod_'+column], how='left')
            .drop(df[column])
            .drop(_df['Cod_'+column])
         )
  except Exception as e:
    raise Exception(f'Erro: {e};\nmap: {map_}')

# COMMAND ----------

columns = [name[1] for name in var_prm_dict[sheet_name]]
df = df.select(columns)

dh_insercao_trs = datetime.datetime.now()
df = df.withColumn('dh_insercao_trs', f.lit(dh_insercao_trs).cast('timestamp'))

df.write.mode('overwrite').partitionBy('NR_ANO').parquet(path=adl_trs)
