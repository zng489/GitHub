# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # About trusted area objects:
# MAGIC - these notebooks are very specific to each of the tasks they are performing
# MAGIC - maybe you don't need to use parameters
# MAGIC - don't worry if you're not generalizing enough

# COMMAND ----------

# MAGIC %md
# MAGIC This object is type truncate/full insert
# MAGIC 
# MAGIC Processo raw_trs_etapa_ensino
# MAGIC Tabela/Arquivo Origem /raw/crw/inep_matricula/
# MAGIC Tabela/Arquivo Destino /trs/uniepro/matricula/
# MAGIC Particionamento Tabela/Arquivo Destino Não há
# MAGIC Descrição Tabela/Arquivo Destino etapa de ensino forma uma estrutura de códigos que visa orientar, organizar e consolidar 
# MAGIC Tipo Atualização A (Substituição incremental da tabela: Append)
# MAGIC Periodicidade/Horario Execução Anual, após carga raw 

# COMMAND ----------

# MAGIC %md
# MAGIC ADLS DATA  ACCESS

# COMMAND ----------

from cni_connectors import adls_gen1_connector as adls_conn
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import concat, col, lit

import re
import json

# COMMAND ----------

# MAGIC %md
# MAGIC Common variable section. Declare useful variables here

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

tables = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# USE THIS ONLY FOR DEVELOPMENT PURPOSES
#tables =  {
#   "path_origin": "crw/inep_censo_escolar/gestor/",
#   "path_destination": "uniepro/censo_escolar_gestor"
   
# }

#dls = {"folders":#{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}

#adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_gestor',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-05-26T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=tables["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=tables["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

# COMMAND ----------

transformation = [('NU_ANO_CENSO', 'NR_ANO_CENSO', 'Int'), ('ID_GESTOR', 'ID_GESTOR', 'String'),('NU_MES', 'NR_MES', 'Int'), ('NU_ANO', 'NR_ANO', 'Int'), ('NU_IDADE_REFERENCIA', 'NR_IDADE_REFERENCIA', 'Int'), ('NU_IDADE', 'NR_IDADE', 'Int'), ('TP_SEXO', 'TP_SEXO', 'Int'), ('TP_COR_RACA', 'TP_COR_RACA', 'Int'), ('TP_NACIONALIDADE', 'TP_NACIONALIDADE', 'Int'), ('CO_PAIS_ORIGEM', 'CD_PAIS_ORIGEM', 'Int'), ('CO_UF_NASC', 'CD_UF_NASC', 'Int'), ('CO_MUNICIPIO_NASC', 'CD_MUNICIPIO_NASC', 'Int'), ('IN_NECESSIDADE_ESPECIAL', 'FL_NECESSIDADE_ESPECIAL', 'Int'), ('IN_BAIXA_VISAO', 'FL_BAIXA_VISAO', 'Int'), ('IN_CEGUEIRA', 'FL_CEGUEIRA', 'Int'), ('IN_DEF_AUDITIVA', 'FL_DEF_AUDITIVA', 'Int'), ('IN_DEF_FISICA', 'FL_DEF_FISICA', 'Int'), ('IN_DEF_INTELECTUAL', 'FL_DEF_INTELECTUAL', 'Int'), ('IN_SURDEZ', 'FL_SURDEZ', 'Int'), ('IN_SURDOCEGUEIRA', 'FL_SURDOCEGUEIRA', 'Int'), ('IN_DEF_MULTIPLA', 'FL_DEF_MULTIPLA', 'Int'), ('IN_AUTISMO', 'FL_AUTISMO', 'Int'), ('IN_SUPERDOTACAO', 'FL_SUPERDOTACAO', 'Int'), ('TP_ESCOLARIDADE', 'TP_ESCOLARIDADE', 'Int'), ('TP_ENSINO_MEDIO', 'TP_ENSINO_MEDIO', 'Int'), ('CO_AREA_CURSO_1', 'CD_AREA_CURSO_1', 'String'), ('CO_CURSO_1', 'CD_CURSO_1', 'String'), ('IN_LICENCIATURA_1', 'FL_LICENCIATURA_1', 'Int'), ('NU_ANO_CONCLUSAO_1', 'NR_ANO_CONCLUSAO_1', 'Int'), ('TP_TIPO_IES_1', 'TP_TIPO_IES_1', 'Int'), ('CO_IES_1', 'CD_IES_1', 'Int'), ('CO_AREA_CURSO_2', 'CD_AREA_CURSO_2', 'String'), ('CO_CURSO_2', 'CD_CURSO_2', 'String'), ('IN_LICENCIATURA_2', 'FL_LICENCIATURA_2', 'Int'), ('NU_ANO_CONCLUSAO_2', 'NR_ANO_CONCLUSAO_2', 'Int'), ('TP_TIPO_IES_2', 'TP_TIPO_IES_2', 'Int'), ('CO_IES_2', 'CD_IES_2', 'Int'), ('IN_ESPECIALIZACAO', 'FL_ESPECIALIZACAO', 'Int'), ('IN_MESTRADO', 'FL_MESTRADO', 'Int'), ('IN_DOUTORADO', 'FL_DOUTORADO', 'Int'), ('IN_POS_NENHUM', 'FL_POS_NENHUM', 'Int'), ('IN_ESPECIFICO_CRECHE', 'FL_ESPECIFICO_CRECHE', 'Int'), ('IN_ESPECIFICO_PRE_ESCOLA', 'FL_ESPECIFICO_PRE_ESCOLA', 'Int'), ('IN_ESPECIFICO_ANOS_INICIAIS', 'FL_ESPECIFICO_ANOS_INICIAIS', 'Int'), ('IN_ESPECIFICO_ANOS_FINAIS', 'FL_ESPECIFICO_ANOS_FINAIS', 'Int'), ('IN_ESPECIFICO_ENS_MEDIO', 'FL_ESPECIFICO_ENS_MEDIO', 'Int'), ('IN_ESPECIFICO_EJA', 'FL_ESPECIFICO_EJA', 'Int'), ('IN_ESPECIFICO_ED_ESPECIAL', 'FL_ESPECIFICO_ED_ESPECIAL', 'Int'), ('IN_ESPECIFICO_ED_INDIGENA', 'FL_ESPECIFICO_ED_INDIGENA', 'Int'), ('IN_ESPECIFICO_CAMPO', 'FL_ESPECIFICO_CAMPO', 'Int'), ('IN_ESPECIFICO_AMBIENTAL', 'FL_ESPECIFICO_AMBIENTAL', 'Int'), ('IN_ESPECIFICO_DIR_HUMANOS', 'FL_ESPECIFICO_DIR_HUMANOS', 'Int'), ('IN_ESPECIFICO_DIV_SEXUAL', 'FL_ESPECIFICO_DIV_SEXUAL', 'Int'), ('IN_ESPECIFICO_DIR_ADOLESC', 'FL_ESPECIFICO_DIR_ADOLESC', 'Int'), ('IN_ESPECIFICO_AFRO', 'FL_ESPECIFICO_AFRO', 'Int'), ('IN_ESPECIFICO_GESTAO', 'FL_ESPECIFICO_GESTAO', 'Int'), ('IN_ESPECIFICO_OUTROS', 'FL_ESPECIFICO_OUTROS', 'Int'), ('IN_ESPECIFICO_NENHUM', 'FL_ESPECIFICO_NENHUM', 'Int'), ('TP_CARGO_GESTOR', 'TP_CARGO_GESTOR', 'Int'), ('TP_TIPO_ACESSO_CARGO', 'TP_TIPO_ACESSO_CARGO', 'Int'), ('TP_TIPO_CONTRATACAO', 'TP_TIPO_CONTRATACAO', 'Int'), ('CO_ENTIDADE', 'CD_ENTIDADE', 'Int'), ('CO_REGIAO', 'CD_REGIAO', 'Int'), ('CO_MESORREGIAO', 'CD_MESORREGIAO', 'Int'), ('CO_MICRORREGIAO', 'CD_MICRORREGIAO', 'Int'), ('CO_UF', 'CD_UF', 'Int'), ('CO_MUNICIPIO', 'CD_MUNICIPIO', 'Int'), ('CO_DISTRITO', 'CD_DISTRITO', 'Int'), ('TP_DEPENDENCIA', 'TP_DEPENDENCIA', 'Int'), ('TP_LOCALIZACAO', 'TP_LOCALIZACAO', 'Int'), ('TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_CATEGORIA_ESCOLA_PRIVADA', 'Int'), ('IN_CONVENIADA_PP', 'FL_CONVENIADA_PP', 'Int'), ('TP_CONVENIO_PODER_PUBLICO', 'TP_CONVENIO_PODER_PUBLICO', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_EMP', 'FL_MANT_ESCOLA_PRIVADA_EMP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_ONG', 'FL_MANT_ESCOLA_PRIVADA_ONG', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_OSCIP', 'FL_MANT_ESCOLA_PRIVADA_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIV_ONG_OSCIP', 'FL_MANT_ESCOLA_PRIV_ONG_OSCIP', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIND', 'FL_MANT_ESCOLA_PRIVADA_SIND', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_SIST_S', 'FL_MANT_ESCOLA_PRIVADA_SIST_S', 'Int'), ('IN_MANT_ESCOLA_PRIVADA_S_FINS', 'FL_MANT_ESCOLA_PRIVADA_S_FINS', 'Int'), ('TP_REGULAMENTACAO', 'TP_REGULAMENTACAO', 'Int'), ('TP_LOCALIZACAO_DIFERENCIADA', 'TP_LOCALIZACAO_DIFERENCIADA', 'Int'), ('IN_EDUCACAO_INDIGENA', 'FL_EDUCACAO_INDIGENA', 'Int')]

# COMMAND ----------

for column, renamed, _type in transformation:
  df = df.withColumn(column, col(column).cast(_type))
  df = df.withColumnRenamed(column, renamed)

# COMMAND ----------

# Command to insert a field for data control.
df = df.withColumn('dh_insercao_trs', lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing in target

# COMMAND ----------

df.write.partitionBy('NR_ANO_CENSO').save(path=target, format="parquet", mode='overwrite')