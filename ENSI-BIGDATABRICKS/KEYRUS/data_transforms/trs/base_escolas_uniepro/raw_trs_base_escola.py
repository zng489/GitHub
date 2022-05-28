# Databricks notebook source
import cni_connectors.adls_gen1_connector as connector
import json
import pyspark.sql.functions as f
import re

# COMMAND ----------

var_adls_uri = connector.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

table = json.loads(re.sub("\'", '\"', dbutils.widgets.get("tables")))
dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))

# COMMAND ----------

# table = {
#   'path_origin': 'usr/uniepro/base_escolas/',
#   'path_destination': 'mtd/corp/base_escolas',
# }

# dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs"}}
# dls = {"folders":{"landing":"/lnd","error":"/err","staging":"/stg","log":"/log","raw":"/raw","trusted":"/trs"}}

# adf = {
#   'adf_factory_name': 'cnibigdatafactory', 
#   'adf_pipeline_name': 'raw_trs_elegiveis_16_18',
#   'adf_pipeline_run_id': 'p1',
#   'adf_trigger_id': 't1',
#   'adf_trigger_name': 'author_dev',
#   'adf_trigger_time': '2020-06-16T17:57:06.0829994Z',
#   'adf_trigger_type': 'Manual'
# }

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']

# COMMAND ----------

source = "{adl_path}{raw}/{origin}".format(adl_path=var_adls_uri, raw=raw, origin=table["path_origin"])
source

# COMMAND ----------

target = "{adl_path}{trs}/{origin}".format(adl_path=var_adls_uri, trs=trs, origin=table["path_destination"])
target

# COMMAND ----------

df = spark.read.parquet(source)

# COMMAND ----------

df = df.withColumn('nm_unidade_atendimento_inep', f.when(f.col('no_entidade').isNotNull(), f.col('no_entidade'))
                                                  .otherwise(f.when(f.col('nome_unidade').isNotNull(), f.col('nome_unidade'))
                                                             .otherwise(f.when(f.col('nome_escola').isNotNull(), f.col('nome_escola'))
                                                                        .otherwise(f.when(f.col('nomeescolageo').isNotNull(), f.col('nomeescolageo'))
                                                                                   .otherwise(f.concat_ws(' ', f.lit('Não Informado'), f.col('co_entidade')))))))

df = df.withColumn('cd_dep_adm_escola_sesi_senai', f.when(f.col('escola_sesi') == f.lit(1), f.lit(11))
                                                   .otherwise(f.when(f.col('escola_senai') == f.lit(1), f.lit(12))
                                                               .otherwise(f.when((f.col('escola_sesi') == f.lit(1)) & (f.col('escola_senai') == f.lit(1)), f.lit(13))
                                                                          .otherwise(f.col('tp_dependencia')))))

# COMMAND ----------

na_columns = ['escola_senai', 'escola_sesi', 'escola_sesi_senai']
for na in na_columns:
  df = df.withColumn(na, f.when(f.col(na).isNull(), f.lit(0)).otherwise(f.col(na)))

# COMMAND ----------

select_alias = [('co_entidade', 'cd_entidade', 'Int'), ('no_entidade', 'nm_entidade', 'string'), ('nm_unidade_atendimento_inep', 'nm_unidade_atendimento_inep', 'String'), ('escola_senai', 'fl_escola_senai', 'Int'), ('escola_sesi', 'fl_escola_sesi', 'Int'), ('escola_sesi_senai', 'fl_escola_sesi_senai', 'Int'), ('cd_dep_adm_escola_sesi_senai', 'cd_dep_adm_escola_sesi_senai', 'Int'), ('nome_escola', 'nm_nome_escola', 'string'), ('nome_unidade', 'nm_nome_unidade', 'string'), ('bairro', 'nm_bairro', 'string'), ('base_fonte', 'nm_base_fonte', 'string'), ('cep', 'nr_cep', 'Int'), ('classif', 'nr_classif', 'Int'), ('co_distrito', 'cd_distrito', 'Int'), ('co_escola_sede_vinculada', 'cd_escola_sede_vinculada', 'Int'), ('co_ies_ofertante', 'cd_ies_ofertante', 'Int'), ('co_mesorregiao', 'cd_mesorregiao', 'Int'), ('co_microrregiao', 'cd_microrregiao', 'Int'), ('co_municipio', 'cd_municipio', 'Int'), ('co_orgao_regional', 'cd_orgao_regional', 'Int'), ('co_regiao', 'cd_regiao', 'Int'), ('co_uf', 'cd_uf', 'Int'), ('cod_oba', 'cd_oba', 'Int'), ('complemento', 'ds_complemento', 'string'), ('descricao', 'ds_descricao', 'string'), ('dt_ano_letivo_inicio', 'dt_ano_letivo_inicio', 'date'), ('dt_ano_letivo_termino', 'dt_ano_letivo_termino', 'date'), ('ende_comp', 'ds_ende_comp', 'string'), ('endereco', 'ds_endereco', 'string'), ('f03', 'nr_f03', 'Int'), ('fonte', 'ds_fonte', 'string'), ('in_conveniada_pp', 'fl_conveniada_pp', 'Int'), ('in_mant_escola_privada_emp', 'fl_mant_escola_privada_emp', 'Int'), ('in_mant_escola_privada_ong', 'fl_mant_escola_privada_ong', 'Int'), ('in_mant_escola_privada_s_fins', 'fl_mant_escola_privada_s_fins', 'Int'), ('in_mant_escola_privada_sind', 'fl_mant_escola_privada_sind', 'Int'), ('in_mant_escola_privada_sist_s', 'fl_mant_escola_privada_sist_s', 'Int'), ('lat', 'nr_lat', 'Int'), ('lon', 'nr_lon', 'Int'), ('municpgeo', 'nm_municpgeo', 'string'), ('nomedomunicipio', 'nm_nomedomunicipio', 'string'), ('nomeescolageo', 'nm_nomeescolageo', 'string'), ('nu_ano_censo', 'nr_ano_censo', 'Int'), ('num', 'nr_num', 'Int'), ('numeracao', 'ds_numeracao', 'string'), ('observ', 'ds_observ', 'string'), ('tp_categoria_escola_privada', 'tp_categoria_escola_privada', 'Int'), ('tp_convenio_poder_publico', 'tp_convenio_poder_publico', 'Int'), ('tp_dependencia', 'tp_dependencia', 'Int'), ('tp_localizacao', 'tp_localizacao', 'Int'), ('tp_regulamentacao', 'tp_regulamentacao', 'Int'), ('tp_situacao_funcionamento', 'tp_situacao_funcionamento', 'Int'), ('ufgeo', 'ds_ufgeo', 'string'), ('x', 'nr_x', 'Int'), ('y', 'nr_y', 'Int')]

select_alias = (f.col(org).cast(_type).alias(dst) for org, dst, _type in select_alias)
df = df.select(*select_alias)

# COMMAND ----------

df = df.withColumn('dh_insercao_trs', f.lit(adf["adf_trigger_time"].split(".")[0]).cast('timestamp'))

# COMMAND ----------

df.coalesce(1).write.parquet(target, mode='overwrite')