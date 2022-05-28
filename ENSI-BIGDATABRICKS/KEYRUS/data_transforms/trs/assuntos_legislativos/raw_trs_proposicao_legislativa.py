# Databricks notebook source
 import pyspark.sql.functions as f 

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC select 
# MAGIC     pl.cod_proposicao, 
# MAGIC     pl.tip_casa_origem, 
# MAGIC     pl.cod_pl_congresso, 
# MAGIC     pl.cod_pl_congresso_apensante, 
# MAGIC     prior pl.cod_pl_congresso, 
# MAGIC     SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), 
# MAGIC     instr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), '/', -1) as posicao, 
# MAGIC     substr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), 2, instr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), '/', 2) - 2) as subs, 
# MAGIC     SYS_CONNECT_BY_PATH(cod_pl_congresso, '/') subsfull, 
# MAGIC     level 
# MAGIC   from 
# MAGIC     robo_legisdata.proposicao_legislativa pl 
# MAGIC     where level > 1 
# MAGIC   start with 
# MAGIC     pl.cod_pl_congresso_apensante is null 
# MAGIC   connect by NOCYCLE 
# MAGIC     prior pl.cod_pl_congresso = pl.cod_pl_congresso_apensante 
# MAGIC     and prior pl.tip_casa_origem = pl.tip_casa_origem 
# MAGIC     
# MAGIC   </pre>
# MAGIC   

# COMMAND ----------

var_adls_uri = "adl://cnibigdatadls.azuredatalakestore.net"
var_schema_prefix = "/tmp/dev/raw/bdo/robo_legisdata/"

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC     select 
# MAGIC     pl.cod_proposicao, 
# MAGIC     pl.tip_casa_origem, 
# MAGIC     pl.cod_pl_congresso, 
# MAGIC     pl.cod_pl_congresso_apensante, 
# MAGIC     prior pl.cod_pl_congresso, 
# MAGIC     SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), 
# MAGIC     instr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), '/', -1) as posicao, 
# MAGIC     substr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), 2, instr(SYS_CONNECT_BY_PATH(cod_pl_congresso, '/'), '/', 2) - 2) as subs, 
# MAGIC     SYS_CONNECT_BY_PATH(cod_pl_congresso, '/') subsfull, 
# MAGIC     level 
# MAGIC   from 
# MAGIC     robo_legisdata.proposicao_legislativa pl 
# MAGIC     where level > 1 
# MAGIC   start with 
# MAGIC     pl.cod_pl_congresso_apensante is null 
# MAGIC   connect by NOCYCLE 
# MAGIC     prior pl.cod_pl_congresso = pl.cod_pl_congresso_apensante 
# MAGIC     and prior pl.tip_casa_origem = pl.tip_casa_origem
# MAGIC   </pre>

# COMMAND ----------

var_path_pp = var_adls_uri + var_schema_prefix + "vw_proposicao_principal_bdata"
pp = spark.read.parquet(var_path_pp)

# COMMAND ----------

#pp.rdd.getNumPartitions()

# COMMAND ----------

#pp.count()

# COMMAND ----------

#display(pp.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <pre>
# MAGIC select
# MAGIC     vp.cod_proposicao_vinculada as cod_proposicao,
# MAGIC     rp.cod_resultado_proposicao,
# MAGIC     cf.cod_complemento_forma,
# MAGIC     cf.nom_complemento_forma,
# MAGIC     tpr.cod_tipo_resultado,
# MAGIC     tpr.des_tipo_resultado
# MAGIC   from
# MAGIC     robo_legisdata.resultado_proposicao rp
# MAGIC   inner join
# MAGIC     robo_legisdata.vw_vinculo_proposicao vp
# MAGIC     on vp.cod_proposicao_vinculante = rp.cod_proposicao
# MAGIC   left join
# MAGIC     robo_legisdata.tipo_resultado_proposicao tpr
# MAGIC     on tpr.cod_tipo_resultado = rp.cod_tipo_resultado
# MAGIC   left join
# MAGIC     robo_legisdata.complemento_forma cf
# MAGIC     on cf.cod_complemento_forma = rp.compl_forma
# MAGIC     
# MAGIC </pre>

# COMMAND ----------

var_path_rp = var_adls_uri + var_schema_prefix + "resultado_proposicao"
rp_columns  = ["cod_proposicao", "cod_tipo_resultado", "compl_forma", "cod_resultado_proposicao"]
rp = spark.read.parquet(var_path_rp).select(*rp_columns)

# COMMAND ----------

#rp.rdd.getNumPartitions()

# COMMAND ----------

# victor -> vw_vinculo_proposicao 

var_path_vp = var_adls_uri + var_schema_prefix + "vw_vinculo_proposicao"
vp_columns  = ["cod_proposicao_vinculante","cod_proposicao_vinculada"]
vp = spark.read.parquet(var_path_vp).select(*vp_columns)

# COMMAND ----------

#vp.rdd.getNumPartitions()

# COMMAND ----------

# leo -> tipo_resultado_proposicao

var_path_trp = var_adls_uri + var_schema_prefix + "tipo_resultado_proposicao"
trp_columns  = ["cod_tipo_resultado", "des_tipo_resultado"]
trp = spark.read.parquet(var_path_trp).select(*trp_columns)

# COMMAND ----------

#trp.rdd.getNumPartitions()

# COMMAND ----------

#trp.count()

# COMMAND ----------

# Coisas do victor
dbutils.fs.ls(var_adls_uri + var_schema_prefix)

# COMMAND ----------

# thiago -> complemento_forma

var_path_cf = var_adls_uri + var_schema_prefix + "complemento_forma"
cf_columns  = ["cod_complemento_forma", "nom_complemento_forma"]
cf = spark.read.parquet(var_path_cf).select(*cf_columns)

# COMMAND ----------

#cf.rdd.getNumPartitions()

# COMMAND ----------

pt2 = rp. \
join(vp, rp["cod_proposicao"] == vp["cod_proposicao_vinculante"], "inner")

# COMMAND ----------

# Notação de join para colunas que possuem o mesmo nome para evitar a duplicidade, caso eja necessário manter apenas 1 delas
pt2_1 = pt2.join(trp, ["cod_tipo_resultado"] , "left")

# COMMAND ----------

pt2_2 = pt2_1.join(cf, cf["cod_complemento_forma"] == rp["compl_forma"],"left")

# COMMAND ----------

#pt2_2.rdd.getNumPartitions()

# COMMAND ----------

#pt2_2.write.save(var_adls_uri + "/tmp/thomaz/join1", format="parquet", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC select 
# MAGIC         tipo_posicao.nom_tipo_posicao 
# MAGIC       from 
# MAGIC         robo_legisdata.tipo_posicao tipo_posicao 
# MAGIC       inner join 
# MAGIC         robo_legisdata.posicao posicao 
# MAGIC         on posicao.cod_tipo_posicao = tipo_posicao.cod_tipo_posicao 
# MAGIC       where 
# MAGIC         posicao.cod_posicao = ( 
# MAGIC         select 
# MAGIC           max(p.cod_posicao) 
# MAGIC         from 
# MAGIC           robo_legisdata.posicao p 
# MAGIC         where 
# MAGIC           p.cod_proposicao in (select vp.cod_proposicao_vinculada from robo_legisdata.vw_vinculo_proposicao vp where vp.cod_proposicao_vinculante = t1."Código da proposição") 
# MAGIC           and p.ind_habilitado = 1 
# MAGIC           and p.dat_posicao = ( 
# MAGIC             select 
# MAGIC               max(pmax.dat_posicao) 
# MAGIC             from 
# MAGIC               robo_legisdata.posicao pmax 
# MAGIC             where 
# MAGIC               pmax.ind_habilitado = 1 
# MAGIC               and pmax.cod_proposicao in (select vp.cod_proposicao_vinculada from robo_legisdata.vw_vinculo_proposicao vp where vp.cod_proposicao_vinculante = t1."Código da proposição") 
# MAGIC           ) 
# MAGIC         )
# MAGIC </pre>

# COMMAND ----------

var_path_pl = var_adls_uri + var_schema_prefix + "proposicao_legislativa"
var_path_pl

# COMMAND ----------

prop_legs = spark.read.parquet(var_path_pl)

# COMMAND ----------

var_pl1_columns = ["cod_proposicao", "cod_pl_congresso", "num_proposicao", "ano_proposicao", "txt_regime_tramitacao", "des_foco_projeto", "tip_casa_origem", "cod_autor", "tip", "num_total_prioridade"]
pl1 = prop_legs.filter(f.col("sit_acompanhamento") == 1).select(*var_pl1_columns)

# COMMAND ----------

var_path_tp = var_adls_uri + var_schema_prefix + "tipo_proposicao"
var_path_tp

# COMMAND ----------

var_tp_columns = ["cod_tip_proposicao", "tip_proposicao"] 
tp = spark.read.parquet(var_path_tp).select(*var_tp_columns)

# COMMAND ----------

var_path_tco = var_adls_uri + var_schema_prefix + "tipo_casa_origem"
var_path_tco

# COMMAND ----------

var_tco_columns = ["cod_tipo_casa_origem", "des_casa_origem", "sig_casa_origem"]
tco = spark.read.parquet(var_path_tco).select(*var_tco_columns)

# COMMAND ----------

var_path_autor = var_adls_uri + var_schema_prefix + "autor"
var_path_autor

# COMMAND ----------

var_a_columns = ["cod_autor", "nom_autor", "nom_partido", "uf"]
a = spark.read.parquet(var_path_autor).select(*var_a_columns)

# COMMAND ----------

var_path_ep = var_adls_uri + var_schema_prefix + "escala_prioridade"
var_path_ep

# COMMAND ----------

var_ep_columns = ["nom_escala_prioridade", "num_prioridade_inferior", "num_prioridade_superior"]
ep = spark.read.parquet(var_path_ep).select(*var_ep_columns)

# COMMAND ----------

var_path_pd = var_adls_uri + var_schema_prefix + "proposicao_divisao"
var_path_pd

# COMMAND ----------

var_pd_columns = ["cod_proposicao", "cod_divisao"]
pd = spark.read.parquet(var_path_pd).select(*var_pd_columns)

# COMMAND ----------

var_path_dv = var_adls_uri + var_schema_prefix + "divisao"
var_path_dv

# COMMAND ----------

var_dv_columns = ["nom_divisao", "cod_divisao"]
dv = spark.read.parquet(var_path_dv).select(*var_dv_columns)

# COMMAND ----------

var_path_cp = var_adls_uri + var_schema_prefix + "classificacao_proposicao"
var_cp_columns = ["cod_classificacao", "cod_proposicao"]
cp = spark.read.parquet(var_path_cp).select(*var_cp_columns)

# COMMAND ----------

var_path_c = var_adls_uri + var_schema_prefix + "classificacao"
var_c_columns = ["cod_classificacao", "nom_classificacao"]
c = spark.read.parquet(var_path_c).select(*var_c_columns)

# COMMAND ----------

var_path_nc = var_adls_uri + var_schema_prefix + "nivel_classificacao"
var_path_nc

# COMMAND ----------

var_nc_columns = ["cod_classificacao", "cod_nivel"]
nc = spark.read.parquet(var_path_nc).select(*var_nc_columns)

# COMMAND ----------

t1_0 = pl1.join(tp, tp["cod_tip_proposicao"]==pl1["tip"], "inner") 

# COMMAND ----------

t1_1 = t1_0.join(tco, tco["cod_tipo_casa_origem"]==t1_0["tip_casa_origem"], "inner") 

# COMMAND ----------

t1_2 = t1_1.join(a, ["cod_autor"], "left")

# COMMAND ----------

t1_3 = t1_2.join(ep, (pl1["num_total_prioridade"].isNull() & ep["num_prioridade_inferior"].isNull()) | (pl1["num_total_prioridade"].between(ep["num_prioridade_inferior"], ep["num_prioridade_superior"])) , "left")   

# COMMAND ----------

#display(t1_3.limit(10))

# COMMAND ----------

t1_4 = t1_3.join(pd, ["cod_proposicao"], "left")

# COMMAND ----------

t1_5 = t1_4.join(dv, ["cod_divisao"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC select
# MAGIC           listagg(c.nom_classificacao, ';') within group (order by c.nom_classificacao nulls last)
# MAGIC         from robo_legisdata.classificacao c 
# MAGIC         inner join robo_legisdata.classificacao_proposicao cp 
# MAGIC         on cp.cod_classificacao = c.cod_classificacao 
# MAGIC         inner join robo_legisdata.nivel_classificacao nc 
# MAGIC         on nc.cod_classificacao = c.cod_classificacao 
# MAGIC         where 
# MAGIC         nc.cod_nivel = 9 
# MAGIC         and cp.cod_proposicao = pl.cod_proposicao 
# MAGIC       ) as "Interesse"
# MAGIC </pre>

# COMMAND ----------

c1 = c.join(cp, ["cod_classificacao"], "inner")
interesse = c1.join(nc, ["cod_classificacao"], "inner").filter(f.col("cod_nivel") == 9)

# COMMAND ----------

interesse_agg = interesse.groupBy('cod_proposicao').agg(f.collect_list('nom_classificacao').alias('nom_classificacao_interesse'))

# COMMAND ----------

tema = c1.join(nc, ["cod_classificacao"], "inner").filter(f.col("cod_nivel") == 5)

# COMMAND ----------

tema_agg = tema.groupBy('cod_proposicao').agg(f.collect_list('nom_classificacao').alias('nom_classificacao_tema'))
                                   #.withColumn('nom_classificacao', f.concat_ws(f.split(f.col('nom_classificacao'))))

# COMMAND ----------

subtema = c1.join(nc, ["cod_classificacao"], "inner").filter(f.col("cod_nivel") == 6)

# COMMAND ----------

subtema_agg = subtema.groupBy('cod_proposicao').agg(f.collect_list('nom_classificacao').alias('nom_classificacao_subtema'))

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC select
# MAGIC           listagg(tco_c.sig_casa_origem||'-'||coalesce(c.sig_comissao, c.nom_comissao, ';')) within group (order by dc.dat_localizacao nulls last)
# MAGIC         from
# MAGIC           robo_legisdata.comissao c
# MAGIC         inner join
# MAGIC           robo_legisdata.tipo_casa_origem tco_c
# MAGIC           on tco_c.cod_tipo_casa_origem = c.tip_casa_origem
# MAGIC         inner join
# MAGIC           robo_legisdata.distribuicao_comissao dc
# MAGIC           on dc.cod_comissao = c.cod_comissao
# MAGIC         where
# MAGIC           dc.ind_loc_atual = 1
# MAGIC           and exists (
# MAGIC             select 1 from robo_legisdata.distribuicao d
# MAGIC             inner join robo_legisdata.vw_vinculo_proposicao vp
# MAGIC             on vp.cod_proposicao_vinculada = d.cod_proposicao
# MAGIC             where dc.cod_distribuicao = d.cod_distribuicao
# MAGIC             and vp.cod_proposicao_vinculante = pl.cod_proposicao
# MAGIC           )
# MAGIC </pre>

# COMMAND ----------

var_path_comissao = var_adls_uri + var_schema_prefix + "comissao"
var_comissao_columns = ["cod_comissao","nom_comissao", "sig_comissao", "tip_casa_origem"]
comissao = spark.read.parquet(var_path_comissao).select(*var_comissao_columns)

# COMMAND ----------

loc1 = comissao.join(tco, tco["cod_tipo_casa_origem"] == comissao["tip_casa_origem"], "inner")

# COMMAND ----------

var_path_dc = var_adls_uri + var_schema_prefix + "distribuicao_comissao"
var_dc_columns = ["ind_loc_atual","cod_comissao", "cod_distribuicao", "dat_localizacao"]
dc = spark.read.parquet(var_path_dc).select(*var_dc_columns)


# COMMAND ----------

loc2 = loc1.join(dc, ["cod_comissao"], "inner").filter(f.col("ind_loc_atual") == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC select 1 from robo_legisdata.distribuicao d
# MAGIC             inner join robo_legisdata.vw_vinculo_proposicao vp
# MAGIC             on vp.cod_proposicao_vinculada = d.cod_proposicao
# MAGIC             where dc.cod_distribuicao = d.cod_distribuicao
# MAGIC             and vp.cod_proposicao_vinculante = pl.cod_proposicao
# MAGIC </pre>

# COMMAND ----------

var_path_d = var_adls_uri + var_schema_prefix + "distribuicao"
var_d_columns = ["cod_proposicao","cod_distribuicao"]
d = spark.read.parquet(var_path_d).select(*var_d_columns)

# COMMAND ----------

distribuicao = d.join(vp, d["cod_proposicao"]==vp["cod_proposicao_vinculada"], "inner")

# COMMAND ----------

localizacao_atual = loc2.join(distribuicao, (loc2["cod_distribuicao"] == distribuicao["cod_distribuicao"]) , "inner") #& (distribuicao["cod_proposicao_vinculante"] == loc2["cod_proposicao"])

# COMMAND ----------

localizacao_atual_agg = localizacao_atual.groupBy('cod_proposicao_vinculante').agg(f.collect_list(f.concat(f.col('sig_casa_origem'), f.lit(" "), f.col('sig_comissao'))).alias('nom_classificacao_loc_atual'))

# COMMAND ----------

t1 = t1_5.join(interesse_agg, ["cod_proposicao"], "left") \
      .join(tema_agg, ["cod_proposicao"], "left") \
      .join(subtema_agg, ["cod_proposicao"], "left") \
      .join(localizacao_atual_agg, t1_5["cod_proposicao"] == localizacao_atual_agg["cod_proposicao_vinculante"], "left")

# COMMAND ----------

#t1.count()

# COMMAND ----------

# tabela t1 do script
#display(t1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Para acessar os dados com as ferramentas externas de visualização (Tableau incluso), o objeto deve ser exposto via JDBC, o que, em outras palavras, implica em ser salvo como uma tabela geranciada pelo HiveMetastore. Apenas salvar no ADLS não será suficiente, mas há uma possibilidade de caminho misto entre os dois.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cenário 1 
# MAGIC Salvar do DBFS como uma tabela gerenciada.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --CREATE DATABASE STI_INTELIGENCIA;

# COMMAND ----------

t1.coalesce(2).write.saveAsTable("sti_inteligencia.proposicao__dbfs", format="parquet", mode="overwrite", partitionBy=["ano_proposicao"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cenário 2
# MAGIC 
# MAGIC salvar como external table utilizando a notação:
# MAGIC 
# MAGIC CREATE TABLE --DB--.--TABLE-- USING PARQUET LOCATION --ADLS PATH--
# MAGIC 
# MAGIC APARENTEMENTE NÃO FUNCIONA PARA O TABLEAU

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cenário 3
# MAGIC ter o objeto do cenário 1 no dbfs e também no adls

# COMMAND ----------

t1.coalesce(2).write.save(<path do adls>, format="parquet", mode="overwrite", partitionBy=["ano_proposicao"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cenário 4
# MAGIC 
# MAGIC Salvar no SQLDW
# MAGIC No momento proponho que a keyrus desenvolva um encapsulamento como biblioteca

# COMMAND ----------

