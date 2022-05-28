# Databricks notebook source
# DBTITLE 1,Widgets
import json
import datetime

var_pipelines_allowed = [
  "fechamento_suplemento_kpi",
  "fechamento_producao_senai",
  "fechamento_producao_sesi",
  "fechamento_unidades_atendimento",
  "fechamento_orcamento",
]
var_years_allowed = [str(y) for y in range(2000, datetime.datetime.now().year + 1)]
var_months_allowed = [str(m) for m in range(1, 13)]
var_days_allowed = [str(d) for d in range(1, 32)]

dbutils.widgets.dropdown('pipeline', var_pipelines_allowed[0], var_pipelines_allowed, 'Pipeline')
dbutils.widgets.dropdown('year', '2000', var_years_allowed, 'Ano')
dbutils.widgets.dropdown('month', '1', var_months_allowed , 'Mês')
dbutils.widgets.text('closing_date', 'yyyy-MM-dd', 'Data de Fechamento')

# COMMAND ----------

# DBTITLE 1,Parsing params and Executing
var_pipeline = dbutils.widgets.get('pipeline')
var_year = dbutils.widgets.get('year')
var_month =  dbutils.widgets.get('month')
var_closing_date = dbutils.widgets.get('closing_date')

var_closing_date_split = var_closing_date.split('-')
if len(var_closing_date_split) != 3:
  raise Exception("Erro no formato da Data de Fechamento informada: '{dt}'. Deve ser do tipo: 'yyyy-MM-dd'".format(dt=var_closing_date))

var_closing_year = str(int(var_closing_date_split[0]))  #This removes the leading 0s
if var_closing_year not in var_years_allowed:
  raise Exception("Ano informado '{year}' na Data de Fechamento não é permitido".format(year=var_closing_year))
  
var_closing_month = str(int(var_closing_date_split[1]))  #This removes the leading 0s
if var_closing_month not in var_months_allowed:
  raise Exception("Mês informado '{month}' na Data de Fechamento não é permitido".format(month=var_closing_month))

var_closing_day = str(int(var_closing_date_split[2]))
if var_closing_day not in var_days_allowed:
  raise Exception("Dia informado '{day}' na Data de Fechamento não é permitido".format(day=var_closing_day))
  
exec_params = {'pipeline': dbutils.widgets.get('pipeline'),
               'closing': json.dumps({
                 'year': var_year,
                 'month': var_month,
                 'dt_closing': '{y}-{m}-{d}'.format(y=var_closing_year, m=var_closing_month, d=var_closing_day)
               }), 
               'user': spark.sql("select current_user() as user").collect()[0]["user"],
              }

# COMMAND ----------

# DBTITLE 1,Enviando o fechamento para execução
dbutils.notebook.run("/KEYRUS/utils/lib_adf_closing/lib_adf_closing__impl", 120, exec_params)