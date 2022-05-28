# Databricks notebook source
# DBTITLE 1,Imports
from adf_closing.adf_closing import ADFClosing
import requests
import json

# COMMAND ----------

# DBTITLE 1,Fetch Widgets
widgets =  ['user', 'pipeline', 'closing']

params = {i: dbutils.widgets.get(i) for i in widgets}

# COMMAND ----------

# DBTITLE 1,Check closing parameters
var_user_parameters = {}
var_user_parameters['closing'] = json.loads(params['closing'])
var_closing_keys = ['year', 'month', 'dt_closing']
if "closing" not in var_user_parameters:
  raise Exception("'closing' not in expected keys")
if set(var_closing_keys).issubset(var_user_parameters['closing'].keys()):
  # will try parsing directly
  var_user_parameters['closing']['year'] = int(var_user_parameters['closing']['year'])
  var_user_parameters['closing']['month'] = int(var_user_parameters['closing']['month'])
else:
  raise Exception("Keys not found in closing dict: '{keys}'".format(keys=var_closing_keys))

# COMMAND ----------

# DBTITLE 1,Instantiate ADFClosing
# Prepare user_parameters for notifications
var_user_parameters['email'] = params['user']
var_user_parameters['from_databricks_wrapper']= 1

closing = ADFClosing(
  user=params['user'],
  adf_pipeline_name=params['pipeline'],
  adf_pipeline_params={'user_parameters': var_user_parameters, 
                       'env': {'env':'prod'}
                      },
)

# COMMAND ----------

# DBTITLE 1,Run closing
run_id = closing.run()

# COMMAND ----------

# DBTITLE 1,Return
dbutils.notebook.exit("Sua execução iniciou com sucesso! O id do processo é: '{id}'. Ao término do processamento, você receberá um e-mail informando o status".format(id=run_id))
