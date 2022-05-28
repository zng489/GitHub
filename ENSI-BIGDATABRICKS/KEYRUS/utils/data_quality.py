# Databricks notebook source
from cni_connectors import adls_gen1_connector as adls_conn

import uld_unigest.functions as uf
import crawler.functions as cf
import json
import re

# COMMAND ----------

var_adls_uri = adls_conn.adls_gen1_connect(spark, dbutils, scope="adls_gen1", dynamic_overwrite="dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPLEMENTATION

# COMMAND ----------

var_file = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file")))
var_dls = json.loads(re.sub("\'", '\"', dbutils.widgets.get("dls")))
var_adf = json.loads(re.sub("\'", '\"', dbutils.widgets.get("adf")))
var_file_parse = json.loads(re.sub("\'", '\"', dbutils.widgets.get("file_parse")))

# COMMAND ----------

#USE THIS ONLY FOR DEVELOPMENT PURPOSES
"""
var_file = {'namespace':'unigest','file_folder':'kpi_meta','extension':'CSV','column_delimiter':';','encoding':'UTF-8','null_value':''}

var_adf = {
  "adf_factory_name": "cnibigdatafactory",
  "adf_pipeline_name": "org_raw_kpi_meta",
  "adf_pipeline_run_id": "60ee3485-4a56-4ad1-99ae-666666666",
  "adf_trigger_id": "62bee9e9-acbb-49cc-80f2-666666666",
  "adf_trigger_name": "62bee9e9-acbb-49cc-80f2-66666666",
  "adf_trigger_time": "2020-11-19T01:42:41.5507749Z",
  "adf_trigger_type": "PipelineActivity"
}

var_dls = {"folders":{"landing": "/tmp/dev/uld",
                      "error": "/tmp/dev/err", 
                      "staging": "/tmp/dev/stg", 
                      "log": "/tmp/dev/log", 
                      "raw": "/tmp/dev/raw", 
                      "archive": "/tmp/dev/ach", 
                      "reject": "/tmp/dev/rjt"},
           "sub_folders":{"usr":"usr", 
                          "udl":"udl"}}

var_file_parse = {'file_path':'/prm/usr/unigest/CNI_BigData_KPIs_origens_planilhas_layouts_v02.6.xlsx', 
                  'headers':{'name_header':'campo','pos_header':'A','pos_org':'A','pos_dst':'A','pos_type':'B', 'pos_file_name_mask':'A', 'index_file_name_mask':0}
                 }
"""

# COMMAND ----------

lnd = var_dls['folders']['landing']
raw = var_dls['folders']['raw']

# COMMAND ----------

var_source = "{lnd}/{namespace}/{file_folder}/".format(lnd=lnd, namespace=var_file['namespace'], file_folder=var_file['file_folder'])
var_source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking if exist source

# COMMAND ----------

if not cf.directory_exists(dbutils, var_source):
  dbutils.notebook.exit({'list_files_to_reject': [], 'mgs': 'Path "%s" not exist or is empty' % var_source})
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality

# COMMAND ----------

parse_ba, file_name_mask = uf.parse_ba_doc(dbutils, var_adl_path=var_file_parse['file_path'], headers=var_file_parse['headers'], file_sheet=var_file['file_folder'], file_system='datalake', scope='adls_gen2')
parse_ba

# COMMAND ----------

if len(parse_ba)==0:
  list_files_to_reject = uf.list_adl_files_path(dbutils, adl_list_dir=var_source, file_system='datalake', scope='adls_gen2')
  file_quality_dict = {adl_file: ["file_folder_error"] for adl_file in list_files_to_reject}
  dbutils.notebook.exit(file_quality_dict)

# COMMAND ----------

extension_mask = "{}{}".format('.', file_name_mask.split('.')[-1])
name_complete_mask = file_name_mask.replace(extension_mask, '')
period_mask = name_complete_mask.split('_')[-1]
name_text_mask = name_complete_mask.replace("{}{}".format('_', period_mask), '')

print("file_name_mask: {},\nfile_format: {},\nfile_name_complete: {},\nfile_period:{},\nfile_name_text: {}".format(file_name_mask, extension_mask, name_complete_mask, period_mask, name_text_mask))

# COMMAND ----------

errors_adl_files_name_dict = uf.check_adl_files_name(dbutils=dbutils, var_adl_path=var_source, name_text_mask=name_text_mask, period_mask=period_mask, extension_mask=extension_mask, 
                                                  file_system='datalake', scope='adls_gen2')
errors_adl_files_name_dict

# COMMAND ----------

errors_adl_files_format_dict = uf.check_adl_files_format(dbutils=dbutils, var_adl_path=var_source, sep=var_file['column_delimiter'],
                                                     encoding=var_file['encoding'].lower(), parse_ba=parse_ba, sheet=var_file['file_folder'], file_system='datalake', scope = 'adls_gen2')
errors_adl_files_format_dict

# COMMAND ----------

for k in errors_adl_files_name_dict.keys():
  errors_adl_files_name_dict[k].extend(errors_adl_files_format_dict[k])    

# COMMAND ----------

data_quality_dict = {'list_files_to_reject': [k for k, v in errors_adl_files_name_dict.items() if v], 'errors': {k: v for k, v in errors_adl_files_name_dict.items() if v}}

# COMMAND ----------

dbutils.notebook.exit(data_quality_dict)

# COMMAND ----------

