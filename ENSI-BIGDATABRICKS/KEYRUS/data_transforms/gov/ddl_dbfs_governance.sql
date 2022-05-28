-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS governance_dbfs_dev;
-- MAGIC CREATE DATABASE IF NOT EXISTS governance_dbfs_prd;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.source;
-- MAGIC --Table Catalog DEV - source
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.source 
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  description STRING, 
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/source";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.source;
-- MAGIC --Table Catalog PRD - source
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.source 
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  description STRING, 
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/source";

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.schema;
-- MAGIC --Table Catalog DEV - schema
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.schema 
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  description STRING, 
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/schema";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.schema;
-- MAGIC --Table Catalog PRD - schema
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.schema 
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  description STRING, 
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/schema"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.table;
-- MAGIC --Table Catalog DEV - table
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.table
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  description STRING,
-- MAGIC  replica INT,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/table";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.table;
-- MAGIC --Table Catalog PRD - table
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.table
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  description STRING,
-- MAGIC  replica INT,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/table"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.field;
-- MAGIC --Table Catalog DEV - field
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.field
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  field_name STRING,
-- MAGIC  data_type STRING,
-- MAGIC  data_steward STRING,
-- MAGIC  login STRING,
-- MAGIC  description STRING,
-- MAGIC  personal_data INT,
-- MAGIC  is_derivative INT,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/field";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.field;
-- MAGIC --Table Catalog PRD - field
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.field
-- MAGIC (source_name STRING,
-- MAGIC  source_type STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  field_name STRING,
-- MAGIC  data_type STRING,
-- MAGIC  data_steward STRING,
-- MAGIC  login STRING,
-- MAGIC  description STRING,
-- MAGIC  personal_data INT,
-- MAGIC  is_derivative INT,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/field"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.profiling;
-- MAGIC --Table Catalog DEV - profiling
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.profiling
-- MAGIC (source_name STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  field_name STRING,
-- MAGIC  metric STRING,
-- MAGIC  value DECIMAL(38,18),
-- MAGIC  created_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/profiling";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.profiling;
-- MAGIC --Table Catalog PRD - profiling
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.profiling
-- MAGIC (source_name STRING,
-- MAGIC  schema_name STRING,
-- MAGIC  table_name STRING,
-- MAGIC  field_name STRING,
-- MAGIC  metric STRING,
-- MAGIC  value DECIMAL(38,18),
-- MAGIC  created_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/profiling";

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS governance_dbfs_dev.metrics;
-- MAGIC --Table Catalog DEV - metrics
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.metrics
-- MAGIC (metric_name STRING,
-- MAGIC  type STRING,
-- MAGIC  description STRING,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/metrics";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_dbfs_prd.metrics;
-- MAGIC --Table Catalog PRD - metrics
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.metrics
-- MAGIC (metric_name STRING,
-- MAGIC  type STRING,
-- MAGIC  description STRING,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/metrics"

-- COMMAND ----------

---Atualizações 17/03/2021 (Adição das informações de data steward)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS governance_dev.data_steward;
-- MAGIC --Table Catalog DEV - data_steward
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_dev.data_steward
-- MAGIC (cod_data_steward STRING,
-- MAGIC  name_data_steward STRING,
-- MAGIC  dsc_business_subject STRING,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/dev/dbfs_tables/data_steward";
-- MAGIC  
-- MAGIC DROP TABLE IF EXISTS governance_prd.data_steward;
-- MAGIC --Table Catalog PRD - data_steward
-- MAGIC CREATE TABLE IF NOT EXISTS governance_dbfs_prd.data_steward
-- MAGIC (cod_data_steward STRING,
-- MAGIC  name_data_steward STRING,
-- MAGIC  dsc_business_subject STRING,
-- MAGIC  created_at TIMESTAMP,
-- MAGIC  updated_at TIMESTAMP)
-- MAGIC  USING delta
-- MAGIC  LOCATION "/dbfs/gov/prod/dbfs_tables/data_steward";

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.source ADD COLUMNS (cod_data_steward int);
ALTER TABLE governance_dbfs_prd.source ADD COLUMNS (cod_data_steward int)

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.schema ADD COLUMNS (cod_data_steward int);
ALTER TABLE governance_dbfs_prd.schema ADD COLUMNS (cod_data_steward int)

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.table ADD COLUMNS (dsc_business_subject string, cod_data_steward int);
ALTER TABLE governance_dbfs_prd.table ADD COLUMNS (dsc_business_subject string, cod_data_steward int)

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.field ADD COLUMNS (dsc_business_subject string, cod_data_steward int);
ALTER TABLE governance_dbfs_prd.field ADD COLUMNS (dsc_business_subject string, cod_data_steward int)

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.field ADD COLUMNS (ind_relevance int);
ALTER TABLE governance_dbfs_prd.field ADD COLUMNS (ind_relevance int)

-- COMMAND ----------

ALTER TABLE governance_dbfs_dev.schema ADD COLUMNS (replica int);
ALTER TABLE governance_dbfs_prd.schema ADD COLUMNS (replica int);