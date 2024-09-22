--# version History
--# Sept 2022, Stephanie Hong initial version
--# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
--#
-- Databricks notebook source
CREATE WIDGET TEXT databaseName DEFAULT "";

-- COMMAND ----------

-- Copy OMOP tables into deploy database
CREATE or replace TABLE crisp_omop.care_site deep clone ${databaseName}.care_site;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.cvlab_ext deep clone ${databaseName}.nedss_test_output_file_mapped;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.condition_occurrence_domain_map deep clone ${databaseName}.condition_occurrence_domain_map;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.death deep clone ${databaseName}.death;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.drug_exposure deep clone ${databaseName}.drug_exposure;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.location deep clone ${databaseName}.location;



-- COMMAND ----------

CREATE or replace TABLE crisp_omop.measurement deep clone ${databaseName}.measurement;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.observation deep clone ${databaseName}.observation;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.person deep clone ${databaseName}.person;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.procedure_occurrence deep clone ${databaseName}.procedure_occurrence;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.visit_occurrence deep clone ${databaseName}.visit_occurrence;

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.crisp_covid_correction deep clone ${databaseName}.crisp_covid_correction ;

-- COMMAND ----------

-- Create manifest table indicates which database the deploy database is currently usin

-- COMMAND ----------

-- CREATE or replace TABLE crisp_omop.Manifest (DatabaseName String,
-- ProcessedDate Date,
-- min_condition_start_date Date,
-- max_condition_start_date Date,
-- min_procedure_date Date,
-- max_procedure_date Date,
-- min_drug_exposure_start_date Date,
-- max_drug_exposure_start_date Date,
-- omop_vocab_version, string)

-- COMMAND ----------

CREATE or replace TABLE crisp_omop.Manifest
select 'crisp_08sept2022' as DatabaseName,
current_date() as ProcessedDate,
'2018-01-01' as min_condition_start_date,
'2022-06-30' as max_condition_start_date,
'2010-07-09' as min_procedure_date,
'2023-04-25' as max_procedure_date,
'1928-11-04' as min_drug_exposure_start_date,
'2022-08-22' as max_drug_exposure_start_date,
'v5.0 29-AUG-22' as omop_vocab_version



-- COMMAND ----------

select * from crisp_omop.Manifest

-- COMMAND ----------
