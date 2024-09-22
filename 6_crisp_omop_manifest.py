# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
dbutils.widgets.text("databaseName", "yc_test", "CRISP database name")
databaseName=dbutils.widgets.get("databaseName")

# COMMAND ----------

# MAGIC %sql
# MAGIC select  min(condition_start_date),max(condition_start_date) from ${databaseName}.condition_occurrence_domain_map

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select max(procedure_date) as max, min(procedure_date) as min_procedure_date from ${databaseName}.procedure_occurrence
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select max(drug_exposure_start_date) as max, min(drug_exposure_start_date) as minprocedure_date from ${databaseName}.drug_exposure
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from omop_vocab.vocabulary where vocabulary_id='None'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from omop_vocab.vocabulary limit 5

# COMMAND ----------
