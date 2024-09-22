# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "crisp_08sept2022", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create cdm_manifest table
# MAGIC **comment out for now as we have manifest table created earlier**

# COMMAND ----------



###dfloc.write.format("delta").partitionBy("location_id").save("/mnt/crisp-covid/crisp_omop_2/location")
###tblproperties(delta.autoOptimize.optimizeWrite=true)
# spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
# spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite=true")
# spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# spark.sql("SET spark.databricks.delta.overwriteSchema=true")
# spark.sql("set spark.databricks.delta.commitValidation.enabled=false")

#### create cdm_manifest table
####
### set dbname
### set table path

# path="crisp-covid/warehouse/"+databaseName
# tablename="cdm_manifest"
#####################################################
## create the database we are using for this payload
## create the database if not exist
#spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(databaseName,path) )
##
### Create the database
##dbname = "db1"
##dblocation = "abfss://container@storage_account.dfs.core.windows.net/" + dbname + ".db"
##spark.sql("CREATE DATABASE IF NOT EXISTS" + dbname + " LOCATION '" + dblocation + "'")
#####################################################


##data source =Maryland CRISP data
##data run data = March 2021
##data contribution date = 09_20_2021
## parquet data file path '/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/'
## data processed date 10_29_2021
## data counts:
## domain - patient, visit, diag, proc, lab, cvlab, vaccination
## create the table if not exist
# spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
#   data_source string,
#   data_source_description string,
#   data_source_file_path string,
#   source_documentation_reference string,
#   etl_reference	string,
#   data_run_date date,
#   data_contribution_date date,
#   data_processed_date date,
#   cdm_version string,
#   vocabulary_version string,
#   note string
# )
# using delta
# tblproperties(delta.autoOptimize.optimizeWrite=true)
# location "/mnt/{path}/{tablename}"
# """)



# COMMAND ----------

path="crisp-covid/warehouse/"+databaseName
tablename="cdm_manifest"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  data_source string,
  data_source_description string,
  data_source_file_path string,
  source_documentation_reference string,
  etl_reference	string,
  data_run_date date,
  data_contribution_date date,
  data_processed_date date,
  cdm_version string,
  vocabulary_version string,
  note string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create cdm_source table
# MAGIC
# MAGIC # auto it?

# COMMAND ----------

#### create cdm_source table
####
####

path="crisp-covid/warehouse/"+databaseName
tablename="cdm_source"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  cdm_source_name string,
  cdm_source_abbreviation string,
  cdm_holder string,
  source_description string,
  source_documentation_reference string,
  cdm_etl_reference	string,
  source_release_date date,
  cdm_release_date date,
  cdm_version string,
  vocabulary_version string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ${databaseName}.cdm_source
# MAGIC SELECT 'Maryland State Health Information Exchange Dataset' as cdm_source_name,
# MAGIC 'CRISP' as cdm_source_abbreviation,
# MAGIC 'JHU' as cdm_holder,
# MAGIC 'Maryland State COVID19 dataset up to March 2021' as source_description,
# MAGIC 'https://crisphealth.org/resources/documents/' AS source_documentation_reference,
# MAGIC 'https://ohdsi.github.io/CommonDataModel/cdm531.html' AS cdm_etl_reference,
# MAGIC (select from_unixtime(unix_timestamp('09-20-2021', 'MM-dd-yyyy'))) as source_release_date,
# MAGIC (select from_unixtime(unix_timestamp('10-29-2021', 'MM-dd-yyyy'))) as cdm_release_date,
# MAGIC  'version 5.3.1' AS cdm_version,
# MAGIC  'v5.0 29-AUG-22' AS vocabulary_version;

# COMMAND ----------
