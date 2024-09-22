# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')

dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "crisp_08sept2022", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

### Note, Vaccination data files are already merged in with Procedures etl files - prcedures to drug_exposure steps.
### Stephanie Hong 11/11/21
###"${parquetFilePath}Covid19_Vaccinations_File"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${databaseName}.vaccSepTableFromParquet
# MAGIC USING parquet
# MAGIC OPTIONS (path "${parquetFilePath}Covid19_Vaccinations_File")

# COMMAND ----------

# ####
# ####vaccination data to drug exposure has already added from the procedures to drug code
# ####

# path="crisp-covid/warehouse/"+databaseName
# tablename="vaccinationDrugExposure"


# spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
#   procedure_occurrence_id bigint,
# )
# using delta
# tblproperties(delta.autoOptimize.optimizeWrite=true)
# location "/mnt/{path}/{tablename}"

# """)
