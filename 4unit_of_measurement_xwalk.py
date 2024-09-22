# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

print(previousDatabaseName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create crisp_covid_correction

# COMMAND ----------

### create a table for the crisp_covid_correction table

path="crisp-covid/warehouse/"+databaseName
tablename = "unit_of_measure_xwalk"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
 cdm_source string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

# COMMAND ----------


tablename = "unit_of_measure_xwalk"

###  fillin the data to build the unit_of_measure_xwalk table
df_unit_of_measure_xwalk = spark.sql(f"""
select cdm_source,CDM_TBL,CDM_TBL_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE,TARGET_TBL_COLUMN_NAME from {previousDatabaseName}.unit_of_measure_xwalk
  """)

# COMMAND ----------

###persist the table
### persist condition dataset
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

tablename="unit_of_measure_xwalk"

# persist the data to the table for later access.
df_unit_of_measure_xwalk.selectExpr("*","current_timestamp() as run_timestamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------
