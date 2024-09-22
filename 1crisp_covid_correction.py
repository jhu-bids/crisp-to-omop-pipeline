# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
# MAGIC %md
# MAGIC ## Create crisp_covid_correction table

# COMMAND ----------

##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "yc_test", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

### create a table for the crisp_covid_correction table

tablename = "crisp_covid_correction"
path="crisp-covid/warehouse/"+databaseName

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
 lab_name string,
 corrected_code string,
 run_timestamp timestamp
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

#############
############
## if new entries should be added the logic should go here and union the new entries with existing  entries from previous load like crisp_apr2021 db
##
df_crisp_covid_correction = spark.sql("""
select lab_name, corrected_code, current_timestamp() as run_timestamp
from {}.crisp_covid_correction
""".format(previousDatabaseName))


# COMMAND ----------


tablename = "crisp_covid_correction"
## persist the data to a table
##df_qual_xwalk.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{dbname}.{tablename}")
df_crisp_covid_correction.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------
