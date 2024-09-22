# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "crisp_08sept2022", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

###tblproperties(delta.autoOptimize.optimizeWrite=true)
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite=true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("SET spark.databricks.delta.overwriteSchema=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")


tablename="care_site"
path="crisp-covid/warehouse/"+databaseName
location="/mnt/{path}/{tablename}"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  care_site_id int,
  care_site_name string,
  place_of_service_concept_id int,
  location_id int,
  care_site_source_value string,
  place_of_service_source_value string
)
using delta
location "/mnt/{path}/{tablename}"
""")


# COMMAND ----------



dfcaresite = spark.sql(f"""
select
care_site_id,
care_site_name,
  place_of_service_concept_id,
  location_id,
  care_site_source_value,
  place_of_service_source_value
from {previousDatabaseName}.care_site

""")

# COMMAND ----------

### write and persist the content
write_format = 'delta'
##load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
##partition_by = 'gender_concept_id'

table_name = 'care_site'

###dfcaresite.write.format("delta").option("mergeSchema","true").mode("overwrite").save(f"{dbname}.{tablename}") we should have selectExpr("*") when we pass the dbname and tablename as parameters.
dfcaresite.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")
#added partitionBy to increase performance

# COMMAND ----------
