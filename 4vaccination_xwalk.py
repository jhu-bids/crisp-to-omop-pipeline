# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
# dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "yc_test", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

###vaccination xwalk table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# path=dbutils.widgets.get("parquetFilePath")

# patientFile = path+'Patient_File'
# vaccFile = path+'Covid19_Vaccinations_File'

# dfPatient = spark.read.parquet(patientFile)
# dfVacc = spark.read.parquet(vaccFile)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${databaseName}.vaccTableView
# MAGIC USING parquet
# MAGIC options(path "${parquetFilePath}Covid19_Vaccinations_File" )
# MAGIC
# MAGIC

# COMMAND ----------

# path=dbutils.widgets.get("parquetFilePath")
# filename='Covid19_Vaccinations_File'

# spark.sql(f"""CREATE or replace TABLE {databaseName}.vaccTableView
# USING parquet
# OPTIONS (path "/mnt/{path}/{filename}")

# """)

# COMMAND ----------


path="crisp-covid/warehouse/"+databaseName

tablename="vaccination_xwalk"
location= "/mnt/{path}/{tablename}"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {databaseName}.{tablename}(
manufacturer_id bigint
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create vaccination xwalk

# COMMAND ----------

###
### build vaccination xwalk
###

dfVaccination_xwalk = spark.sql(f"""
 select distinct
cast(conv(substr(md5(concat(x.manufacturer, cast(x.target_concept_id as string))), 1, 15), 16, 10) as bigint) & 2251799813685247 as manufacturer_id,
x.*
from
(select distinct
  vc.manufacturer,
  case when vc.manufacturer='MOD' then 'Moderna'
   when vc.manufacturer= 'PFR' then 'Pfizer'
   when vc.manufacturer='JSN' then 'Janssen'
   end as manufacturer_name,
   case when vc.manufacturer='MOD' then 37003518
   when vc.manufacturer='PFR' then 37003436
   when vc.manufacturer='JSN' then 739906
   else 0 end as target_concept_id,
  'Drug' as target_domain_id,
  current_timestamp() as run_timestamp
  from {databaseName}.vaccTableView vc
  where vc.manufacturer is not null or trim(vc.manufacturer) !=null
) x
  """)

# COMMAND ----------

### persist condition dataset
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

tablename="vaccination_xwalk"
# persist the data to the table for later access.
dfVaccination_xwalk.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------



# COMMAND ----------
