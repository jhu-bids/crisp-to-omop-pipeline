# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
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
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


# if the new data contains qualitative result value that is not found in the current qualitative_result_xwalk table then
# we would need to add those to the qualitative_result_xwalk table.
# start from the existing table built from the prior data ingestion and add to the list that is found in the newly submitted dataset.



tablename='qualitative_result_xwalk'
path="crisp-covid/warehouse/"+databaseName


spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}
deep clone {previousDatabaseName}.qualitative_result_xwalk
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

#####crisp_sep2021.qualitative_result_xwalk
##### import the previous version here!!
##### These are the new entries


path="crisp-covid/warehouse/"+databaseName
tablename="qualitative_result_xwalk"

df_qual_xwalk = spark.sql("""
select column_name, qualitative_result, concept_id,
current_timestamp() as run_timeStamp
from {0}.qualitative_result_xwalk
union
select distinct 'AbnormalFlag' as column_name,
l.abnormalflag as qualitative_result,
0 as concept_id,
current_timestamp() as run_timeStamp
from {1}.viewLabs l
left join {0}.qualitative_result_xwalk qxw on lower(l.abnormalFlag) = lower(qxw.qualitative_result)
where qxw.qualitative_result is null and trim(length(l.abnormalFlag)) > 0
""".format(previousDatabaseName,databaseName))


# COMMAND ----------

#### persis data for qualitative_result_xwalk table
####


tablename="qualitative_result_xwalk"
## persist the data to a table
##df_qual_xwalk.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{dbname}.{tablename}")
df_qual_xwalk.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")
