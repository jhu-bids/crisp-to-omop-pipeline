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

# COMMAND ----------

####### generate the visit type cross walk table
corrected_loinc_xwalk = spark.sql(f"""
select *,(
-- add more columns above, all the nedss columns + mapped
-- case when observationID not like '%-%' then corrected_code
-- case when observationID not in ('94500-6','94309-2','94558-4','94316-7','95209-3','94534-5','94559-2','94720-0','94533-7','95409-9','94565-9','94845-5','94531-1','94759-8','94502-2','94745-7','96119-3','95423-0') then corrected_code
case when observationID not in ('94500-6','94309-2','94558-4','94316-7','95209-3','94534-5','94559-2','94720-0','94533-7','95409-9','94565-9','94845-5','94531-1','94759-8','94502-2','94745-7','96119-3','95423-0','94505-5','95425-5','94500-6','95941-1','94314-2','94760-6') then corrected_code
else observationID
end) as mapped_code
from {databaseName}.viewCVLabs cvlab
left join {databaseName}.crisp_covid_correction map on cvlab.ObservationName = map.lab_name
""")

# COMMAND ----------

tablename="NEDSS_Test_Output_File_mapped"
corrected_loinc_xwalk.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

# %sql
# select distinct mapped_code from yc_test.NEDSS_Test_Output_File_mapped

# COMMAND ----------

### Testing

# COMMAND ----------

# %sql
# select observationID,corrected_code,mapped_code from yc_test.NEDSS_Test_Output_File_mapped where observationID not in ('94500-6','94309-2','94558-4','94316-7','95209-3','94534-5','94559-2','94720-0','94533-7','95409-9','94565-9','94845-5','94531-1','94759-8','94502-2','94745-7','96119-3','95423-0','94505-5','95425-5','94500-6','95941-1','94314-2','94760-6') limit 50

# COMMAND ----------

# %sql
# select * from yc_test.viewCVLabs where observationID not in ('94500-6','94309-2','94558-4','94316-7','95209-3','94534-5','94559-2','94720-0','94533-7','95409-9','94565-9','94845-5','94531-1','94759-8','94502-2','94745-7','96119-3','95423-0','94505-5','95425-5','94500-6','95941-1','94314-2','94760-6') limit 50

# COMMAND ----------
