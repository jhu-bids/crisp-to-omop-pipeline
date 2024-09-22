# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
## Following is the general overview.
## parse the incoming data files into the separate dataframes for processing
## create manifest and data count files
## check the rows count for each domain against the reported informations
## the data pipeline is setup in the following steps
## 1. parsing / prepare data for mapping
## 2. sql mapping to OMOP domains, including crisp2OMOP terminology mapping/
## 3. manage primary key management
## 4. generate global ids - resolve dups
## 5. prepare final OMOP tables and persist the tables
## Last Edit: Date
## author: Date:      Comments:
## Shong   10/1/2021  Initial uptake of the files drop on 9/28/21 processing data upto 3/31/21 files-  create manifest / cmd source/ person and visits
## Tanner 2/1/2022 Add logic to include covid tests without visit information
## Yvette 4/25/2023 Change the hard-wired path


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

path=dbutils.widgets.get("parquetFilePath")

# patientFile = path+'Patient_File'
# encFile = path+'PatientVisits_output_File'
# genderFile = path+'Missing_gender_File'
# diagnosisFile = path+'PatientDiagnosisVisits_output_File'
# procFile = path+'PatientProcedureVisits_output_File'
# labFile = path+'All_Lab_observations_output_File'
# cvLabFile = path+'NEDSS_Test_Output_File'
# cvLabUndupFile = path+'NEDSS_Test_Output_unduplicated_File'
# vaccFile = path+'Covid19_Vaccinations_File'
# visitFile= path+'PatientVisits_output_File'

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
#Read the contents of the parquet file into a dataframe
# dfPatient = spark.read.parquet(patientFile)
# ##dfEnc = spark.read.parquet('/mnt/crisp-covid/sftp/JHU_Full_Load_04_26_Parquet/PatientVisits_output_masked_File')
# dfEnc = spark.read.parquet(encFile)
# dfGender = spark.read.parquet(genderFile)
# dfVisit = spark.read.parquet(visitFile)
# dfDiagnosis = spark.read.parquet(diagnosisFile)
# dfProc = spark.read.parquet(procFile)
# dfLabs = spark.read.parquet(labFile)
# dfCVLabs = spark.read.parquet(cvLabFile)
# dfCVLabsUndup = spark.read.parquet(cvLabUndupFile)
# ## no vaccination file?
# dfVacc = spark.read.parquet(vaccFile)

# COMMAND ----------

# dfPatient= dfPatient.drop_duplicates()
# patCount = dfPatient.select('Research_Patient_Key').distinct().count()
# patCount
# ##6,103,600

# COMMAND ----------

# ##dfEnc = dfEnc.drop_duplicates()
# encCount = dfEnc.select('Research_Patient_Key').count()
# encCount
# ##18,110,514

# COMMAND ----------

# visitCount = dfVisit.select('Research_Patient_Key', 'VisitsID_Reference_Key').distinct().count()
# ##18,110,514
# ##dfVisit.createOrReplaceTempView('viewVisit')
# visitCount

# COMMAND ----------

# genderCount = dfGender.select('Research_Patient_Key').distinct().count()
# genderCount
#67,010 distinct with research patient key

# COMMAND ----------

# dfDiagnosis = dfDiagnosis.drop_duplicates()
# diagCount = dfDiagnosis.select('Research_Patient_Key', 'VisitsID_Reference_Key').count()

# ##135,764,905 before dups
# ##98,652,340- patient/visit key count 59,420,821

# ##display(dfDiagnosis)
# diagCount
# ##98,652,340

# COMMAND ----------

# dfProc = dfProc.drop_duplicates()
# procCount = dfProc.distinct().count()
# ##96,116,022 - patient key 86,938,771
# procCountPatientVisit = dfProc.select('Research_Patient_Key', 'VisitsID_Reference_Key').count()

# procCountPatientVisit
# ##96,116,022

# COMMAND ----------

# procCount

# COMMAND ----------

# dfLabs = dfLabs.drop_duplicates()
# #labCount = dfLabs.select('Research_Patient_Key','VisitsID_Reference_Key').distinct().count()
# labCount = dfLabs.distinct().count()
# labCount
##1,116,920,526 count labs after de-dup

# COMMAND ----------

# dfCVLabs = dfCVLabs.drop_duplicates()
# #cvLabCount = dfCVLabs.select('Research_Patient_Key','SourceCaretAccessionNumber','OrderID').distinct().count()
# cvLabCount = dfCVLabs.distinct().count()
# cvLabCount
# ##26,694,663


# COMMAND ----------

# dfVacc= dfVacc.drop_duplicates()
# #vacCount = dfVacc.select('research_patient_key', 'vaccination_date').distinct().count()
# vacCount = dfVacc.distinct().count()
# ###display(dfVacc)
# ## research_patient_key, vaccination_date, manufacturer, vaccine_lot_id, ADMINISTRATION_ORG_NAME, Administration_Org_Facility_System_Name
# ###6,340,629
# ###6,337,099 de-dup patient key and vaccination date
# ###6,340,629 raw count after de-dup
# vacCount
# display(dfVacc)

# COMMAND ----------

# ### prep parsed data for views
# from pyspark.sql import functions as F
# from pyspark.sql import Window
# from pyspark.sql.functions import sha2, concat_ws

# dfPatient = dfPatient.withColumn("loc_id0", F.concat_ws("-", "patient_zip", "census_block", "census_geoid"))
# display(dfPatient)

# COMMAND ----------

# ## create views
# dfPatient.createOrReplaceTempView('viewPatient')
# dfGender.createOrReplaceTempView('viewGender')

# dfEnc.createOrReplaceTempView('viewEncounter')
# dfVisit.createOrReplaceTempView('viewVisit')

# dfDiagnosis.createOrReplaceTempView('viewDiagnosis')
# dfProc.createOrReplaceTempView('viewProcedure')

# dfLabs.createOrReplaceTempView('viewLabs')
# dfCVLabs.createOrReplaceTempView('viewCVLabs')
# dfVacc.createOrReplaceTempView('viewVaccine')

# COMMAND ----------

# %sql
# ---create or replace table viewPatientGenderEnriched as
# select p.Research_Patient_Key,
# p.Gender,
# g.Research_Patient_Key as gender_Research_Patient_Key,
# g.Gender as enrichedGender,
# case when g.Gender is null then p.Gender
# else g.Gender
# end as mapped_gender
# from viewPatient p
# left join viewGender g on p.Research_Patient_Key = g.Research_Patient_Key
# where g.Research_patient_key is not null

# COMMAND ----------

# %sql
# ---"loc_id0", F.concat_ws("-", "patient_zip", "census_block", "census_geoid")
# select
# cast(conv(substr(md5(loc_id0), 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id,
# null as address_1,
# null as address_2,
# null as city,
# null as state,
# patient_zip as zip,
# null as county,
# loc_id0 as location_source_value
# from
#   viewPatient

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the location dataframe

# COMMAND ----------

## create the location dataframe
##
##row_number() over( partition by patient_zip, Census_Block, Census_Geoid order by patient_zip, Census_Block, Census_Geoid )
##md5(loc_id0) as location_id
###cast(conv(substr(md5(F.concat_ws("-", "patient_zip", "census_block", "census_geoid")), 1, 16), 16, 10) as bigint) &  18446744073709551615 as  location_id
###-- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
# dfloc = spark.sql("""
# select
# cast(conv(substr(md5(loc_id0), 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id,
# null as address_1,
# null as address_2,
# null as city,
# null as state,
# patient_zip as zip,
# null as county,
# loc_id0 as location_source_value
# from
#   viewPatient
# """)


dfloc = spark.sql(f"""
select
cast(conv(substr(md5(loc_id0), 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id,
null as address_1,
null as address_2,
null as city,
null as state,
patient_zip as zip,
Census_Block as block,
Census_Geoid as geoid,
null as county,
loc_id0 as location_source_value
from
  {databaseName}.viewPatient
""")

# COMMAND ----------

display(dfloc)
dfloc.createOrReplaceTempView("viewLocation")
dfloc.show()

# COMMAND ----------




# COMMAND ----------

### create the location table

path="crisp-covid/warehouse/"+databaseName
tablename="location"

#spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path) )
#dfloc.write.format("delta") \
#  .mode("overwrite") \
#  .option("overwriteSchema", "true") \
#  .partitionBy("location_id") \
#  .option("path", "/mnt/crisp-covid/crisp_omop_2") \
#  .saveAsTable("location") # External table


## had to use long type for location id
## location2 table has bigint for location_id
# spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
#   location_id long,
#   address_1 string,
#   address_2 string,
#   city string,
#   state string,
#   zip string,
#   county string,
#   location_source_value string
# )
spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  location_id BIGINT,
  address_1 string,
  address_2 string,
  city string,
  state string,
  zip string,
  block string,
  geoid string,
  county string,
  location_source_value string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------


dfloc.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yc_test.location

# COMMAND ----------

# %sql
# /*
# --if the new data contains qualitative result value that is not found in the current qualitative_result_xwalk table then
# ----we would need to add those to the qualitative_result_xwalk table.
# --- start from the existing table built from the prior data ingestion and add to the list that is found in the newly submitted dataset.
# ----
# ---- insert new entries from this dataset into the
# --- column_name | qualitative_result | concept_id | runtime_stamp
# --- find entries need to add to the qualitative_result_xwalk table
# select
# distinct l.abnormalflag, qxw.*
# from viewLabs l
# left join ${databaseName}.qualitative_result_xwalk qxw on lower(l.abnormalFlag) = lower(qxw.qualitative_result)
# where qxw.qualitative_result is null and trim(length(l.abnormalFlag)) > 0
# */

# COMMAND ----------

# %sql
# /*
# select distinct 'AbnormalFlag' as column_name,
# l.abnormalflag as qualitative_result,
# 0 as concept_id
# from viewLabs l
# left join ${databaseName}.qualitative_result_xwalk qxw on lower(l.abnormalFlag) = lower(qxw.qualitative_result)
# where qxw.qualitative_result is null and trim(length(l.abnormalFlag)) > 0
# */

# COMMAND ----------

# %sql
# /*

# --existing entries and new entries
# select column_name, qualitative_result, concept_id,
# current_timestamp() as run_timeStamp
# from ${databaseName}.qualitative_result_xwalk --36 rows +1 = 37 rows.
# union
# select distinct 'AbnormalFlag' as column_name,
# l.abnormalflag as qualitative_result,
# 0 as concept_id,
# current_timestamp() as run_timeStamp
# from viewLabs l
# left join ${databaseName}.qualitative_result_xwalk qxw on lower(l.abnormalFlag) = lower(qxw.qualitative_result)
# where qxw.qualitative_result is null and trim(length(l.abnormalFlag)) > 0
# */
