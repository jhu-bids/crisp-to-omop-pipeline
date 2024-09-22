# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

### process condition from diagnosis/procedures/labs/covid labs
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")


# path=dbutils.widgets.get("parquetFilePath")

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


# dfPatient = spark.read.parquet(patientFile)
# dfVisit = spark.read.parquet(visitFile)
# dfDiagnosis = spark.read.parquet(diagnosisFile)
# dfProc = spark.read.parquet(procFile)
# dfLabs = spark.read.parquet(labFile)
# dfCVLabs = spark.read.parquet(cvLabFile)
# dfVacc = spark.read.parquet(vaccFile)

# dfPatient.distinct()
# dfVisit.distinct()
# dfDiagnosis.distinct()
# dfProc.distinct()
# dfLabs.distinct()
# dfCVLabs.distinct()
# dfPatient.count()

# dfDiagnosis.count()
# ##
# dfProc.count()
# ##
# dfLabs.count()
# ##
# dfCVLabs.count()
# ##
# dfVacc.count()
# ##5,285,011

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, udf, to_date, to_timestamp
from pyspark.sql.functions import length
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import sha2, concat_ws
from datetime import datetime
from pyspark.sql import types as T
from pyspark.sql.types import DateType
from pyspark.sql.types import StringType
from pyspark.sql.functions import date_format

# #Read the contents of the patient parquet file into a dataframe
# dfPatient = dfPatient.withColumn("loc_id0", F.concat_ws("-", "patient_zip", "census_block", "census_geoid"))

# dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
# dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
# dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))

# dfVisit = dfVisit.withColumn( "admitdt_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_datetime", to_timestamp(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_date", to_date(dfVisit['dischargedt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_datetime", to_timestamp(dfVisit['dischargedt'], 'MM/dd/yyyy'))

# dfLabs.dropDuplicates()
# ###incoming data format: 2020-06-24T17:20:51.000+0000 incoming data pattern
# ts_pattern ="YYYY-MM-DD HH:mm:ss.SSS"
# dfLabs = dfLabs.withColumn("mapped_vocabulary_id", lit("LOINC"))
# dfLabs = dfLabs.withColumn("lab_date", to_date(dfLabs['DateIssued'], 'YYYY-MM-DD'))
# dfLabs = dfLabs.withColumn("time", date_format(dfLabs['DateIssued'], 'HH:mm:ss'))
# dfLabs = dfLabs.withColumn("lab_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "partitionDate", "ObservationNumber", "source_Reference_Key", "Parent_ID_Identifer", "Parent_description", "LoincCode", "Status", "AbnormalFlag", "DateIssued","Method", "ValueConcept", "ValueDateTime","ValueString", "ValueQuantity", "ValueQuantityUnits", "Loinc_Desc"))

# dfCVLabs.dropDuplicates()
# dfCVLabs=dfCVLabs.withColumn("mapped_vocabulary_id", lit("LOINC"))
# dfCVLabs = dfCVLabs.withColumn("lab_id", F.concat_ws("-", "Research_Patient_Key", "SourceCaretAccessionNumber", "OrderID", "observationID", "ObservationName", "processed_datetime", "order_date", "specimen_collection_date", "result_date", "OBX_5_1_Result", "OBX_5_2_Result", "calculated_test_result", "ordering_physician", "ordering_facility" ))
# ##### use specimen_collection_date to extract measurement time
# dfCVLabs = dfCVLabs.withColumn("specimen_collection_time", date_format(dfCVLabs['specimen_collection_date'], 'HH:mm:ss'))
# ###dfCVLabs  order_date pattern = 2020-05-27



# COMMAND ----------


### create view of the dataframe
# Create a view or table
# spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# tbl_name = "viewPatient"
# dfPatient.createOrReplaceTempView(tbl_name)

# tblName = "viewVisit"
# dfVisit.createOrReplaceTempView(tblName)

# diag_tbl_name = "viewDiag"
# dfDiagnosis.createOrReplaceTempView(diag_tbl_name)

# proc_tbl_name = "viewProc"
# dfProc.createOrReplaceTempView(proc_tbl_name)

# lab_tbl_name = "viewLabs"
# dfLabs.createOrReplaceTempView(lab_tbl_name)

# cvlab_tbl_name = "viewCVLabs"
# dfCVLabs.createOrReplaceTempView(cvlab_tbl_name)

# COMMAND ----------

#### measurement table
####1.create measurement table so that we can save the data for dqd
####

path="crisp-covid/warehouse/"+databaseName
tablename = "measurement"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  measurement_id BIGINT,
  person_id	int,
  measurement_concept_id int,
  measurement_date date,
  measurement_datetime timestamp,
  measurement_time string,
  measurement_type_concept_id int,
  operator_concept_id int,
  value_as_number float,
  value_as_concept_id int,
  unit_concept_id int,
  range_low float,
  range_high float,
  provider_id int,
  visit_occurrence_id int,
  visit_detail_id int,
  measurement_source_value string,
  measurement_source_concept_id int,
  unit_source_value string,
  value_source_value string,
  target_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------

# %sql
# /****
# select the data from the source files
# select measurement from the lab data ******/
# --select count(*) from
# --(
# select distinct
# ------------generated id based on the each row of measurement data
# l.lab_id,
# md5(concat(lab_id, coalesce(unit.target_concept_id, ' '))) as hashed_id,
# cast(conv(substr(md5(concat(lab_id, coalesce(unit.target_concept_id, v.provider_id,v.care_site_id,p.person_id, xw.source_concept_id))), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,
# p.person_id,
# xw.target_concept_id as measurement_concept_id,
# cast( DateIssued as date ) as measurement_date,
# cast( DateIssued as timestamp) as measurement_datetime,
# l.time as measurement_time,
# 32817 as measurement_type_concept_id,
#         CAST(null as int) as operator_concept_id,
# CAST(ValueQuantity as float ) as value_as_number, --result_num
#         CAST(qxw.concept_id as int) as value_as_concept_id,---categorical value , qualitative_labresult concept_id
#         COALESCE(unit.target_concept_id, 0 ) as unit_concept_id, ---mapped value of the ValueQuantityUnits
#         CAST(null as float) as range_low,
#         CAST(null as float) as range_high,
#         CAST(null as int) AS provider_id,
#  v.visit_occurrence_id,
#  0 as  visit_detail_id,
#  concat(l.LoincCode, '|', l.Loinc_Desc, '|', l.AbnormalFlag )  as measurement_source_value,
#  xw.source_concept_id as measurement_source_concept_id,
#  l.ValueQuantityUnits as unit_source_value,
#  l.ValueQuantity as  value_source_value,
#  xw.target_concept_name
# from ${databaseName}.viewLabs l
#   join ${databaseName}.person p on l.Research_Patient_Key = p.person_id
#   join ${databaseName}.visit_occurrence v on v.visit_occurrence_id = l.VisitsID_Reference_Key and v.person_id = l.Research_Patient_Key
#   -- map loinc code
#   left join ${databaseName}.c2o_code_xwalk xw on l.LoincCode = xw.src_code and l.mapped_vocabulary_id = xw.src_vocab_code --do join, only add labs with LOINC code
#   -- if person_id join is dropped then the count goes to back up to 145,748,624, if person_id join is added the couxnt goes to 115
#   left join ${databaseName}.qualitative_result_xwalk qxw on qxw.column_name = 'AbnormalFlag' and qxw.qualitative_result = l.AbnormalFlag
#   left join ${databaseName}.unit_of_measure_xwalk unit on trim(l.ValueQuantityUnits) = trim(unit.src_code)
#   where xw.target_domain_id ='Measurement' and xw.src_vocab_code = 'LOINC' and l.status = 'final' and  VisitsID_Reference_Key is not null and LoincCode is not null
# --) f


# COMMAND ----------

dfMeasurement1 = spark.sql(f"""
select distinct
l.lab_id,
md5(concat(lab_id, coalesce(unit.target_concept_id, ' '))) as hashed_id,
-- cast(conv(substr(md5(concat(lab_id, coalesce(unit.target_concept_id, v.visit_occurrence_id,p.person_id,l.time,DateIssued,xw.source_concept_id,xw.target_concept_name,cvl.calculated_test_result,ValueQuantity,qxw.concept_id,))), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,
p.person_id,
xw.target_concept_id as measurement_concept_id,
cast( DateIssued as date ) as measurement_date,
cast( DateIssued as timestamp) as measurement_datetime,
l.time as measurement_time,
32817 as measurement_type_concept_id,
        CAST(null as int) as operator_concept_id,
CAST(ValueQuantity as float ) as value_as_number,
        CAST(qxw.concept_id as int) as value_as_concept_id,
        COALESCE(unit.target_concept_id, 0 ) as unit_concept_id,
        CAST(null as float) as range_low,
        CAST(null as float) as range_high,
        CAST(null as int) AS provider_id,
 v.visit_occurrence_id,
 0 as  visit_detail_id,
 concat(l.LoincCode, '|', l.Loinc_Desc,  '|', l.AbnormalFlag )  as measurement_source_value,
 xw.source_concept_id as measurement_source_concept_id,
 l.ValueQuantityUnits as unit_source_value,
 l.ValueQuantity as  value_source_value,
  xw.target_concept_name
from {databaseName}.viewLabs l
  join  {databaseName}.person p on l.Research_Patient_Key = p.person_id
  join  {databaseName}.visit_occurrence v on v.visit_occurrence_id = l.VisitsID_Reference_Key and v.person_id = l.Research_Patient_Key
  left join {databaseName}.c2o_code_xwalk xw on l.LoincCode = xw.src_code and l.mapped_vocabulary_id = xw.src_vocab_code
  left join {databaseName}.qualitative_result_xwalk qxw on qxw.column_name = 'AbnormalFlag' and qxw.qualitative_result = l.AbnormalFlag
  left join {databaseName}.unit_of_measure_xwalk unit on trim(l.ValueQuantityUnits) = trim(unit.src_code)
  where xw.target_domain_id ='Measurement' and xw.src_vocab_code = 'LOINC' and l.status = 'final' and  VisitsID_Reference_Key is not null and LoincCode is not null

""")

# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

table_measurement1 = "temp1"
dfMeasurement1.createOrReplaceTempView(table_measurement1)

# COMMAND ----------

# %sql
# select * from yc_test.test

# COMMAND ----------

# %sql
# select
# distinct
# cast(conv(substr(md5(concat(lab_id, row_index,coalesce(measurement_concept_id,""),coalesce(person_id,""),COALESCE(measurement_date," "),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) as measurement_id,
# *
# from (
# select *,
#   row_number() over (
#   partition by lab_id,
#   measurement_concept_id,
#   person_id,
#   measurement_date,
#   measurement_time,
#   measurement_datetime,
#   value_as_number,
#   value_as_concept_id,
#   unit_concept_id,
#   measurement_source_value,
#   unit_source_value,
#   target_concept_name,
#   value_source_value
#   order by lab_id,
#   measurement_concept_id,
#   person_id,
#   measurement_date,
#   measurement_time,
#   measurement_datetime,
#   value_as_number,
#   value_as_concept_id,
#   unit_concept_id,
#   measurement_source_value,
#   unit_source_value,
#   target_concept_name,
#   value_source_value) as row_index
# from yc_test.test )

# COMMAND ----------

# %sql
# -- select conv(measurement_id, 16, 10) as base_10_hash_value,* from(
# select
# distinct
# cast(conv(substr(md5(concat(lab_id, coalesce(measurement_concept_id,""),coalesce(person_id,""),COALESCE(measurement_date," "),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,



# -- substr(md5(concat_ws('.',COALESCE(lab_id," "), COALESCE(person_id," "),COALESCE(measurement_concept_id," "),COALESCE(measurement_datetime," "),COALESCE(measurement_time," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(visit_occurrence_id," "), COALESCE(measurement_source_value," "),COALESCE(measurement_source_concept_id," "),COALESCE(unit_source_value," "),COALESCE(value_source_value," "),COALESCE(target_concept_name," "))), 1, 50 ) as measurement_id,


# -- cast(conv(substr(md5(concat_ws('.', COALESCE(person_id," "),COALESCE(measurement_concept_id," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(visit_occurrence_id," "),COALESCE(measurement_source_concept_id," "),COALESCE(value_source_value," "))), 1, 50 ), 16, 10) & 2251799813685247 as bigint) as measurement_id,
# *
# from yc_test.test


# COMMAND ----------

dfMeasurement1_final = spark.sql(f"""
select
distinct
cast(conv(substr(md5(concat_ws(".",lab_id, row_index,coalesce(person_id,""),COALESCE(measurement_date," "),coalesce(measurement_concept_id,""),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) as measurement_id,
*
from (
select *,
  row_number() over (
  partition by lab_id,
  measurement_concept_id,
  person_id,
  measurement_date,
  measurement_time,
  measurement_datetime,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  measurement_source_value,
  unit_source_value,
  target_concept_name,
  value_source_value
  order by lab_id,
  measurement_concept_id,
  person_id,
  measurement_date,
  measurement_time,
  measurement_datetime,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  measurement_source_value,
  unit_source_value,
  target_concept_name,
  value_source_value) as row_index
from temp1)


--select
--distinct
-- # cast(conv(substr(md5(concat(lab_id, coalesce(measurement_concept_id,""),coalesce(person_id,""),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,

-- # *
-- # from temp1


""")

# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

table_measurement1 = "view_measurement1"
dfMeasurement1_final.createOrReplaceTempView(table_measurement1)

# COMMAND ----------

# %sql
#  select count(*) from view_measurement1 where measurement_id is null

# COMMAND ----------

# %sql
#  select count(*) from view_measurement1 where measurement_id is not null

# COMMAND ----------

# %sql
#  select count(distinct measurement_id) from view_measurement1 where measurement_id is not null

# COMMAND ----------

# %sql
#  select * from view_measurement1 limit 5

# COMMAND ----------

###
### Check for uniqueness of each row we are persisting. The following query should not return any rows.
###
dfTestDups = spark.sql("""
select distinct
measurement_id,
person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name,
  count(*) cnt
from view_measurement1
group by
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name
 having cnt >1
""")

# COMMAND ----------

dfTestDups.createOrReplaceTempView("test")

# COMMAND ----------

# #### extend cvlab data with mapped LOINC code
# ####1.create the table so that we can save the data for dqd
# ####

# path="crisp-covid/warehouse/"+databaseName
# tablename="cvlab_ext"

# spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
#   Research_Patient_Key int,
#   SourceCaretAccessionNumber string,
#   orderID string,
#   observationID string,
#   observationName string,
#   processed_datetime timestamp,
#   order_date date,
#   specimen_collection_date timestamp,
#   result_date timestamp,
#   OBX_5_1_Result string,
#   OBX_5_2_Result string,
#   calculated_test_result string,
#   ordering_physician double,
#   ordering_facility string,
#   from string,
#   mapped_vocabulary_id string,
#   lab_id string,
#   specimen_collection_time string,
#   corrected_code string,
#   mapped_code string
# )
# using delta
# tblproperties(delta.autoOptimize.optimizeWrite=true)
# location "/mnt/{path}/{tablename}"

# """)

# COMMAND ----------

# ####### generate the cvlab data with corrected LOINC code
# dfcvlab_ext = spark.sql(f"""
# select cvlab.*, corrected_code,
#           case when observationID not like '%-%' then corrected_code
#           when observationID like '%-%' then observationID
#           when (observationID not like '%-%' and lab_name IS null ) then '0'
#           end as mapped_code
# from viewCVLabs cvlab
# left join {databaseName}.crisp_covid_correction map on cvlab.ObservationName = map.lab_name
# """)

# COMMAND ----------

# spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# tablename = "cvlab_ext"
# # persist the data to the table for later access.
# ##dfcvlab_ext.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/cvlab_ext")
# dfcvlab_ext.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------

dfMeasurement2 = spark.sql(f"""
select distinct
cvl.lab_id,
md5(cvl.lab_id) as hashed_id,
-- cast(conv(substr(md5(concat(unit.target_concept_id, v.visit_occurrence_id,p.person_id,cvl.mapped_code,xw.source_concept_id,xw.target_concept_name,cvl.calculated_test_result)), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,
p.person_id,
xw.target_concept_id as measurement_concept_id,
cast( cvl.specimen_collection_date as date ) as measurement_date,
cast( cvl.specimen_collection_date as timestamp) as measurement_datetime,
cvl.specimen_collection_time as measurement_time,
32817 as measurement_type_concept_id,
    CAST(null as int) as operator_concept_id,
    CAST(null as float ) as value_as_number,
        CAST(qxw.concept_id as int) as value_as_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as float) as range_low,
        CAST(null as float) as range_high,
        CAST(null as int) AS provider_id,
 v.visit_occurrence_id,
 0 as  visit_detail_id,
cvl.mapped_code as measurement_source_value,
 xw.source_concept_id as measurement_source_concept_id,
 CAST(null as string) as unit_source_value,
 cvl.calculated_test_result as  value_source_value,
 xw.target_concept_name
from {databaseName}.nedss_test_output_file_mapped cvl
 join  {databaseName}.person p on cvl.Research_Patient_Key = p.person_id
 join  {databaseName}.visit_occurrence v on v.person_id = cvl.Research_Patient_Key and cvl.order_date = v.visit_start_date
 /** --left join crisp_sep2021.c2o_code_xwalk xw on cvl.observationID = xw.src_code and cvl.mapped_vocabulary_id = xw.src_vocab_code **/
 left join {databaseName}.c2o_code_xwalk xw on upper(trim(cvl.mapped_code)) = upper(trim(xw.src_code)) and cvl.mapped_vocabulary_id = xw.src_vocab_code
 left join {databaseName}.qualitative_result_xwalk qxw on qxw.column_name = 'calculated_test_result' and qxw.qualitative_result = cvl.calculated_test_result
  where xw.target_domain_id ='Measurement' and xw.src_vocab_code = 'LOINC' and  cvl.order_date is not null and calculated_test_result is not null

""")


# COMMAND ----------


# test_code for including row_index before the genration of measurement_ids.
dfMeasurement2 = spark.sql(f"""
select distinct
cvl.lab_id,
md5(cvl.lab_id) as hashed_id,
-- cast(conv(substr(md5(concat(unit.target_concept_id, v.visit_occurrence_id,p.person_id,cvl.mapped_code,xw.source_concept_id,xw.target_concept_name,cvl.calculated_test_result)), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,
p.person_id,
xw.target_concept_id as measurement_concept_id,
cast( cvl.specimen_collection_date as date ) as measurement_date,
cast( cvl.specimen_collection_date as timestamp) as measurement_datetime,
cvl.specimen_collection_time as measurement_time,
32817 as measurement_type_concept_id,
    CAST(null as int) as operator_concept_id,
    CAST(null as float ) as value_as_number,
        CAST(qxw.concept_id as int) as value_as_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as float) as range_low,
        CAST(null as float) as range_high,
        CAST(null as int) AS provider_id,
 null as visit_occurrence_id,
 0 as  visit_detail_id,
cvl.mapped_code as measurement_source_value,
 xw.source_concept_id as measurement_source_concept_id,
 CAST(null as string) as unit_source_value,
 cvl.calculated_test_result as  value_source_value,
 xw.target_concept_name
from {databaseName}.nedss_test_output_file_mapped cvl
 join  {databaseName}.person p on cvl.Research_Patient_Key = p.person_id
 -- 23 June Updates: we are not linking the covid lab test records based on the test result dates and patient id.
 -- join  {databaseName}.visit_occurrence v on v.person_id = cvl.Research_Patient_Key and cvl.order_date = v.visit_start_date
 /** --left join crisp_sep2021.c2o_code_xwalk xw on cvl.observationID = xw.src_code and cvl.mapped_vocabulary_id = xw.src_vocab_code **/
 left join {databaseName}.c2o_code_xwalk xw on upper(trim(cvl.mapped_code)) = upper(trim(xw.src_code)) and cvl.mapped_vocabulary_id = xw.src_vocab_code
 left join {databaseName}.qualitative_result_xwalk qxw on qxw.column_name = 'calculated_test_result' and qxw.qualitative_result = cvl.calculated_test_result
  where xw.target_domain_id ='Measurement' and xw.src_vocab_code = 'LOINC' and  cvl.order_date is not null and calculated_test_result is not null

""")

# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

table_measurement2 = "temp2"
dfMeasurement2.createOrReplaceTempView(table_measurement2)

# COMMAND ----------

dfMeasurement2_final = spark.sql(f"""
select
distinct
cast(conv(substr(md5(concat_ws(".",lab_id, row_index,coalesce(person_id,""),COALESCE(measurement_date," "),coalesce(measurement_concept_id,""),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) as measurement_id,
*
from (
select *,
  row_number() over (
  partition by lab_id,
  measurement_concept_id,
  person_id,
  measurement_date,
  measurement_time,
  measurement_datetime,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  measurement_source_value,
  unit_source_value,
  target_concept_name,
  value_source_value
  order by lab_id,
  measurement_concept_id,
  person_id,
  measurement_date,
  measurement_time,
  measurement_datetime,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  measurement_source_value,
  unit_source_value,
  target_concept_name,
  value_source_value) as row_index
from temp2)


-- select
-- distinct
-- cast(conv(substr(md5(concat(lab_id, coalesce(measurement_concept_id,""),coalesce(person_id,""),COALESCE(measurement_time," "),COALESCE(measurement_datetime," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(measurement_source_concept_id," "),COALESCE(measurement_source_value," "),COALESCE(unit_source_value," "),COALESCE(target_concept_name," "),COALESCE(value_source_value," "))), 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id,

-- -- substr(md5(concat_ws('.',COALESCE(lab_id," "), COALESCE(person_id," "),COALESCE(measurement_concept_id," "),COALESCE(measurement_datetime," "),COALESCE(measurement_time," "),COALESCE(value_as_number," "),COALESCE(value_as_concept_id," "),COALESCE(unit_concept_id," "),COALESCE(visit_occurrence_id," "), COALESCE(measurement_source_value," "),COALESCE(measurement_source_concept_id," "),COALESCE(unit_source_value," "),COALESCE(value_source_value," "),COALESCE(target_concept_name," "))), 1, 50 ) as measurement_id,
-- *
-- from temp2

""")


# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

table_measurement2 = "view_measurement2"
dfMeasurement2_final.createOrReplaceTempView(table_measurement2)

# COMMAND ----------

# %sql
# select * from view_measurement2 limit 5

# COMMAND ----------

# %sql
# select * from view_measurement1 limit 5

# COMMAND ----------



# COMMAND ----------

#### measurement table
####1.create measurement table
####2.build the data in dataframe
######   created the Measurement table dataframe- we need to union both labs and cvlab
######3. persist the data
dfMeasurement= spark.sql("""
select distinct
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name
  from view_measurement1
union
select distinct
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name
  from view_measurement2
""")


# COMMAND ----------

##defMeasurement contains merged data

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tbl_measurement = "viewMeasurement"
dfMeasurement.createOrReplaceTempView(tbl_measurement)

# COMMAND ----------

  dfMeasurementMerged = spark.sql("""
  select distinct
  *,
  row_number() over (
  partition by
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name
  order by measurement_id
  ) as row_index
  from viewMeasurement
  """)

# COMMAND ----------

dfMeasurementMerged.distinct()


# COMMAND ----------

##defMeasurement contains merged data

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tbl_measurementMerged = "viewMeasurementMerged"
dfMeasurementMerged.createOrReplaceTempView(tbl_measurementMerged)

# COMMAND ----------

##dfFinal = spark.sql("""
##select
##m.*,
##cast(conv(substr(md5(concat(m.measurement_id, visit_occurrence_id)), 1, 15), 16, 10) as bigint) & 2251799813685247  as measurement_id2
##from viewMeasurementMerged m
##""")
### if dup is found then need to resolve the dups -- viewMeasurementMerged
###  ------where measurement_id not in (504875493283409, 1150148851047379 )
dfFinal = spark.sql("""
select distinct
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_time,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_number,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  target_concept_name
  from
  viewMeasurementMerged
  """)


# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tbl_final = "viewFinalMeasurement"
dfFinal.createOrReplaceTempView(tbl_final)

# COMMAND ----------

#######persist transformed data into measurement domain

tablename = "measurement"

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
dfFinal.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yc_test.Measurement

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct measurement_id) from yc_test.Measurement

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yc_test.Measurement where measurement_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from yc_test.measurement where measurement_id not in (select distinct measurement_id from yc_test.measurement)
# MAGIC
# MAGIC SELECT measurement_id, COUNT(*)
# MAGIC FROM crisp_omop.measurement
# MAGIC GROUP BY measurement_id
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct person_id) from yc_test.measurement m
# MAGIC where m.measurement_concept_id in (757685,723476,706173,36661375,715272,586526,757678,36661376,586528,706161,706165,715260,706160,706158,706163,706177,706170,36031861,723459,723478,586515,706175,723474,36032419,723477,36661378)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct m.person_id) from yc_test.measurement m
# MAGIC join yc_test.visit_occurrence v on m.visit_occurrence_id = v.visit_occurrence_id and m.measurement_concept_id in (757685,723476,706173,36661375,715272,586526,757678,36661376,586528,706161,706165,715260,706160,706158,706163,706177,706170,36031861,723459,723478,586515,706175,723474,36032419,723477,36661378)

# COMMAND ----------
