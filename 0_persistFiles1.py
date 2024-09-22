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

spark.sql(f"create or replace table {databaseName}.Patient_File DEEP CLONE parquet.`{path}/Patient_File`")
spark.sql(f"OPTIMIZE {databaseName}.Patient_File")
dfPatient = spark.table(f"{databaseName}.Patient_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.PatientVisits_output_File DEEP CLONE parquet.`{path}/PatientVisits_output_File`")
spark.sql(f"OPTIMIZE {databaseName}.PatientVisits_output_File")
dfEnc = spark.table(f"{databaseName}.PatientVisits_output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.Missing_gender_File DEEP CLONE parquet.`{path}/Missing_gender_File`")
spark.sql(f"OPTIMIZE {databaseName}.Missing_gender_File")
dfGender = spark.table(f"{databaseName}.Missing_gender_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.PatientVisits_output_File DEEP CLONE parquet.`{path}/PatientVisits_output_File`")
spark.sql(f"OPTIMIZE {databaseName}.PatientVisits_output_File")
dfVisit = spark.table(f"{databaseName}.PatientVisits_output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.PatientDiagnosisVisits_output_File DEEP CLONE parquet.`{path}/PatientDiagnosisVisits_output_File`")
spark.sql(f"OPTIMIZE {databaseName}.PatientDiagnosisVisits_output_File")
dfDiagnosis = spark.table(f"{databaseName}.PatientDiagnosisVisits_output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.PatientProcedureVisits_output_File DEEP CLONE parquet.`{path}/PatientProcedureVisits_output_File`")
spark.sql(f"OPTIMIZE {databaseName}.PatientProcedureVisits_output_File")
dfProc = spark.table(f"{databaseName}.PatientProcedureVisits_output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.All_Lab_observations_output_File DEEP CLONE parquet.`{path}/All_Lab_observations_output_File`")
spark.sql(f"OPTIMIZE {databaseName}.All_Lab_observations_output_File")
dfLabs = spark.table(f"{databaseName}.All_Lab_observations_output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.NEDSS_Test_Output_File DEEP CLONE parquet.`{path}/NEDSS_Test_Output_File`")
spark.sql(f"OPTIMIZE {databaseName}.NEDSS_Test_Output_File")
dfCVLabs = spark.table(f"{databaseName}.NEDSS_Test_Output_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.NEDSS_Test_Output_unduplicated_File DEEP CLONE parquet.`{path}/NEDSS_Test_Output_unduplicated_File`")
spark.sql(f"OPTIMIZE {databaseName}.NEDSS_Test_Output_unduplicated_File")
dfCVLabsUndup = spark.table(f"{databaseName}.NEDSS_Test_Output_unduplicated_File")

# COMMAND ----------

spark.sql(f"create or replace table {databaseName}.Covid19_Vaccinations_File DEEP CLONE parquet.`{path}/Covid19_Vaccinations_File`")
spark.sql(f"OPTIMIZE {databaseName}.Covid19_Vaccinations_File")
dfVacc = spark.table(f"{databaseName}.Covid19_Vaccinations_File")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

path=dbutils.widgets.get("parquetFilePath")

patientFile = path+'Patient_File'
encFile = path+'PatientVisits_output_File'
genderFile = path+'Missing_gender_File'
diagnosisFile = path+'PatientDiagnosisVisits_output_File'
procFile = path+'PatientProcedureVisits_output_File'
labFile = path+'All_Lab_observations_output_File'
cvLabFile = path+'NEDSS_Test_Output_File'
cvLabUndupFile = path+'NEDSS_Test_Output_unduplicated_File'
vaccFile = path+'Covid19_Vaccinations_File'
visitFile= path+'PatientVisits_output_File'

# COMMAND ----------

# spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# #Read the contents of the parquet file into a dataframe
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

dfPatient= dfPatient.drop_duplicates()
patCount = dfPatient.select('Research_Patient_Key').distinct().count()
patCount
##6,103,600

# COMMAND ----------

##dfEnc = dfEnc.drop_duplicates()
encCount = dfEnc.select('Research_Patient_Key').count()
encCount
##18,110,514

# COMMAND ----------

visitCount = dfVisit.select('Research_Patient_Key', 'VisitsID_Reference_Key').distinct().count()
##18,110,514
##dfVisit.createOrReplaceTempView('viewVisit')
visitCount

# COMMAND ----------

genderCount = dfGender.select('Research_Patient_Key').distinct().count()
genderCount
#67,010 distinct with research patient key

# COMMAND ----------

dfDiagnosis = dfDiagnosis.drop_duplicates()
diagCount = dfDiagnosis.select('Research_Patient_Key', 'VisitsID_Reference_Key').count()

##135,764,905 before dups
##98,652,340- patient/visit key count 59,420,821

##display(dfDiagnosis)
diagCount
##98,652,340

# COMMAND ----------

dfProc = dfProc.drop_duplicates()
procCount = dfProc.distinct().count()
##96,116,022 - patient key 86,938,771
procCountPatientVisit = dfProc.select('Research_Patient_Key', 'VisitsID_Reference_Key').count()

procCountPatientVisit
##96,116,022

# COMMAND ----------

procCount

# COMMAND ----------

dfLabs = dfLabs.drop_duplicates()
#labCount = dfLabs.select('Research_Patient_Key','VisitsID_Reference_Key').distinct().count()
labCount = dfLabs.distinct().count()
labCount
##1,116,920,526 count labs after de-dup

# COMMAND ----------

dfCVLabs = dfCVLabs.drop_duplicates()
#cvLabCount = dfCVLabs.select('Research_Patient_Key','SourceCaretAccessionNumber','OrderID').distinct().count()
cvLabCount = dfCVLabs.distinct().count()
cvLabCount
##26,694,663


# COMMAND ----------

dfVacc= dfVacc.drop_duplicates()
#vacCount = dfVacc.select('research_patient_key', 'vaccination_date').distinct().count()
vacCount = dfVacc.distinct().count()
###display(dfVacc)
## research_patient_key, vaccination_date, manufacturer, vaccine_lot_id, ADMINISTRATION_ORG_NAME, Administration_Org_Facility_System_Name
###6,340,629
###6,337,099 de-dup patient key and vaccination date
###6,340,629 raw count after de-dup
vacCount


# COMMAND ----------

### prep parsed data for views
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import sha2, concat_ws

dfPatient = dfPatient.withColumn("loc_id0", F.concat_ws("-", "patient_zip", "census_block", "census_geoid"))
display(dfPatient)

# COMMAND ----------

## create views
# dfPatient.createOrReplaceTempView('viewPatient')
# dfGender.createOrReplaceTempView('viewGender')
# dfEnc.createOrReplaceTempView('viewEncounter')
# dfVisit.createOrReplaceTempView('viewVisit')
# dfDiagnosis.createOrReplaceTempView('viewDiagnosis')
# dfProc.createOrReplaceTempView('viewProcedure')
# dfLabs.createOrReplaceTempView('viewLabs')
# dfCVLabs.createOrReplaceTempView('viewCVLabs')
# dfVacc.createOrReplaceTempView('viewVaccine')


dfPatient.distinct()
dfVisit.distinct()
dfDiagnosis.distinct()
dfProc.distinct()
dfLabs.distinct()
dfCVLabs.distinct()

dfPatient = dfPatient.dropDuplicates()
dfVisit = dfVisit.dropDuplicates()
dfDiagnosis = dfDiagnosis.dropDuplicates()
dfProc = dfProc.dropDuplicates()
dfLabs = dfLabs.dropDuplicates()
dfCVLabs = dfCVLabs.dropDuplicates()
dfVacc = dfVacc.dropDuplicates()

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

# COMMAND ----------

## From 3visit_occurrence

dfPatient = dfPatient.withColumn("loc_id0", F.concat_ws("-", dfPatient["Research_Patient_Key"].cast(StringType()), "patient_zip", "census_block", "census_geoid"))
dfPatient = dfPatient.withColumn("loc_id1", F.concat_ws("-", "patient_zip", "census_block", "census_geoid"))


dfEnc = dfEnc.withColumn( "admitdt_date", to_date(dfEnc['admitdt'], 'MM/dd/yyyy'))
dfEnc = dfEnc.withColumn( "visit_start_date", to_date(dfEnc['admitdt'], 'MM/dd/yyyy'))
dfEnc = dfEnc.withColumn( "visit_start_datetime", to_timestamp(dfEnc['admitdt'], 'MM/dd/yyyy'))
dfEnc = dfEnc.withColumn( "visit_end_date", to_date(dfEnc['dischargedt'], 'MM/dd/yyyy'))
dfEnc = dfEnc.withColumn( "visit_end_datetime", to_timestamp(dfEnc['dischargedt'], 'MM/dd/yyyy'))

# COMMAND ----------

## From 5_measurement

#Read the contents of the patient parquet file into a dataframe
# dfPatient = dfPatient.withColumn("loc_id0", F.concat_ws("-", "patient_zip", "census_block", "census_geoid"))

dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))

dfVisit = dfVisit.withColumn( "admitdt_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
dfVisit = dfVisit.withColumn( "visit_start_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
dfVisit = dfVisit.withColumn( "visit_start_datetime", to_timestamp(dfVisit['admitdt'], 'MM/dd/yyyy'))
dfVisit = dfVisit.withColumn( "visit_end_date", to_date(dfVisit['dischargedt'], 'MM/dd/yyyy'))
dfVisit = dfVisit.withColumn( "visit_end_datetime", to_timestamp(dfVisit['dischargedt'], 'MM/dd/yyyy'))

dfLabs.dropDuplicates()
###incoming data format: 2020-06-24T17:20:51.000+0000 incoming data pattern
ts_pattern ="YYYY-MM-DD HH:mm:ss.SSS"
dfLabs = dfLabs.withColumn("mapped_vocabulary_id", lit("LOINC"))
dfLabs = dfLabs.withColumn("lab_date", to_date(dfLabs['DateIssued'], 'YYYY-MM-DD'))
dfLabs = dfLabs.withColumn("time", date_format(dfLabs['DateIssued'], 'HH:mm:ss'))
dfLabs = dfLabs.withColumn("lab_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "partitionDate", "ObservationNumber", "source_Reference_Key", "Parent_ID_Identifer", "Parent_description", "LoincCode", "Status", "AbnormalFlag", "DateIssued","Method", "ValueConcept", "ValueDateTime","ValueString", "ValueQuantity", "ValueQuantityUnits", "Loinc_Desc"))

dfCVLabs.dropDuplicates()
dfCVLabs=dfCVLabs.withColumn("mapped_vocabulary_id", lit("LOINC"))
dfCVLabs = dfCVLabs.withColumn("lab_id", F.concat_ws("-", "Research_Patient_Key", "SourceCaretAccessionNumber", "OrderID", "observationID", "ObservationName", "processed_datetime", "order_date", "specimen_collection_date", "result_date", "OBX_5_1_Result", "OBX_5_2_Result", "calculated_test_result", "ordering_physician", "ordering_facility" ))
##### use specimen_collection_date to extract measurement time
dfCVLabs = dfCVLabs.withColumn("specimen_collection_time", date_format(dfCVLabs['specimen_collection_date'], 'HH:mm:ss'))
###dfCVLabs  order_date pattern = 2020-05-27

# COMMAND ----------

## From 4c2o_xwalk

# dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
# dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
# dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))


# COMMAND ----------

## From 5_condition_occurrence
# dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
# dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
# dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))

# dfVisit = dfVisit.withColumn( "admitdt_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_datetime", to_timestamp(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_date", to_date(dfVisit['dischargedt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_datetime", to_timestamp(dfVisit['dischargedt'], 'MM/dd/yyyy'))



dfDiagnosis = dfDiagnosis.withColumn("diag_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "Sequence", "Diagnosis", "data_source", "dx_code"))
dfDiagnosis.distinct()

# COMMAND ----------

## From 5_observation
# dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
# dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
# dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))

# dfVisit = dfVisit.withColumn( "admitdt_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_date", to_date(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_start_datetime", to_timestamp(dfVisit['admitdt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_date", to_date(dfVisit['dischargedt'], 'MM/dd/yyyy'))
# dfVisit = dfVisit.withColumn( "visit_end_datetime", to_timestamp(dfVisit['dischargedt'], 'MM/dd/yyyy'))


# dfDiagnosis = dfDiagnosis.withColumn("diag_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "Sequence", "Diagnosis", "data_source", "dx_code"))
# dfDiagnosis.distinct()

# COMMAND ----------

## 5_procedure_occurrence
# from pyspark.sql.functions import col, when, lit, udf, to_date, to_timestamp
# from pyspark.sql.functions import length
# from pyspark.sql import functions as F
# from pyspark.sql import Window
# from pyspark.sql.functions import sha2, concat_ws
# from datetime import datetime
# from pyspark.sql import types as T
# from pyspark.sql.types import DateType
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import date_format

# dfDiagnosis = dfDiagnosis.withColumn("dx_code", F.concat(dfDiagnosis.Diagnosis.substr(1,3), lit('.'), dfDiagnosis.Diagnosis.substr(4, 10)))
# dfDiagnosis = dfDiagnosis.withColumn("mapped_vocabulary_id", lit("ICD10CM"))
# dfDiagnosis = dfDiagnosis.withColumn("diag_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "Sequence", "Diagnosis", "data_source"))

dfProc = dfProc.withColumn("mapped_vocabulary_id", lit("ICD10PCS"))
dfProc = dfProc.withColumn("proc_id", F.concat_ws("-", "Research_Patient_Key", "VisitsID_Reference_Key", "Sequence", "procedureType", "proceduredate", "data_source"))
dfProc = dfProc.withColumn("proc_date", to_date(dfProc['proceduredate'], 'MM/dd/yyyy'))
dfProc = dfProc.withColumn("proc_datetime", to_timestamp(dfProc['proceduredate'], 'MM/dd/yyyy'))

###VaccinationData- in 02/12/2021
dfVacc = dfVacc.withColumn("vacc_id", F.concat_ws("-", "Research_Patient_Key", "vaccination_date","manufacturer","vaccine_lot_id","ADMINISTERING_ORG_NAME","Administering_Org_Facility_System_Name"))
dfVacc = dfVacc.withColumn("vacc_date", to_date(dfVacc['vaccination_date'], 'MM/dd/yyyy'))

# COMMAND ----------

## From 5_vaccination


# dfVacc = dfVacc.withColumn("vacc_id", F.concat_ws("-", "Research_Patient_Key","vaccination_date","manufacturer","vaccine_lot_id","ADMINISTERING_ORG_NAME","Administering_Org_Facility_System_Name"))
# dfVacc = dfVacc.withColumn("vacc_date", to_date(dfVacc['vaccination_date'], 'MM/dd/yyyy'))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables
# MAGIC

# COMMAND ----------

path="crisp-covid/warehouse/"+databaseName


# COMMAND ----------

tablename="viewPatient"
dfPatient.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewEncounter"
dfEnc.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewGender"
dfGender.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewVisit"
dfVisit.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewDiagnosis"
dfDiagnosis.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewProcedure"
dfProc.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewLabs"
dfLabs.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewCVLabs"
dfCVLabs.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewCVLabsUndup"
dfCVLabsUndup.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

tablename="viewVaccine"
dfVacc.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

tablename = "dfPatient"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
 lab_name string,
 corrected_code string,
 run_timestamp timestamp
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------



# COMMAND ----------

# observation ID mapped and add an addtional column for it (mapped_loinc_code)

# COMMAND ----------

tablename='CVLabs'
dfCVLabs.selectExpr("*").write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------
