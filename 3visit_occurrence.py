# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')
dbutils.fs.ls('/mnt/crisp-covid/sftp/')
dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "yc_test", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

## some settings to set before reading parquet files
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite=true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("SET spark.databricks.delta.overwriteSchema=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")

path="crisp-covid/warehouse/"+databaseName

# COMMAND ----------

#Read the contents of the visit parquet file into a dataframe
#dfPatient = spark.read.parquet('/mnt/crisp-covid/sftp/JHU_Full_Load_04_26_Parquet/Patient_File')
#dfEnc = spark.read.parquet('/mnt/crisp-covid/sftp/JHU_Full_Load_04_26_Parquet/PatientVisits_output_masked_File')
# dfPatient = spark.read.parquet(patientFile)
# dfEnc = spark.read.parquet(encFile)

# ##display(dfPatient)
# display(dfEnc)
# dfEnc.count()
##dfPatient.count()
##5,523,842
##6,935,491 encounter
##10,927,267 - crisp_sep2021

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create visit type crosswalk table

# COMMAND ----------

#### create the visit type crosswalk table

path="crisp-covid/warehouse/"+databaseName
tablename="visit_xwalk"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {databaseName}.{tablename}(
PatientType string,
Data_Source string,
mapped_visit_concept_id int,
mapped_visit_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

####### generate the visit type cross walk table
dfvisit_xwalk = spark.sql(f"""
select distinct
PatientType, Data_Source,
case when PatientType ='Outpatient' THEN 9202
  when PatientType ='Chronic patient' THEN 9202
  when PatientType ='Chronic patient with ED' THEN 9202
  when PatientType ='Outpatient with ED' THEN 9202
  when PatientType = 'Inpatient' THEN 9201
  when PatientType = 'Inpatient with ED' THEN 9201
  when PatientType = 'Observation' THEN 581385
  else 0
end as mapped_visit_concept_id,
case when PatientType ='Outpatient' THEN 'Outpatient Visit'
  when PatientType ='Chronic patient' THEN 'Outpatient Visit'
  when PatientType ='Chronic patient with ED' THEN 'Outpatient Visit'
  when PatientType ='Outpatient with ED' THEN 'Outpatient Visit'
  when PatientType = 'Inpatient' THEN 'Inpatient Visit'
  when PatientType = 'Inpatient with ED' THEN 'Inpatient Visit'
  when PatientType = 'Observation' THEN 'Observation Room'
  else 0
end as mapped_visit_concept_name
from {databaseName}.viewEncounter
""")

# COMMAND ----------

# Persist the visit cross walktable created from above
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

tablename="visit_xwalk"

# persist the data to the table for later access.
##dfvisit_xwalk.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/visit_xwalk")
dfvisit_xwalk.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create visit_occurence

# COMMAND ----------

## visit_occurrence data
dfvisit = spark.sql(f"""
select distinct
VisitsID_Reference_Key as visit_occurrence_id,
p.person_id as person_id,
xw.mapped_visit_concept_id as visit_concept_id,
  e.visit_start_date,
  e.visit_start_datetime,
  e.visit_end_date,
  e.visit_end_datetime,
  32035 as visit_type_concept_id, /**** visit derived from EHR encounter record*****/
  0 as provider_id,
  c.care_site_id as care_site_id,
  e.PatientType as visit_source_value,
  mapped_visit_concept_id as visit_source_concept_id,
  0 as admitting_source_concept_id,
  e.Data_Source as admitting_source_value,
  0 as discharge_to_concept_id,
  null as discharge_to_source_value,
  0 as preceding_visit_occurrence_id,
  mapped_visit_concept_name as visit_concept_name,
  care_site_name
  from {databaseName}.viewEncounter e
  join {databaseName}.person p on e.Research_Patient_Key = p.person_id
  left join {databaseName}.visit_xwalk xw on e.PatientType = xw.PatientType
  left join {databaseName}.care_site c on c.care_site_source_value = e.sourceid
  """)


# COMMAND ----------

#### create the visit type crosswalk table

path="crisp-covid/warehouse/"+databaseName
tablename="visit_occurrence"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {databaseName}.{tablename}(
visit_occurrence_id	 int,
  person_id	int,
  visit_concept_id int,
  visit_start_date date,
  visit_start_datetime timestamp,
  visit_end_date date,
  visit_end_datetime timestamp,
  visit_type_concept_id	int,
  provider_id int,
  care_site_id int,
  visit_source_value string,
  visit_source_concept_id int,
  admitting_source_concept_id int,
  admitting_source_value string,
  discharge_to_concept_id int,
  discharge_to_source_value string,
  preceding_visit_occurrence_id	int,
  visit_concept_name string,
  care_site_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

##tblproperties(delta.autoOptimize.optimizeWrite=true)
##location "/mnt/crisp-covid/crisp_apr2021/visit_occurrence"

# COMMAND ----------

## Persist the visit occurrene data
###
##dfvisit.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/visit_occurrence")

tablename="visit_occurrence"
partitionBy="visit_concept_id"
###.partitionBy("visit_concept_id")
dfvisit.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

#############################################################################################

# COMMAND ----------

# %sql
# --Primary key checks
# select visit_occurrence_id, count(*)
# from crisp_08sept2022.visit_occurrence
# group by visit_occurrence_id
# having count(*) >1

# COMMAND ----------

# %sql
# select * from yc_test.viewvisit
# where VisitsID_Reference_Key = 97486108

# COMMAND ----------

# %sql
# select * from yc_test.viewEncounter
# where VisitsID_Reference_Key = 97486108


# COMMAND ----------

# %sql
# select * from yc_test.visit_occurrence
# where visit_occurrence_id = 97486108

# COMMAND ----------
