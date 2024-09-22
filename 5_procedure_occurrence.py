# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
##dbutils.fs.ls('/mnt/crisp-covid/sftp/JHU_Full_Load_09_20_2021_Parquet/')

dbutils.widgets.removeAll()
dbutils.widgets.text("parquetFilePath", "/mnt/crisp-covid/sftp/JHU_Full_Load_09_08_2022_Parquet/", "CRISP parquet file path")
dbutils.widgets.text("databaseName", "crisp_08sept2022", "CRISP database name")
dbutils.widgets.text("omop vocabulary files", "omop_vocab", "OMOP vocabulary database name")
dbutils.widgets.text("previousDatabaseName", "crisp_sep2022", "Previous Database name")
path=dbutils.widgets.get("parquetFilePath")
databaseName=dbutils.widgets.get("databaseName")
previousDatabaseName=dbutils.widgets.get("previousDatabaseName")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

### persist the visit_occurrence data
### diag_id = 30487155-90000024-1-J189-Inpatient Visit Diagnosis/hashed_id= c2788efb390845b04fc6d5757a004723/condition_occurrence_id = 2120887928128603
dfProcedure_key_lookup = spark.sql(f"""
select
   proc_id,
   md5(proc_id) as hashed_id,
   cast(conv(substr(md5(proc_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id,
   sequence,
   0 as pk_collison_bit
   from {databaseName}.viewprocedure
""")

# COMMAND ----------

#### create the condition_key_lookup table to check for pk collision
#    proc_id,
#   hashed_id	string,
#   procedure_occurrence_id long,
#   sequence int,
#   pk_collision_bit int
tablename="procedure_key_lookup"
path="crisp-covid/warehouse/"+databaseName

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  proc_id string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

# COMMAND ----------

tablename = "procedure_key_lookup"
# Persist the condition key lookup table
# persist the data to the table for later access.
##dfProcedure_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/procedure_key_lookup")
dfProcedure_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

## build df with source data
dfProcedure = spark.sql(f"""
select distinct
  cast(conv(substr(md5(pr.proc_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id,
  p.person_id,
  xw.target_concept_id as procedure_concept_id,
  pr.proc_date as procedure_date,
  pr.proc_datetime as procedure_datetime,
  32817 as procedure_type_concept_id,
   CAST(null as int) as modifier_concept_id,
  CAST(null as int) quantity,
  CAST(null as int) AS provider_id,
  v.visit_occurrence_id,
  cast(null as int) visit_detail_id,
  pr.procedureType as procedure_source_value,
  xw.source_concept_id as procedure_source_concept_id,
  CAST(null as string) modifier_source_value,
  xw.target_concept_name
  from {databaseName}.viewprocedure pr
  join {databaseName}.person p on pr.Research_Patient_Key = p.person_id
  join {databaseName}.visit_occurrence v on v.visit_occurrence_id = pr.VisitsID_Reference_Key and v.person_id = pr.Research_Patient_Key
  left join {databaseName}.c2o_code_xwalk xw on pr.procedureType = xw.src_code and pr.mapped_vocabulary_id = xw.src_vocab_code
  where xw.target_domain_id ='Procedure' and xw.src_vocab_code = pr.mapped_vocabulary_id and VisitsID_Reference_Key is not null and pr.procedureType is not null

  union

  select distinct
  cast(conv(substr(md5(d.diag_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id,
  p.person_id,
  xw.target_concept_id as procedure_concept_id,
  v.visit_start_date as procedure_date,
  v.visit_start_datetime as procedure_datetime,
  32817 as procedure_type_concept_id,
  CAST(null as int) as modifier_concept_id,
  CAST(null as int) quantity,
  CAST(null as int) AS provider_id,
  v.visit_occurrence_id as visit_occurrence_id,
  cast(null as int) visit_detail_id,
  concat( d.dx_code, '|', xw.source_code_description) as procedure_source_value,
  xw.source_concept_id as procedure_source_concept_id,
  CAST(null as string) modifier_source_value,
  xw.target_concept_name
from {databaseName}.viewdiagnosis d
  join {databaseName}.person p on d.Research_Patient_Key = p.person_id
  join {databaseName}.visit_occurrence v on v.visit_occurrence_id = d.VisitsID_Reference_Key and v.person_id = d.Research_Patient_Key
  left join {databaseName}.c2o_code_xwalk xw on d.dx_code = xw.src_code and  xw.src_vocab_code = 'ICD10CM'
  where xw.target_domain_id ='Procedure' and xw.src_vocab_code = 'ICD10CM' and d.VisitsID_Reference_Key is not null and d.dx_code is not null
""")

# COMMAND ----------

# Create a view or table to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tbl_procedure = "view_procedure_occurrence"
dfProcedure.createOrReplaceTempView(tbl_procedure)

# COMMAND ----------

dfProcedure.count()
###1,353,540
###1,590,434
### sept2021: 1,992,331

# COMMAND ----------

## create procedure table
####1.create procedure_occurrence table so that we can save the data for dqd
####

path="crisp-covid/warehouse/"+databaseName
tablename="procedure_occurrence"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  procedure_occurrence_id bigint,
  person_id	int,
  procedure_concept_id int,
  procedure_date date,
  procedure_datetime timestamp,
  procedure_type_concept_id int,
  modifier_concept_id int,
  quantity int,
  provider_id int,
  visit_occurrence_id int,
  visit_detail_id int,
  procedure_source_value string,
  procedure_source_concept_id int,
  modifier_source_value	string,
  target_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")


# COMMAND ----------

#######persist transformed data into measurement domain

tablename="procedure_occurrence"
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
##dfProcedure.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/procedure_occurrence")
dfProcedure.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

dfDrug = spark.sql(f"""
  select distinct
  cast(conv(substr(md5(pr.proc_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id,
  p.person_id,
  xw.target_concept_id as drug_concept_id,
  pr.proc_date as drug_exposure_start_date,
  cast(pr.proc_date as timestamp) as drug_exposure_start_datetime,
  cast(null as date) as drug_exposure_end_date,
  cast(null as timestamp) as drug_exposure_end_datetime,
  cast(null as date ) as verbatim_end_date,
  32817 as drug_type_concept_id,
  cast( null as string) as stop_reason,
  cast( null as int) as refills,
  cast( null as float) as quantity,
  cast( null as int) as days_supply,
  cast( null as string) as sig,
  0 as route_concept_id,
  cast( null as string) as lot_number,
  cast( null as int) as provider_id,
  v.visit_occurrence_id,
   cast( null as int) as visit_detail_id,
  concat( pr.procedureType, '|', xw.source_code_description ) as drug_source_value,
  xw.source_concept_id as drug_source_concept_id,
  cast( null as string ) as route_source_value,
  cast( null as string ) as dose_unit_source_value,
  xw.target_concept_name
from {databaseName}.viewprocedure pr
  join {databaseName}.c2o_code_xwalk xw on pr.procedureType = xw.src_code and pr.mapped_vocabulary_id = xw.src_vocab_code
  join {databaseName}.person p on pr.Research_Patient_Key = p.person_id
  join {databaseName}.visit_occurrence v on v.visit_occurrence_id = pr.VisitsID_Reference_Key and v.person_id = pr.Research_Patient_Key
  where xw.target_domain_id ='Drug' and xw.src_vocab_code = pr.mapped_vocabulary_id and VisitsID_Reference_Key is not null and pr.procedureType is not null

  union

  select distinct
  cast(conv(substr(md5(vc.vacc_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id,
  p.person_id,
  xw.target_concept_id as drug_concept_id,
  vc.vacc_date as drug_exposure_start_date,
  vc.vacc_date as drug_exposure_start_datetime,
  cast(null as date) as drug_exposure_end_date,
  cast(null as timestamp) as drug_exposure_end_datetime,
  cast(null as date ) as verbatim_end_date,
  32817 as drug_type_concept_id,
  cast( null as string) as stop_reason,
  cast( null as int) as refills,
  cast( null as float) as quantity,
  cast( null as int) as days_supply,
  cast( null as string) as sig,
  4302612 as route_concept_id,---injectable concept_id/Injectable Suspension 4302612
  vaccine_lot_id as lot_number,
  cast( null as int) as provider_id,
  cast( null as int) as visit_occurrence_id,
   cast( null as int) as visit_detail_id,
  xw.manufacturer_name as drug_source_value,
  xw.target_concept_id as drug_source_concept_id,
  cast( null as string ) as route_source_value,
  cast( null as string ) as dose_unit_source_value,
  xw.manufacturer_name as target_concept_name
from {databaseName}.viewvaccine vc
join {databaseName}.person p on vc.Research_Patient_Key = p.person_id
left join {databaseName}.vaccination_xwalk xw on vc.manufacturer = xw.manufacturer

where xw.manufacturer is NOT null and length(trim(xw.manufacturer)) >0
""")

# COMMAND ----------

dfDrug.count()
###2,114,477

# COMMAND ----------

  dfDrugVacc = spark.sql(f"""
  select distinct
  cast(conv(substr(md5(vc.vacc_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id,
  p.person_id,
  xw.target_concept_id as drug_concept_id,
  vc.vacc_date as drug_exposure_start_date,
  vc.vacc_date as drug_exposure_start_datetime,
  cast(null as date) as drug_exposure_end_date,
  cast(null as timestamp) as drug_exposure_end_datetime,
  cast(null as date ) as verbatim_end_date,
  32817 as drug_type_concept_id,
  cast( null as string) as stop_reason,
  cast( null as int) as refills,
  cast( null as float) as quantity,
  cast( null as int) as days_supply,
  cast( null as string) as sig,
  4302612 as route_concept_id,---injectable concept_id/Injectable Suspension 4302612
  vaccine_lot_id as lot_number,
  cast( null as int) as provider_id,
  cast( null as int) as visit_occurrence_id,
   cast( null as int) as visit_detail_id,
  xw.manufacturer_name as drug_source_value,
  xw.target_concept_id as drug_source_concept_id,
  cast( null as string ) as route_source_value,
  cast( null as string ) as dose_unit_source_value,
  xw.manufacturer_name as target_concept_name
from {databaseName}.viewvaccine vc
join {databaseName}.person p on vc.Research_Patient_Key = p.person_id
left  join {databaseName}.vaccination_xwalk xw on vc.manufacturer = xw.manufacturer
  where xw.manufacturer is NOT null and length(trim(xw.manufacturer)) >0
""")

# COMMAND ----------

######dfDrug has merged vaccination data
dfDrugVacc.count()
###2,109,586
###sep2021: 4,235,879

# COMMAND ----------

# Create a view in order to check for duplicates
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tbl_drug = "viewDrug"
dfDrug.createOrReplaceTempView(tbl_drug)

# COMMAND ----------

 ####1.create drug_exposure table
####

path="crisp-covid/warehouse/"+databaseName
tablename="drug_exposure"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  drug_exposure_id bigint,
  person_id	int,
  drug_concept_id int,
  drug_exposure_start_date date,
  drug_exposure_start_datetime timestamp,
  drug_exposure_end_date date,
  drug_exposure_end_datetime timestamp,
  verbatim_end_date	date,
  drug_type_concept_id int,
  stop_reason string,
  refills int,
  quantity float,
  days_supply int,
  sig string,
  route_concept_id int,
  lot_number string,
  provider_id int,
  visit_occurrence_id int,
  visit_detail_id int,
  drug_source_value string,
  drug_source_concept_id int,
  route_source_value string,
  dose_unit_source_value string,
  target_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------

#######persist transformed data into drug exposure domain domain
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

tablename="drug_exposure"

###dfDrug.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/drug_exposure")
dfDrug.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---select * from crisp_omop_3.qualitative_result_crosswalk2
# MAGIC select count(drug_exposure_id) from ${databaseName}.drug_exposure
# MAGIC

# COMMAND ----------



# COMMAND ----------
