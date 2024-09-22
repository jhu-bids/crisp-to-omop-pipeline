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

#########################
## update the following for this notebook

path="crisp-covid/warehouse/"+databaseName
tablename= "temp"
location= "/mnt/{path}/{tablename}"


# COMMAND ----------

### persist the visit_occurrence data
### diag_id = 30487155-90000024-1-J189-Inpatient Visit Diagnosis/hashed_id= c2788efb390845b04fc6d5757a004723/condition_occurrence_id = 2120887928128603
dfCondition_key_lookup = spark.sql(f"""
select
   diag_id,
   md5(diag_id) as hashed_id,
   cast(conv(substr(md5(diag_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id,
   sequence,
   0 as pk_collison_bit
   from {databaseName}.viewdiagnosis d
""")

# COMMAND ----------

#### create the condition_key_lookup table to check for pk collision
#   diag_id string,
#   hashed_id	string,
#   condition_occurrence_id long,
#   pk_collision_bit int

tablename="condition_key_lookup"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  diag_id string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

# COMMAND ----------

# Persist the condition key lookup table
# persist the data to the table for later access.

tablename="condition_key_lookup"
##dfCondition_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/condition_key_lookup")
dfCondition_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

### 1. create table, 2 create df with source data 3. persist data
### create condition_occurrence table

path="crisp-covid/warehouse/"+databaseName
tablename="condition_occurrence_domain_map"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  condition_occurrence_id bigint,
  person_id int,
  condition_concept_id int,
  condition_start_date date,
  condition_start_datetime timestamp,
  condition_end_date date,
  condition_end_datetime timestamp,
  condition_type_concept_id int,
  stop_reason string,
  provider_id int,
  visit_occurrence_id int,
  visit_detail_id int,
  condition_source_value string,
  condition_source_concept_id int,
  condition_status_source_value string,
  condition_status_concept_id int,
  condition_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

# COMMAND ----------

# persist the visit_occurrence data
### diag_id = 30487155-90000024-1-J189-Inpatient Visit Diagnosis/hashed_id= c2788efb390845b04fc6d5757a004723/condition_occurrence_id = 2120887928128603
dfConditionDomainMap = spark.sql(f"""
  select
  cast(conv(substr(md5(diag_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id,
  p.person_id,
  target_concept_id AS condition_concept_id,
  visit_start_date as condition_start_date,
  visit_start_datetime as condition_start_datetime,
  visit_end_date as condition_end_date,
  visit_end_datetime as condition_end_datetime,
  32817 as condition_type_concept_id,
  cast(null as string) as stop_reason,
  cast(0 as int) as provider_id,
  visit_occurrence_id,
  0 as visit_detail_id,
  xw.source_code_description as condition_source_value,
  xw.source_concept_id as condition_source_concept_id,
  cast(null as string) as condition_status_source_value,
  0 as condition_status_concept_id,
  xw.target_concept_name as condition_concept_name
  from {databaseName}.viewdiagnosis d
  join  {databaseName}.person p on d.Research_Patient_Key = p.person_id
  join  {databaseName}.visit_occurrence v on v.visit_occurrence_id = d.VisitsID_Reference_Key and v.person_id = d.Research_Patient_Key
  left join {databaseName}.c2o_code_xwalk xw on d.dx_code = xw.src_code and d.mapped_vocabulary_id = xw.src_vocab_code
  where xw.target_domain_id ='Condition'
""")

# COMMAND ----------

dfConditionDomainMap.count()
## 46,586,723
## sept2021 : 46,586,723
## 46,579,767 preDomainMap

# COMMAND ----------

dfConditionDomainMap= dfConditionDomainMap.drop_duplicates()

# COMMAND ----------

dfConditionDomainMap.count()


# COMMAND ----------

### persist condition dataset
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
tablename = "condition_occurrence_domain_map"
# persist the data to the table for later access.
###(dfCondition.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-###covid/crisp_sep2021/condition_occurrence"))
dfConditionDomainMap.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------

### persist condition dataset

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

dfObservation = spark.sql(f"""
select distinct
  cast(conv(substr(md5(diag_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id,
  p.person_id,
  xw.target_concept_id as observation_concept_id,
  v.visit_start_date as observation_date,
  v.visit_start_datetime as observation_datetime,
  32817 as observation_type_concept_id,
  0 as value_as_number,
  null as value_as_string,
  xw.source_concept_id as value_as_concept_id,
  0 as qualifier_concept_id,
  0 as unit_concept_id,
  0 as provider_id,
  v.visit_occurrence_id,
  cast(0 as int) as visit_detail_id,
  xw.source_code_description as observation_source_value,
  xw.source_concept_id as observation_source_concept_id,
  cast(null as string) as unit_source_value,
  cast(null as string) as qualifier_source_value,
  xw.target_concept_name as observation_concept_name
  from {databaseName}.viewdiagnosis d
  join {databaseName}.c2o_code_xwalk xw on d.dx_code = xw.src_code and d.mapped_vocabulary_id = xw.src_vocab_code
  join {databaseName}.person p on d.Research_Patient_Key = p.person_id
  join {databaseName}.visit_occurrence v on v.visit_occurrence_id = d.VisitsID_Reference_Key and v.person_id = d.Research_Patient_Key
  where xw.target_domain_id ='Observation'
  """)

# COMMAND ----------

##15490526
dfObservation.count()
##before : 15,638,777

# COMMAND ----------

dfObservation= dfObservation.drop_duplicates()

# COMMAND ----------

dfObservation.count()
##after : 15,638,777
##sept2021: 15638777

# COMMAND ----------

### create the observation table

path="crisp-covid/warehouse/"+databaseName
tablename="observation"


spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  observation_id long,
  person_id	int,
  observation_concept_id int,
  observation_date date,
  observation_datetime timestamp,
  observation_type_concept_id int,
  value_as_number int,
  value_as_string string,
  value_as_concept_id int,
  qualifier_concept_id int,
  unit_concept_id int,
  provider_id int,
  visit_occurrence_id int,
  visit_detail_id int,
  observation_source_value string,
  observation_source_concept_id	int,
  unit_source_value	string,
  qualifier_source_value string,
  observation_concept_name string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------

### persist condition dataset
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

tablename = "observation"
# persist the data to the table for later access.
###dfObservation.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-###covid/crisp_sep2021/observation")
dfObservation.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")
