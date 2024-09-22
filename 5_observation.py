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

#########################
## update the following for this notebook

path="crisp-covid/warehouse/"+databaseName
tablename= "observation"
location= "/mnt/{path}/{tablename}"


# COMMAND ----------

### persist condition dataset

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

dfObservationDomainMap = spark.sql(f"""
select distinct
  cast(conv(substr(md5(diag_id), 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id,
  p.person_id,
  xw.target_concept_id as observation_concept_id,
  v.visit_start_date as observation_date,
  v.visit_start_datetime as observation_datetime,
  32817 as observation_type_concept_id,
  cast(null as int) as value_as_number,
  null as value_as_string,
  xw.source_concept_id as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  null as provider_id,
  v.visit_occurrence_id,
  cast(null as int) as visit_detail_id,
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

dfObservationDomainMap.count()
##15,637,709

# COMMAND ----------

dfObservationDomainMap = dfObservationDomainMap.drop_duplicates()

# COMMAND ----------

dfObservationDomainMap.count()
##15,637,709

# COMMAND ----------

### create the observation table

path="crisp-covid/warehouse/"+databaseName
tablename="observation_domain_map"


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

tablename = "observation_domain_map"
# persist the data to the table for later access.
###dfObservation.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-###covid/crisp_sep2021/observation")
dfObservationDomainMap.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

### persist the observation with pk key conflict
### rownum > 1 pk with conflict - rownum 1 is ok, but all rownum>1 must be saved and resolved.
###dfobs key conflict
dfObs_key_conflict = spark.sql(f"""
select * from
(
    select observation_id, observation_concept_id, row_number() over ( partition by observation_id order by observation_id) as rownum
    from {databaseName}.observation_domain_map
) x
where x.rownum>1

""")

# COMMAND ----------

#### create the observation primary key conflict table
path="crisp-covid/warehouse/"+databaseName
tablename="observation_key_conflict"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
  observation_id long
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")

# COMMAND ----------

##--save the ids in conflict so we can resolve it. crisp_sep2021.observation_key_conflict
# persist the data to the table so we can join to resolve the ids.

tablename="observation_key_conflict"
##dfCondition_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/condition_key_lookup")
dfObs_key_conflict.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

### persist condition dataset

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

dfObservationUpdate = spark.sql(f"""
select
shiftleft(pk.observation_id, 2) + pk.rownum as global_id,
coalesce( (shiftleft(pk.observation_id, 2) + pk.rownum), o.observation_id )  as observation_id,
  o.person_id,
  o.observation_concept_id,
  o.observation_date,
  o.observation_datetime,
  o.observation_type_concept_id,
  o.value_as_number,
  o.value_as_string,
  o.value_as_concept_id,
  o.qualifier_concept_id,
  o.unit_concept_id,
  o.provider_id,
  o.visit_occurrence_id,
  o.visit_detail_id,
  o.observation_source_value,
  o.observation_source_concept_id,
  o.unit_source_value,
  o.qualifier_source_value,
  o.observation_concept_name
  from {databaseName}.observation_domain_map o
  join {databaseName}.person p on p.person_id = o.person_id
  left join {databaseName}.observation_key_conflict pk on o.observation_id = pk.observation_id and o.observation_concept_id = pk.observation_concept_id

""")

# COMMAND ----------

## save the updated observation with pk conflict resolution

tablename="observation"
##dfCondition_key_lookup.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_sep2021/condition_key_lookup")
dfObservationUpdate.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${databaseName}.observation
# MAGIC -- total observation: 15,637,709

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${databaseName}.observation

# COMMAND ----------



# COMMAND ----------
