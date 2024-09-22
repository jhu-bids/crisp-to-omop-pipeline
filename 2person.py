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

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite=true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("SET spark.databricks.delta.overwriteSchema=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create person_with_dual_gender

# COMMAND ----------


tablename = "person_with_dual_gender"
path="crisp-covid/warehouse/"+databaseName

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
 person_id	   int
 )
 using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")

# COMMAND ----------

dfDupGenderPatid = spark.sql(f"""
select person_id from
(
select person_id, count(*) cnt
from
(
select distinct p.Research_Patient_Key as person_id,
case when g.Gender is null then p.Gender
else g.Gender
end as mapped_gender,
case when trim(p.gender) in ('Female', 'F', 'f', 'FEMALE', 'w') then 8532
when trim(p.gender) in ('M','m', 'Male', 'MALE') then 8507
when trim(g.gender) in ('Female', 'F', 'f', 'FEMALE', 'w') then 8532
when trim(g.gender) in ('M','m', 'Male', 'MALE') then 8507
else 0
end gender_concept_id
from {databaseName}.viewPatient p
left join {databaseName}.viewGender g on p.Research_Patient_Key = g.Research_Patient_Key
)x
group by person_id
having cnt > 1
) y
""")

# COMMAND ----------

dfDupGenderPatid.createOrReplaceTempView("viewPersonWithDualGender")

# COMMAND ----------

### persist the person_id with dual gender for reference

table_name = "person_with_dual_gender"

dfDupGenderPatid.selectExpr("*", "current_timestamp() as run_timestamp").write.format("delta").option("overwriteSchema","true") \
.mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create person

# COMMAND ----------

###44811569 Other Mixed background - ethnic category 2001 census
#### select patient and location data into df1
####
### create or replace the the table for person
### select the person data in df1
### persist the data
###
df1 = spark.sql(f"""
select distinct p.Research_Patient_Key as person_id,
case when trim(p.gender) in ('Female', 'F', 'f', 'FEMALE', 'w') then 8532
when trim(p.gender) in ('M','m', 'Male', 'MALE') then 8507
when trim(g.gender) in ('Female', 'F', 'f', 'FEMALE', 'w') then 8532
when trim(g.gender) in ('M','m', 'Male', 'MALE') then 8507
else 0
end gender_concept_id,
case when g.Gender is null then p.Gender
else g.Gender
end as mapped_gender,
cast(substr(DOB, 4, 4) as INT) as year_of_birth ,
cast(substr(DOB, 1, 2) as INT) as month_of_birth ,
0 as day_of_birth,
CAST('0' AS timestamp) as birth_datetime,
case when race ='WHITE' THEN 8527
  when race = 'BLACK OR AFRICAN AMERICAN' THEN 8516
  when race = 'AMERICAN INDIAN OR ALASKAN NATIVE' THEN 8657
  when race = 'ASIAN' THEN 8515
  when race = 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER' THEN 8557
  when race = 'TWO OR MORE RACES' THEN 44811569
  when race = 'OTHER' THEN 45878142
  else 0
end as race_concept_id ,
CASE when ethnicity ='HISPANIC' then 38003563
  when ethnicity ='NOT HISPANIC' then 38003564
  else 0 end as  ethnicity_concept_id ,
l.location_id as location_id ,
0 as  provider_id ,
0 as care_site_id ,
'0' as  person_source_value ,
case when g.Gender is null then p.Gender
else g.Gender
end as gender_source_value,
0 as  gender_source_concept_id ,
race as race_source_value ,
0  race_source_concept_id ,
ethnicity as  ethnicity_source_value,
0 as ethnicity_source_concept_id
from {databaseName}.viewPatient p
left join {databaseName}.Location l
on l.zip = p.patient_zip and p.loc_id0 = l.location_source_value
left join {databaseName}.viewGender g on p.Research_Patient_Key = g.Research_Patient_Key
where p.Research_Patient_Key not in ( select x.person_id from viewPersonWithDualGender x )
""")

# COMMAND ----------

path="crisp-covid/warehouse/"+databaseName
tablename="person"
location = "/mnt/{path}/{tablename}"

spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename}(
 person_id	   int,
gender_concept_id	int,
year_of_birth int,
month_of_birth int,
day_of_birth int,
birth_datetime timestamp,
race_concept_id	int,
ethnicity_concept_id int,
location_id	BIGINT,
provider_id	int,
care_site_id int,
person_source_value	string,
gender_source_value string,
gender_source_concept_id int,
race_source_value string,
race_source_concept_id int,
ethnicity_source_value string,
ethnicity_source_concept_id	int
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)

""")


# COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite=true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("SET spark.databricks.delta.overwriteSchema=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")
### save patient data to the table with location ids
#df.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema","true").mode("overwrite").save("/mnt/crisp-covid/<dbname>/<tableName>")
##persist person data
##
###
### update dbname
##df1.selectExpr("*","current_timestamp() as run_timeStamp").write.format("delta").option("mergeSchema","true").mode("overwrite").save("/mnt/crisp-covid/crisp_sep2021/person")
write_format = 'delta'
##load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
partition_by = 'gender_concept_id'
#save_path = '/mnt/crisp_covid/crisp_sep2021'

table_name = "person"
### /mnt/crisp-covid/crisp_sep2021/person
df1.selectExpr("*", "current_timestamp() as run_timestamp").write.format("delta").option("overwriteSchema","true").partitionBy("gender_concept_id").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create death

# COMMAND ----------

dfDeath = spark.sql(f"""
select distinct
person_id,
CAST(Death_Date as date) as death_date,
CAST(Death_Date as timestamp) as death_datetime,
cast(0 as int) as death_type_concept_id,
cast(0 as int) as cause_concept_id,
CAST(null as string) as cause_source_value,
CAST(0 as int) as cause_source_value_concept_id
from {databaseName}.person p
left join {databaseName}.viewPatient vp on p.person_id = vp.Research_Patient_Key
where length(trim(death_date)) > 0
""")

# COMMAND ----------

path="crisp-covid/warehouse/"+databaseName
tablename="death"


spark.sql(f"""CREATE OR REPLACE TABLE {databaseName}.{tablename} (
  person_id int,
  death_date date,
  death_datetime timestamp,
  death_type_concept_id int,
  cause_concept_id int,
  cause_source_value string,
  cause_source_concept_id int,
  cause_source_value_concept_id int
  )
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
""")


# COMMAND ----------

dfDeath.write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")

# COMMAND ----------
