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

### rebuild c2o_xwalk map using the newly loaded payload and new omop vocabulary tables.
### codes from the diag/ proc/ Labs/ cvLabs / covid correction map are updated in the c2o_xwalk mapping table
### Edit History:
### Date:   Author   : Comments
### 11/8/21 S. Hong  : September2021 payload is updated in October2021 with the omop_vocab_20210727 vocabulary table
### 5/1/2023 Yvette: update the hard-wired path to automate the script

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create c2o_xwalk

# COMMAND ----------

### note :
### all the LOINC codes present in the covid_correction table is added to the xwalk map, incase they are used from the newly submitted payload.
###
dfXwalk = spark.sql(f"""
WITH CTE_VOCAB_MAP AS
       (

          SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID,
                                  c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
                                  c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON,
                                  c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
                                  c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
                   FROM omop_vocab.concept C
                         JOIN omop_vocab.concept_relationship CR
                                    ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                                    AND CR.invalid_reason IS NULL
                                    AND lower(cr.relationship_id) = 'maps to'
                          JOIN omop_vocab.concept C1
                                    ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                                    AND C1.INVALID_REASON IS NULL
        )
        SELECT distinct 'Diagnosis' as CDM_TBL,
        dx_code as src_code,
        mapped_vocabulary_id as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        FROM cte_vocab_map
        join {databaseName}.viewdiagnosis f
        on source_code = dx_code and source_vocabulary_id = f.mapped_vocabulary_id
        where target_standard_concept = 'S'
        union
        SELECT distinct 'Procedure' as CDM_TBL,
        procedureType as src_code,
        mapped_vocabulary_id as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        FROM cte_vocab_map
        join {databaseName}.viewprocedure p
        on upper(source_code) = upper(trim(procedureType)) and  source_vocabulary_id = p.mapped_vocabulary_id
        where target_standard_concept = 'S' and procedureType is not null
        union
        SELECT distinct 'Measurement1' as CDM_TBL,
        l.LoincCode as src_code,
        'LOINC' as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        FROM cte_vocab_map
        join {databaseName}.viewlabs l on upper(source_code) = upper(trim( LoincCode)) and source_vocabulary_id = 'LOINC'
        where target_standard_concept = 'S' and LoincCode is not null
        union
        SELECT distinct 'Measurement2' as CDM_TBL,
        l.observationID as src_code,
        'LOINC' as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        FROM cte_vocab_map
        join {databaseName}.nedss_test_output_file_mapped l on upper(source_code) = upper(trim( observationID)) and source_vocabulary_id = 'LOINC'
        where target_standard_concept = 'S' and observationID is not null
        union
        SELECT distinct 'Measurement3' as CDM_TBL,
        map.corrected_code as src_code,
        'LOINC' as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        FROM cte_vocab_map
        join {databaseName}.crisp_covid_correction map on upper(trim(map.corrected_code)) = upper(trim(source_code))
        and source_vocabulary_id = 'LOINC'
        where target_standard_concept = 'S' and corrected_code is not null
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from yc_test.measurement

# COMMAND ----------

# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

c2o_table_name = "view_c2o_xwalk"
dfXwalk.createOrReplaceTempView(c2o_table_name)

# COMMAND ----------

## some settings to set before reading parquet files
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite.enabled=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")
#####################################################
## create the table

path="crisp-covid/warehouse/"+databaseName
tablename="c2o_code_xwalk"
location= "/mnt/{path}/{tablename}"

spark.sql(f"""CREATE TABLE if not exists {databaseName}.{tablename}(
cdm_tbl string,
src_code string,
src_vocab_code string,
source_code string,
source_concept_id int,
source_code_description string,
source_vocabulary_id string,
source_domain_id string,
target_concept_id int,
target_concept_name string,
target_vocabulary_id string,
target_domain_id string,
target_concept_class_id string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------


tablename="c2o_code_xwalk"

# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# persist the data to the table for later access.
##dfXwalk.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_apr2021/c2o_code_xwalk")
dfXwalk.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{databaseName}.{tablename}")


# COMMAND ----------
