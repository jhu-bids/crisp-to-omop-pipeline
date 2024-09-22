# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
## 1. create table
## dataframe with data
## persist

## some settings to set before reading parquet files
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite.enabled=true")
spark.sql("set spark.databricks.delta.commitValidation.enabled=false")
#####################################################
## create the table
dbname= "crisp_apr2021"
path="crisp-covid/{}".format(dbname)
tablename="unit_of_measure_xwalk"

spark.sql(f"""CREATE TABLE if not exists {dbname}.{tablename}(
CDM_SOURCE string,
CDM_TBL string,
CDM_TBL_COLUMN_NAME string,
SRC_CODE string,
SRC_CD_DESCRIPTION string,
TARGET_CONCEPT_ID int,
TARGET_CONCEPT_NAME string,
TARGET_DOMAIN_ID string,
TARGET_VOCABULARY_ID string,
TARGET_CONCEPT_CLASS_ID string,
TARGET_STANDARD_CONCEPT string,
TARGET_CONCEPT_CODE string,
TARGET_TBL_COLUMN_NAME string
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crisp_omop_3.unit_of_measure_xwalk
# MAGIC

# COMMAND ----------

##2. df with data
dfunitofmeasure = spark.sql("""
select * from crisp_omop_3.unit_of_measure_xwalk
""")

# COMMAND ----------

# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
table_name = "viewUnitOfMeasure"
dfunitofmeasure.createOrReplaceTempView(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from viewUnitOfMeasure

# COMMAND ----------

## persist data for reference
# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# persist the data to the table for later access.
dfunitofmeasure.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_apr2021/unit_of_measure_xwalk")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crisp_apr2021.unit_of_measure_xwalk

# COMMAND ----------
