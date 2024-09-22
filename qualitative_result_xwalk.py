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
tablename="qualitative_result_xwalk"

spark.sql(f"""CREATE OR REPLACE TABLE {dbname}.{tablename}(
column_name string,
qualitative_result string,
concept_id int
)
using delta
tblproperties(delta.autoOptimize.optimizeWrite=true)
location "/mnt/{path}/{tablename}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crisp_omop_3.qualitative_result_crosswalk2
# MAGIC

# COMMAND ----------

##2. df with data
dfqualresult= spark.sql("""
select * from crisp_omop_3.qualitative_result_crosswalk2
""")

# COMMAND ----------

# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
table_name = "vwQualresultxwalk"
dfqualresult.createOrReplaceTempView(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vwQualresultxwalk

# COMMAND ----------

## persist data for reference
# Create a view or table
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
# persist the data to the table for later access.
dfqualresult.selectExpr("*").write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"/mnt/crisp-covid/crisp_apr2021/qualitative_result_xwalk")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crisp_apr2021.qualitative_result_xwalk

# COMMAND ----------
