# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
# MAGIC %sql
# MAGIC select count(distinct person_id) as person_cnt from crisp_sep2021.person
# MAGIC --4,349,184

# COMMAND ----------

## used to store the omop concepts
dbname = "omop_concepts"
path ="crisp-covid/{}".format(dbname)
spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path))



# COMMAND ----------

##CRISP data transformed to OMOP DB before cleanup , parameterized code is used to populate this data ( dbname,  path, parquet filenames)
##dbname = "crisp_omop_prime"
dbname = "crisp_june2022"
path ="crisp-covid/{}".format(dbname)
spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path))

# COMMAND ----------

##CRISP data transformed to OMOP DB only copy the OMOP domain data from crisp_omop_prime to crisp_omop_lds apply cleanup code (dqp process)
dbname = "crisp_omop_lds"
path ="crisp-covid/{}".format(dbname)
spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path))

# COMMAND ----------

## used to store the omop concepts
dbname = "crisp_omop_dqp"
path ="crisp-covid/{}".format(dbname)
spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path))


# COMMAND ----------

dbname = "crisp_sandbox"
path ="crisp-covid/{}".format(dbname)
spark.sql("""create database if not exists {0} location '/mnt/{1}'""".format(dbname,path))

# COMMAND ----------
