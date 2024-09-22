# version History
# Sept 2022, Stephanie Hong initial version
# May 2023, Yvette Chen, use input filepath, database name and vocabulary dbname parameter
#
# Databricks notebook source
!pip install tableone --quiet
!pip install geopandas --quiet
!pip install venn --quiet

# COMMAND ----------

import sys
if sys.version_info[0] == 3 and sys.version_info[1] < 7:
  from packaging import version
else:
  from distutils.version import LooseVersion

from io import StringIO
import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from tableone import TableOne

import seaborn as sns
from matplotlib import pyplot as plt

# COMMAND ----------

#Function to convert a number into a human format (adds letters at the end of the number)
def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.2f%s' % (num, ['', 'k', 'm', 'b', 't', 'p'][magnitude])


# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.combobox("databaseName", "crisp_omop",["crisp_jan2022","crisp_sep2021","crisp_apr2021"], "CRISP database name")
dbutils.widgets.combobox("vocabdatabaseName", "omop_vocab_20220602",["omop_vocab_20220602","omop_vocab_20220204"], "OMOP Vocabulary database name")
dbutils.widgets.text("omopconcepts", "omop_concepts", "Concept Sets Database")

# COMMAND ----------

databaseName = dbutils.widgets.get("databaseName")
vocabdatabaseName = dbutils.widgets.get("vocabdatabaseName")
omopconcepts = dbutils.widgets.get("omopconcepts")

# COMMAND ----------

## procedure_occurrence
## count of future date
query="""Select count(*) proc_future_date from """+databaseName+""".procedure_occurrence where procedure_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
proc_future_date = human_format(pandasDF.proc_future_date[0])
proc_future_date


query="""Select count(*) proc_null from """+databaseName+""".procedure_occurrence where procedure_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
proc_null = human_format(pandasDF.proc_null[0])





print([proc_future_date,proc_null])


# COMMAND ----------

## visit_occurrence
## count of future date
query="""Select count(*) visit_future_date from """+databaseName+""".visit_occurrence where visit_start_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
visit_future_date = human_format(pandasDF.visit_future_date[0])
visit_future_date


query="""Select count(*) visit_null from """+databaseName+""".visit_occurrence where visit_start_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
visit_null = human_format(pandasDF.visit_null[0])



print([visit_future_date,visit_null])

# COMMAND ----------

## measurement
## count of future date
query="""Select count(*) measurement_future_date from """+databaseName+""".measurement where measurement_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
measurement_future_date = human_format(pandasDF.measurement_future_date[0])
measurement_future_date


query="""Select count(*) measurement_null from """+databaseName+""".measurement where measurement_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
measurement_null = human_format(pandasDF.measurement_null[0])



print([measurement_future_date,measurement_null])

# COMMAND ----------

displayHTML("""

<!DOCTYPE html>
<html>
<head>
<!-- Bootstrap CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css">
    <!-- Bootstrap Font Icon CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.5.0/font/bootstrap-icons.css">
<style>
.center {
  margin-left: 20px;
  width: 100%;
}
.value-large {
    margin-top: 20px;
    color: #4B6AB2;
    font-size: 28px;
    line-height: 32px;
    font-weight: 600;
    padding-right: 100px;
}
.title {
    color: #8a9ba8;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    padding-right: 70px;
    padding-top: 10px;
}
.text {
    display: inline-block;
    margin-top: 6px;
}
hr {
    margin-top: 20px;
    margin-left: 10px;
    background-color: #D3D3D3;
    border: none;
    height: 0.5px;
}
.value-small {
    color: #4B6AB2;
    font-size: 23px;
    line-height: 25px;
    font-weight: 600;
    padding-right: 70px;
}
</style>
<table class='center'>
  <tr>
    <td>
      <div class="value-small">"""+str(proc_future_date )+"""</div>
      <div class="title"> Future Date in Procedure Occurrence</div>
    </td>
    <td>
      <div class="value-small"></i> """+str(proc_null)+"""</div>
      <div class="title">Null in Procedure Occurrence</div>
    </td>
    <td>
      <div class="value-small">"""+str(visit_future_date)+"""</div>
      <div class="title">Future Date in Visit Occurrence</div>
    </td>
    <td>
      <div class="value-small">"""+str(visit_null)+"""</div>
      <div class="title">Null in Visit Occurence</div>
    </td>
    <td>
      <div class="value-small">"""+str(measurement_future_date)+"""</div>
      <div class="title">Future Date in Measurement</div>
    </td>
    <td>
      <div class="value-small">"""+str(measurement_null)+"""</div>
      <div class="title">Null in Measurement</div>
    </td>
  </tr>
</table>
  <hr>
</body>
</html>

""")


# COMMAND ----------

query="""Select count(*) drug_exposure_future_date from """+databaseName+""".drug_exposure where drug_exposure_start_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
drug_exposure_future_date = human_format(pandasDF.drug_exposure_future_date[0])
drug_exposure_future_date


query="""Select count(*) drug_exposure_null from """+databaseName+""".drug_exposure where drug_exposure_start_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
drug_exposure_null = human_format(pandasDF.drug_exposure_null[0])



print([drug_exposure_future_date,drug_exposure_null])

# COMMAND ----------

query="""Select count(*) person_future_date from """+databaseName+""".person where year_of_birth > '2022' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
person_future_date = human_format(pandasDF.person_future_date[0])
person_future_date


query="""Select count(*) person_null from """+databaseName+""".person where year_of_birth is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
person_null = human_format(pandasDF.person_null[0])



print([person_future_date,person_null])

# COMMAND ----------

query="""Select count(*) condition_future_date from """+databaseName+""".condition_occurrence_domain_map where condition_start_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
condition_future_date = human_format(pandasDF.condition_future_date[0])

query="""Select count(*) condition_null from """+databaseName+""".condition_occurrence_domain_map where condition_start_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
condition_null = human_format(pandasDF.condition_null[0])



print([condition_future_date,condition_null])

# COMMAND ----------

query="""Select count(*) cvlab_future_date from """+databaseName+""".cvlab_ext where order_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
cvlab_future_date = human_format(pandasDF.cvlab_future_date[0])

query="""Select count(*) cvlab_null from """+databaseName+""".cvlab_ext where order_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
cvlab_null = human_format(pandasDF.cvlab_null[0])



print([cvlab_future_date,cvlab_null])

# COMMAND ----------

query="""Select count(*) death_future_date from """+databaseName+""".death where death_date > '2022-09-08' """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
death_future_date = human_format(pandasDF.death_future_date[0])

query="""Select count(*) death_null from """+databaseName+""".death where death_date is null """
dataframe = sqlContext.sql(query)
pandasDF = dataframe.toPandas()
death_null = human_format(pandasDF.death_null[0])



print([death_future_date,death_null])

# COMMAND ----------

displayHTML("""

<!DOCTYPE html>
<html>
<head>
<!-- Bootstrap CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css">
    <!-- Bootstrap Font Icon CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.5.0/font/bootstrap-icons.css">
<style>
.center {
  margin-left: 20px;
  width: 100%;
}
.value-large {
    margin-top: 20px;
    color: #4B6AB2;
    font-size: 28px;
    line-height: 32px;
    font-weight: 600;
    padding-right: 100px;
}
.title {
    color: #8a9ba8;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    padding-right: 70px;
    padding-top: 10px;
}
.text {
    display: inline-block;
    margin-top: 6px;
}
hr {
    margin-top: 20px;
    margin-left: 10px;
    background-color: #D3D3D3;
    border: none;
    height: 0.5px;
}
.value-small {
    color: #4B6AB2;
    font-size: 23px;
    line-height: 25px;
    font-weight: 600;
    padding-right: 70px;
}
</style>
<table class='center'>
  <tr>
    <td>
      <div class="value-small">"""+str(drug_exposure_future_date)+"""</div>
      <div class="title"> Future Date in drug Exposure</div>
    </td>
    <td>
      <div class="value-small"></i> """+str(drug_exposure_null)+"""</div>
      <div class="title">Null Date in drug Exposure</div>
    </td>
    <td>
      <div class="value-small">"""+str(person_future_date)+"""</div>
      <div class="title">Future Brith Year in Person</div>
    </td>
    <td>
      <div class="value-small">"""+str(person_null)+"""</div>
      <div class="title">Null Date in Person</div>
    </td>
    <td>
      <div class="value-small">"""+str(condition_future_date)+"""</div>
      <div class="title">Future Date in condition occurrence domain map </div>
    </td>
    <td>
      <div class="value-small">"""+str(condition_null)+"""</div>
      <div class="title">Null Date in condition occurrence domain map</div>
    </td>
    <td>
      <div class="value-small">"""+str(cvlab_future_date)+"""</div>
      <div class="title">Future Date in cvlab_ext </div>
    </td>
    <td>
      <div class="value-small">"""+str(cvlab_null)+"""</div>
      <div class="title">Null Date in cvlab_ext</div>
    </td>
    <td>
      <div class="value-small">"""+str(death_future_date)+"""</div>
      <div class="title">Future Date in death </div>
    </td>
    <td>
      <div class="value-small">"""+str(death_null)+"""</div>
      <div class="title">Null Date in death </div>
    </td>
  </tr>
</table>
  <hr>
</body>
</html>

""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.procedure_occurrence

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.procedure_occurrence
# MAGIC WHERE procedure_date > '2022-09-08'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.procedure_occurrence

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.procedure_occurrence
# MAGIC WHERE procedure_date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.procedure_occurrence

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.measurement

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.measurement
# MAGIC WHERE measurement_date > '2022-09-08'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.measurement

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.person

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $databaseName.person where year_of_birth > '2022'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.person
# MAGIC where year_of_birth > '2022'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.person

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.cvlab_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.cvlab_ext where order_date > '2022-09-08'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.cvlab_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.death

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $databaseName.death where death_date is null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $databaseName.death

# COMMAND ----------
