# Databricks notebook source
# MAGIC %md
# MAGIC # Delta lake lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisities:
# MAGIC - Azure Data lake gen 2 created
# MAGIC - Databricks cluster
# MAGIC - owid-covid-data.csv file in Azure Blob Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load covid data into dataframe

# COMMAND ----------

# DBTITLE 1,Mount blob storage
dbutils.fs.mount(
  source="wasbs://data@sannst01.blob.core.windows.net",
  mount_point="/mnt/data",
  extra_configs = {"fs.azure.account.key.sannst01.blob.core.windows.net": "S/xHWqim1DP/mVpixnosvXRwuYOZazG0YtYl3uNLzZlJp063YvWnAGfAA92bTjasTydhmyEkVgCd/AwC/uzvmg=="}
)

# COMMAND ----------

# DBTITLE 1,Define schema
schema = """
iso_code STRING,
continent STRING,
location STRING,
date DATE,
total_cases FLOAT,
new_cases FLOAT,
new_cases_smoothed FLOAT,
total_deaths FLOAT,
new_deaths FLOAT,
new_deaths_smoothed FLOAT,
total_cases_per_million FLOAT,
new_cases_per_million FLOAT,
new_cases_smoothed_per_million FLOAT,
total_deaths_per_million FLOAT,
new_deaths_per_million FLOAT,
new_deaths_smoothed_per_million FLOAT,
reproduction_rate FLOAT,
icu_patients FLOAT,
icu_patients_per_million FLOAT,
hosp_patients FLOAT,
hosp_patients_per_million FLOAT,
weekly_icu_admissions FLOAT,
weekly_icu_admissions_per_million FLOAT,
weekly_hosp_admissions FLOAT,
weekly_hosp_admissions_per_million FLOAT,
new_tests FLOAT,
total_tests FLOAT,
total_tests_per_thousand FLOAT,
new_tests_per_thousand FLOAT,
new_tests_smoothed FLOAT,
new_tests_smoothed_per_thousand FLOAT,
positive_rate FLOAT,
tests_per_case FLOAT,
tests_units FLOAT,
total_vaccinations FLOAT,
people_vaccinated FLOAT,
people_fully_vaccinated FLOAT,
total_boosters FLOAT,
new_vaccinations FLOAT,
new_vaccinations_smoothed FLOAT,
total_vaccinations_per_hundred FLOAT,
people_vaccinated_per_hundred FLOAT,
people_fully_vaccinated_per_hundred FLOAT,
total_boosters_per_hundred FLOAT,
new_vaccinations_smoothed_per_million FLOAT,
new_people_vaccinated_smoothed FLOAT,
new_people_vaccinated_smoothed_per_hundred FLOAT,
stringency_index FLOAT,
population FLOAT,
population_density FLOAT,
median_age FLOAT,
aged_65_older FLOAT,
aged_70_older FLOAT,
gdp_per_capita FLOAT,
extreme_poverty FLOAT,
cardiovasc_death_rate FLOAT,
diabetes_prevalence FLOAT,
female_smokers FLOAT,
male_smokers FLOAT,
handwashing_facilities FLOAT,
hospital_beds_per_thousand FLOAT,
life_expectancy FLOAT,
human_development_index FLOAT,
excess_mortality_cumulative_absolute FLOAT,
excess_mortality_cumulative FLOAT,
excess_mortality FLOAT,
excess_mortality_cumulative_per_million FLOAT
"""

# COMMAND ----------

# DBTITLE 1,Read .csv file into DataFrame
df = spark.read.csv("dbfs:/mnt/data/owid-covid-data.csv", header=True, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2: Save data as a Delta Lake table

# COMMAND ----------

# MAGIC %md
# MAGIC Go to Azure Portal and create a container "rawdata" in Azure Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC In next cell we need to configure credentials to Azure Data Lake. Fill in placeholders:
# MAGIC - DATA_LAKE_NAME
# MAGIC - DATA_LAKE_KEY
# MAGIC - DATA_LAKE_CONTAINER

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.st01datalake.dfs.core.windows.net",
    "Psspa054QVTVWniz5IlVEkUXU8QxpKMSKpJvWcoWU/T5NOpMv8We44HYu8KNGpj0M2vwg6vuLW0UXi9FlkQt4w=="
)
data_lake_path = "abfss://rawdata@st01datalake.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's save dataframe as a Delta Lake table stored in data lake. Two remarks:
# MAGIC - Let's assume that it is 10 January 2020 now. Just for exercise purposes
# MAGIC - Note the "partitionBy" statement that defines how data will be stored in data lake.

# COMMAND ----------

df.where(df["date"] < "2020-01-10") \
    .write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save(data_lake_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now go to Azure Portal and check that your data in saved using partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 3: Work with Delta Lake table

# COMMAND ----------

# DBTITLE 1,Create delta lake table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS covid_data;
# MAGIC CREATE TABLE covid_data using DELTA location "abfss://rawdata@st01datalake.dfs.core.windows.net/";

# COMMAND ----------

# DBTITLE 1,Display data
display(spark.sql("select * from covid_data"))

# COMMAND ----------

# DBTITLE 1,Describe table's transaction history
# MAGIC %sql DESCRIBE HISTORY covid_data

# COMMAND ----------

# DBTITLE 1,Write all data into table
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save(data_lake_path)

# COMMAND ----------

# DBTITLE 1,Check the history again
# MAGIC %sql DESCRIBE HISTORY covid_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 4: Schema enforcement

# COMMAND ----------

# DBTITLE 1,Create new data with additional column that doesn't match table's schema
df2 = spark.sql("select * from covid_data")
new_data = df2.where(df2["date"] == "2021-12-31").where(df2["location"] == "Poland")
new_data = new_data.withColumn("bad_column", df2["location"] + "xxx")
new_data.printSchema()
display(new_data)

# COMMAND ----------

# DBTITLE 1,Try to insert new data - you should get an error
new_data.write.format("delta").mode("append").saveAsTable("covid_data")

# COMMAND ----------

# DBTITLE 1,If you want to change schema, use schema evolution
new_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("covid_data")

# COMMAND ----------

# DBTITLE 1,Check new data
# MAGIC %sql
# MAGIC select * from covid_data where location = "Poland" and date = "2021-12-31"

# COMMAND ----------

# DBTITLE 1,See history again
# MAGIC %sql DESCRIBE HISTORY covid_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 5: Time travel

# COMMAND ----------

# DBTITLE 1,Count number of rows in whole table
spark.sql("SELECT COUNT(*) FROM covid_data").show()

# COMMAND ----------

# DBTITLE 1,Count number of rows in first version
spark.sql("SELECT COUNT(*) FROM covid_data VERSION AS OF 0").show()

# COMMAND ----------

# DBTITLE 1,Restore data to older version
# MAGIC %sql RESTORE covid_data VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Count now
spark.sql("SELECT COUNT(*) FROM covid_data").show()

# COMMAND ----------

# DBTITLE 1,See what has changed in table history
# MAGIC %sql DESCRIBE HISTORY covid_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 6: DML support
# MAGIC In traditional spark data frame you cannot delete or update rows. You must always create a new data frame. Delta lake supports these operations.

# COMMAND ----------

# DBTITLE 1,See current data in table (you should get 36 rows)
# MAGIC %sql select * from covid_data

# COMMAND ----------

# DBTITLE 1,Delete one row from table (now you should get 35 rows)
# MAGIC %sql
# MAGIC DELETE FROM covid_data where location = "Greece";
# MAGIC select * from covid_data

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY covid_data

# COMMAND ----------

# DBTITLE 1,Insert deleted row back
# MAGIC %sql
# MAGIC INSERT INTO covid_data
# MAGIC SELECT * FROM covid_data VERSION AS OF 3
# MAGIC WHERE location = "Greece"

# COMMAND ----------

# DBTITLE 1,You should get 36 rows again
# MAGIC %sql select * from covid_data

# COMMAND ----------

# DBTITLE 1,Generate fake new data
import pyspark.sql.functions as F
new_greece_data = spark.sql("select * from covid_data where location = 'Greece'")
new_greece_data = new_greece_data.withColumn("continent", F.lit("Asia"))
display(new_greece_data)
new_greece_data.createOrReplaceTempView("new_greece_data")

# COMMAND ----------

# DBTITLE 1,Use MERGE INTO. It can work as un UPSERT operation
# MAGIC %sql
# MAGIC MERGE INTO covid_data AS c
# MAGIC USING new_greece_data AS n
# MAGIC ON n.iso_code = c.iso_code
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql select * from covid_data

# COMMAND ----------

