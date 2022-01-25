<img src="./img/logo.png" alt="Chmurowisko logo" width="200" align="right">
<br><br>
<br><br>
<br><br>

# Creating ETL job

## LAB Overview

#### In this lab you will create a simple ETL job and BI dashboard

## Prerequisities:
- access to Databricks workspace
- databricks cluster created
- blob storage account

## Task 1. Load Covid-19 data

In this task you will load covid data into Spark dataframe and display it.

1. In order to create dataframe, Spark needs a data schema. You can either provide this schema manually or Spark can infer this schema automatically. First option is more efficient.
2. Create new cell in notebook and paste schema definition:
```
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
```
3. Download .csv data file:
```
%sh
wget https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv
```
4. Copy file into dbfs:
```
dbutils.fs.cp("file:///databricks/driver/owid-covid-data.csv", "dbfs:/")
```
5. Create a spark dataframe from .csv file:
`df = spark.read.csv("dbfs:/owid-covid-data.csv", header=True, schema=schema)`

6. Register data frame as a spark table: `df.write.saveAsTable("covid_data")`

## Task 2. Using chosen method create two charts

1. Create countries list:
```
covid = spark.sql("select * from covid_data")
countries = covid.select("iso_code").distinct()
countries_list = countries.toPandas()['iso_code']
```
2. Create dropdown:
```
dbutils.widgets.dropdown("country", "POL", countries_list)
```
3. Now, create two charts with country filter. For example:
```
new_cases = covid.where(df["iso_code"] == dbutils.widgets.get("country")) \
    .select(["iso_code", "new_cases", "date"])
display(new_cases)
```
4. Create second chart of your choice

## Task 3. Create new dashboard

## END LAB

<br><br>

<center><p>&copy; 2019 Chmurowisko Sp. z o.o.<p></center>
