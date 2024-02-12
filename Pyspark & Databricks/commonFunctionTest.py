# Databricks notebook source
def create_df(path,format_name,**schema):
    df=spark.read.format(format_name).options(**schema).load(path)
    return df
    
"""path='dbfs:/FileStore/annual_enterprise_survey_2021_financial_year_provisional_csv.csv'
format_name='csv'
schema={"Header":True,"InferSchema":True}"""

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


