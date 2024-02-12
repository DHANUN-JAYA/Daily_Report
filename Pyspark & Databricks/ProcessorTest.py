# Databricks notebook source
# MAGIC %run /Users/sadhu.dhanunjay@diggibyte.com/unitTestingFolder/commonFunctionTest

# COMMAND ----------

# MAGIC %run /Users/sadhu.dhanunjay@diggibyte.com/unitTestingFolder/mainCommonFunction

# COMMAND ----------

import unittest
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime 
class test_(unittest.TestCase):
    def test_duplicate_primary_key(self):
        path="dbfs:/FileStore/tbro"
        format_name="csv"
        options={"Header":True,"InferSchema":True}
        df=create_df(path,format_name,**options)
        primary_key_columns = ['name']
        duplicate_count = duplicate_primary_key(df,primary_key_columns)
        self.assertEqual(duplicate_count, 0)

    def test_non_null_primary_key(self):
        path="dbfs:/FileStore/tbro"
        format_name="csv"
        options={"Header":True,"InferSchema":True}
        df=create_df(path,format_name,**options)
        primary_key_columns = 'name'
        null_count = non_null_primary_key(df,primary_key_columns)
        self.assertEqual(null_count, 0)
    def test_records_going_to_quarantine_path(self):
        data = [
            Row(id=1, name='John', age=30, quarantine_flag=False),
            Row(id=2, name='Alice', age=None, quarantine_flag=True),
            Row(id=3, name='Bob', age=25, quarantine_flag=False),
            Row(id=4, name=None, age=40, quarantine_flag=False),
            Row(id=5, name='Jane', age=35, quarantine_flag=True)
        ]

        # Define schema for DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("quarantine_flag", BooleanType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        quarantined_count = records_going_to_quarantine_path(df)
        self.assertGreater(quarantined_count, 0)
    def test_audit_column(self):
        silver_data = [
        ("John", 30, datetime.now()),
        ("Alice", 25, datetime.now()),
        ("Bob", 40, datetime.now())
        ]

        # Define schema for Silver layer
        silver_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
            StructField("load_date", TimestampType(), True)
            ])

        # Create DataFrame for Silver layer
        silver_df = spark.createDataFrame(silver_data, schema=silver_schema)

        # Sample data for Gold layer
        gold_data = [
            ("John", 30, datetime.now()),
            ("Alice", 25, datetime.now()),
            ("Bob", 40, datetime.now())
        ]

        # Define schema for Gold layer
        gold_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
            StructField("load_date", TimestampType(), True)
        ])
        # Create DataFrame for Gold layer
        gold_df = spark.createDataFrame(gold_data, schema=gold_schema)
        bool_value=audit_column(silver_df,gold_df)
        self.assertEqual(bool_value, True)
        
        
s = unittest.TestLoader().loadTestsFromTestCase(test_)
unittest.TextTestRunner(verbosity=2).run(s)

# COMMAND ----------

path="dbfs:/FileStore/tbro"
format_name="csv"
options={"Header":True,"InferSchema":True}
df=create_df(path,format_name,**options)
display(df)

# COMMAND ----------

df.write.format("delta").save('/FileStore/sampledat')

# COMMAND ----------


