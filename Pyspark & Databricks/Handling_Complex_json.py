# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format('json').option("multiline","true").load("dbfs:/FileStore/Nested_json_file__1_-1.json")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# Define the schema for the properties struct
properties_schema = StructType([
    StructField("name", StringType(), True),
    StructField("storeSize", StringType(), True)
])

# Define the schema for the employees struct
employees_schema = StructType([
    StructField("empId", LongType(), True),
    StructField("empName", StringType(), True),
    StructField("id", LongType(), True),
    StructField("properties", properties_schema, True)
])


# COMMAND ----------

df=spark.read.format('json').option("multiline","true").schema(employees_schema).load("dbfs:/FileStore/Nested_json_file__1_-1.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
exploded_df = df.withColumn("name", col("properties").getItem("name")).withColumn("storeSize", col("properties").getItem("storeSize")).drop("properties")
exploded_df.display()

# COMMAND ----------

df=spark.read.format('json').option("multiline","true").load("dbfs:/FileStore/data1.json")

# COMMAND ----------

quiz_schema = StructType([
    StructField("quiz", StructType([
        StructField("maths", StructType([
            StructField("q1", StructType([
                StructField("answer", StringType(), True),
                StructField("options", ArrayType(StringType()), True),
                StructField("element", StringType(), True),
                StructField("question", StringType(), True)
            ])),
            StructField("q2", StructType([
                StructField("answer", StringType(), True),
                StructField("options", ArrayType(StringType()), True),
                StructField("element", StringType(), True),
                StructField("question", StringType(), True)
            ]))
        ])),
        StructField("sport", StructType([
            StructField("q1", StructType([
                StructField("answer", StringType(), True),
                StructField("options", ArrayType(StringType()), True),
                StructField("element", StringType(), True),
                StructField("question", StringType(), True)
            ]))
        ]))
    ]))
])


# COMMAND ----------

df=spark.read.format('json').option("multiline","true").load("dbfs:/FileStore/data1.json")
df.display()

# COMMAND ----------


df = df.select("quiz.*")
display(df)

# COMMAND ----------

df2 = df.select("maths.*","*").alias("mathsQuestions")
df2=df2.withColumnRenamed("q1","mathsq1").withColumnRenamed("q2","mathsq2")
df2=df2.select("sport.*","*")
df2=df2.withColumnRenamed("q1","sportq1")
df2=df2.select("sportq1","mathsq1","mathsq2")
df2.display()

# COMMAND ----------

df3=df2.select("sportq1.*")
# df3=df3.withColumnRenamed("answer","sportq1.*").withColumnRenamed("options","sportq1.options").withColumnRenamed("question","sportq1.question")
# display(df3)

# COMMAND ----------

df4=df2.select("mathsq1.*")
df4=df4.withColumnRenamed("answer","mathsq1.answer").withColumnRenamed("options","mathsq1.options").withColumnRenamed("question","mathsq1.question")
display(df4)

# COMMAND ----------

df5=df2.select("mathsq2.*")
df5=df5.withColumnRenamed("answer","mathsq2.answer").withColumnRenamed("options","mathsq2.options").withColumnRenamed("question","mathsq2.question")
display(df5)

# COMMAND ----------

display(df3)
display(df4)
display(df5)

# COMMAND ----------

df6=df3.join(df4)
final_df=df6.join(df5)
final_df.display(
    
)

# COMMAND ----------

columnss=df2.columns
print(columnss)

# COMMAND ----------

for i in columnss:
    print(i)

# COMMAND ----------

d

# COMMAND ----------

for i in columnss:
    df2=df2.withColumn(i,col(i).getIteam("answer")).withColumn(i,col(i).getIteam("options")).withColumn(i,col(i).getIteam("question"))
display(df2)

# COMMAND ----------

df = df.select(df.quiz)

# COMMAND ----------

df.wit

# COMMAND ----------

df.show()

# COMMAND ----------

columns=df.columns
print(columns)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


# Sample DataFrame
data = [("1", {"name": "John", "age": "30"}),
        ("2", {"name": "Jane", "age": "25"}),
        ("3",{"name":"sadhu","age":"23"})]
schema = ["id", "attributes"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.display()

# Explode the attributes column to extract id, name, and age
exploded_df = df.selectExpr("id", "explode(attributes) as (key, value)")
exploded_df.display()

# Pivot the DataFrame to get id, name, and age
pivoted_df = exploded_df.groupBy("id").pivot("key").agg({"value": "first"})

# Show the resulting DataFrame
pivoted_df.show()





# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("JSON to DataFrame") \
    .getOrCreate()

# Given JSON data
json_data = '{"maths":{"q1":{"answer":"12","options":["10","11","12","13"],"question":"5 + 7 = ?"},"q2":{"answer":"4","options":["1","2","3","4"],"question":"12 - 8 = ?"}},"sport":{"q1":{"answer":"Huston Rocket","options":["New York Bulls","Los Angeles Kings","Golden State Warriros","Huston Rocket"],"question":"Which one is correct team name in NBA?"}}}'

# Create DataFrame from JSON data
df = spark.read.json(spark.sparkContext.parallelize([json_data]))

# Flatten the nested structure
df = df.select(explode("maths").alias("subject"), col("maths.*")).union(
    df.select(explode("sport").alias("subject"), col("sport.*"))
)

# Flatten the nested structure further for questions
df = df.select("subject", 
               col("q1.answer").alias("q1_answer"),
               col("q1.options").alias("q1_options"),
               col("q1.question").alias("q1_question"),
               col("q2.answer").alias("q2_answer"),
               col("q2.options").alias("q2_options"),
               col("q2.question").alias("q2_question"))

# Show the DataFrame
df.show(truncate=False)


# COMMAND ----------


