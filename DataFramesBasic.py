# Covered in this file 
# PySpark Dataframe , df is a data structure
# Reading The Dataset, print
# Checking the Datatypes of the Column(Schema)
# Selecting Columns And Indexing
# Check Describe option similar to Pandas
# Adding Columns
# Dropping columns
# Renaming Columns
import DataFramesBasic
import pandas as pd
from pyspark.sql import SparkSession

pd.read_csv('test1.csv')


spark=SparkSession.builder.appName('Dataframe').getOrCreate()
df_pyspark=spark.read.option('header','true').csv('test1.csv',inferSchema=True)
df_pyspark.printSchema()
df_pyspark.show()
df_pyspark.printSchema()
type(df_pyspark)
df_pyspark.head(3)
df_pyspark.select(['Name','Age']).show()
df_pyspark['Name']
df_pyspark.dtypes
df_pyspark.describe().show()
### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)

df_pyspark.show()
### Drop the columns
df_pyspark=df_pyspark.drop('Experience After 2 year')

df_pyspark.show()
### Rename the columns
df_pyspark.withColumnRenamed('Name','New Name').show()