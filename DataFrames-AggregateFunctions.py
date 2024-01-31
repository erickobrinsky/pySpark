from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Agg').getOrCreate()
spark

df_pyspark=spark.read.csv('test3.csv',header=True,inferSchema=True)
df_pyspark.show()

df_pyspark.printSchema()
## Groupby
### Grouped to find the maximum salary
df_pyspark.groupBy('Name').sum().show()

df_pyspark.groupBy('Name').avg().show()
### Groupby Departmernts  which gives maximum salary
df_pyspark.groupBy('Departments').sum().show()
df_pyspark.groupBy('Departments').mean().show()
df_pyspark.groupBy('Departments').count().show()
df_pyspark.agg({'Salary':'sum'}).show()
