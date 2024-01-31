# Filter Operation
# &,|,==
# ~

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('dataframe').getOrCreate()
df_pyspark=spark.read.csv('test1.csv',header=True,inferSchema=True)
df_pyspark.show()

### Salary of the people less than or equal to 20000
df_pyspark.filter("Salary<=20000").show()

df_pyspark.filter("Salary<=20000").select(['Name','age']).show()

df_pyspark.filter(df_pyspark['Salary']<=20000).show()

df_pyspark.filter((df_pyspark['Salary']<=20000) | 
                  (df_pyspark['Salary']>=15000)).show()

#negating condition ~
df_pyspark.filter(~(df_pyspark['Salary']<=20000)).show()
