# Dropping Columns
# Dropping Rows
# Various Parameter In Dropping functionalities
# Handling Missing values by Mean, MEdian And Mode

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()

df_pyspark=spark.read.csv('test2.csv',header=True,inferSchema=True)
df_pyspark.printSchema()
df_pyspark.show()

##drop the columns
df_pyspark.drop('Name').show()
df_pyspark.show()

df_pyspark.na.drop().show()

### any==how
df_pyspark.na.drop(how="any").show()

##threshold
df_pyspark.na.drop(how="any",thresh=3).show()

##Subset
df_pyspark.na.drop(how="any",subset=['Age']).show()

### Filling the Missing Value
df_pyspark.na.fill('Missing Values',['age','Experience']).show()


from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")

# Add imputation cols to df
imputer.fit(df_pyspark).transform(df_pyspark).show()