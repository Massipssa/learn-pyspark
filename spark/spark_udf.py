from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


"""
- They are used to extend the PySpark build in capabilities. 
- Very expensive operation  
"""


def upper_case(str_value): 
    return str_value.upper()


spark = SparkSession.builder.appName('udf-example').getOrCreate()
columns = ["id", "name"]
data = [("1", "name one"), ("2", "name two"), ("3", "name three")]

df = spark.createDataFrame(data=data,schema=columns)
df.show(truncate=False)

# define udf 
upper_case_udf = udf(lambda z: upper_case(z), StringType())

# use udf with dataframe 
df.select(col("name"), upper_case_udf(col("name")).alias("name")).show(truncate=False)

# register UDF to use in sql  
spark.udf.register("upper_case_udf", upper_case, StringType())
df.createOrReplaceTempView("test_table")
spark.sql("select id, upper_case_udf(name) as name from test_table").show(truncate=False)

# use annotation
@udf(returnType=StringType()) 
def upperCase(str):
    return str.upper()

df.select(col("name"), upperCase(col("name")).alias("name2")).show(truncate=False)



