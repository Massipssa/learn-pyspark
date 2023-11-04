import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, first, last, size


spark = SparkSession.builder.appName('conditions-app').getOrCreate()


data = [(1, "Alice", 12), (2, "Bob", 30), (3, "Charlie", 25), (4, "David", 80), (1, "Alice", 12), (1, "Alice", 18)]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

print("*** where ***")
filtered_df = df.where(df.id > 2)
filtered_df.show()

filtered_df = df.withColumn("age_group", when(df.age < 18, lit("Young")).otherwise(lit("Adult")))
filtered_df.show()

 
print("*** distinct and dropDuplicates ***")
df.distinct().show()
df.dropDuplicates(["id"]).show()

# first and last 
df.select(first("name"), last("name")).show()

# sample 
df.sample(0.1, 123, True).show()


data = [(1, ["test1", "test2", "test3"]), (2, ["test1", "test2"])]
columns = ["id", "elements"]
test_df = spark.createDataFrame(data, columns)

# size
test_df.filter(size("elements") == 3).show()

# s