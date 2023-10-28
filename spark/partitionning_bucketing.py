import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('partition-bucket-app').getOrCreate()

df = spark.createDataFrame(
    [("person_1", "20"), ("person_2", "56"), ("person_3", "89"), ("person_4", "20")],
    ["name","age"]
)

# Partitionning
df = df.repartition(4, "age")
print(df.rdd.getNumPartitions())

# Coalesce
df = df.coalesce(2)
print(df.rdd.getNumPartitions())

# PartitionBy
df.write.mode("overwrite").partitionBy("age").csv("data/output")

# Bucketing 
df.write.bucketBy(5, "age").saveAsTable("bucketed_table")

df.write.bucketBy(10, "age")\
    .sortBy("name")\
    .saveAsTable("sorted_bucketed_table")