from pyspark import SparkConf
from pyspark.sql import SparkSession

# spark.sql.shuffle.partitions: is set to 200 by default
# spark.sql.crossJoin.enable
# spark.files.maxPartitionBytes: default 128 MB, the size of block in HDFS
# spark.default.parallelism

conf = SparkConf()\
    .setAppName("test-app")\
    .setMaster('local[*]')\


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

sc = spark.sparkContext


sc.parallelize([1, 2, 3])

