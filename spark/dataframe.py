from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import (asc, asc_nulls_first, asc_nulls_last, col,
                                   column, desc, desc_nulls_first,
                                   desc_nulls_last, exp, lit, max)
from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               StructField, StructType)

from src.spark.context import sc, spark


def read_csv(path):
    return spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv(path)


def explain_df(path):

    sorted_df = read_csv(path)\
        .sort("count")

    sorted_df.explain()
    sorted_df.show(10)


def cache_and_persist(df: DataFrame):
    df.cache()
    df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_DESER)
    df.explain(mode="formatted")


def df_sql(df: DataFrame):
    df.createOrReplaceTempView("data_2015")
    max_1 = spark.sql("""select max(count) from data_2015""")
    max_2 = df.select(max("count"))


def top_five_destinations(df: DataFrame):
    df.groupby("DEST_COUNTRY_NAME")\
        .sum("count")\
        .withColumnRenamed("sum(count)", "destination_total")\
        .orderBy(desc("destination_total"))\
        .limit(5)\
        .show()


def set_schema(path: str):
    schema = StructType([
        StructField('col_1', IntegerType(), False),
        StructField('col_2', StringType(), True),
        StructField('col_3', BooleanType, True),
    ])
    spark.read\
        .format('json')\
        .schema(schema)\
        .load(path)


def column_expr():
    """
    Column are expressions
    The three following statements are equivalent
    """
    res_1 = exp("(((col1 + 5) * 200) - 6) < otherCol")
    res_2 = (((exp("col1") + 5) * 200) - 6) < exp("otherCol")
    res_3 = (((col("col1") + 5) * 200) - 6) < col("otherCol")
    res_4 = (((column("col1") + 5) * 200) - 6) < column("otherCol")


def rows():
    my_row = Row("Hello", None, 0, True)
    print(my_row[0])
    print(my_row[2])


def df_transformations(df: DataFrame):
    # add literal
    df.selectExpr("*", lit(1).alias("col_one"))
    # add column
    df.withColumn("new_col", exp("col_1 == col_2"))
    # rename a column
    df.withColumnRenamed("old_col", "new_col")
    # escape reserved chars
    df.selectExpr("`the-name-col`")
    # drop column or multiple columns
    df.drop("col_1", "col_2")
    # cast column
    df.withColumn("col_1", col("cast_col_1").cast("long"))
    # sort
    df.sort("col_1")
    df.orderBy(asc("col_1"), desc("col_2"))
    # specify where nulls will appear
    df.orderBy(asc_nulls_first("col_1"), asc_nulls_last("col_2"))
    df.orderBy(desc_nulls_first("col_1"), desc_nulls_last("col_2"))
    df.sortWithinPartitions("col_1")
    # limit
    df.limit(5)
    # partition and coalesce
    df.repartition(4)
    df.coalesce(5)


def read_df(path: str, mode: str):
    """
    mode:
        - permissive (default): sets all fields to null when it encounters corrupted records
         and places them to string column called _corrupt_record
        - dropMalFormed: drop rows that contain malformed records
        - failFast: Fails immediate upon encountering corrupted records
    """
    return spark.read\
        .format("csv")\
        .option("mode", mode)\
        .load(path)


def write_df(df: DataFrame, mode):
    """
    mode:
        - append
        - overWrite
        - ErrorIfExists (default)
        - ignore
    """

    df.write\
        .format("csv")\
        .partitionBy()\
        .bucketBy()\
        .sortBy()\
        .option("path", "path/to/save/location")\
        .save(mode=mode)

    df.write\
        .parquet(path="")


def read_jdbc():
    spark.read\
        .format("jdbc")\
        .option("url")\
        .option("user")\
        .option("password")\
        .option("dbtable")\
        .option("driver")\
        .option("partitionColumn") \
        .option("lowerBound") \
        .option("upperBound") \
        .option("numPartitions")\
        .option("fetchsize")


def read_csv(df_path):

    return spark.read\
        .csv(df_path)

# toto
if __name__ == '__main__':
    path = "file:///E:/DEV/dev-python/learn-python/tests/data/flight-data/csv"
    print(sc.getConf().getAll())
    print(read_csv(path).rdd.getNumPartitions())
