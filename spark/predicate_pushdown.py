from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("predicate-push-demo")\
            .getOrCreate()
    
    data = [(0, "person1", 28, "Doctor"),
            (1, "person2", 35, "Singer"),
            (3, "person3", 42, "Teacher")
    ]
    columns = ["id", "name", "age", "job_title"]
    df = spark.createDataFrame(data, columns)

    parquet_file_name = "persons_data.parquet"
    df.write.parquet(parquet_file_name, mode="overwrite")

    df_with_pd = spark.read \
        .parquet(parquet_file_name) \
        .filter("age > 25").select("name", "job_title")
    df_with_pd.explain(mode="extended")  

    
    # enable p
    import time 
    time.sleep(120)
    spark.stop()