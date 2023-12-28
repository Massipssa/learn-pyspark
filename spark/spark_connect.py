from pyspark.sql import SparkSession

if __name__ == "__main__":


    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    
    data = [(0, "person1", 28, "Doctor", "Male"),
            (1, "person2", 35, "Singer", "Female"),
            (3, "person3", 42, "Teacher", "Male")
    ]
    columns = ["id", "name", "age", "job_title", "sex"]
    df = spark.createDataFrame(data, columns)
    df.show()
    spark.stop()