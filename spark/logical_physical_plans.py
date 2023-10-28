from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


spark = SparkSession.builder\
            .master("local[*]")\
            .appName("spark-execution-plan-demo")\
            .getOrCreate()

persons = spark.createDataFrame([
                    (0, "person_1", 0, [100]),\
                    (1, "person_2", 1, [500, 250, 100]),\
                    (2, "person_3", 1, [250, 100])])\
                .toDF("id", "name", "graduate_program", "spark_status")

graduate_programs = spark.createDataFrame([
                    (0, "Masters", "School of Information", "UC Berkeley"),\
                    (2, "Masters", "EECS", "UC Berkeley"),\
                    (1, "Ph.D.", "EECS", "UC Berkeley")])\
                .toDF("id", "degree", "department", "school")
                 
join_result = persons.join(graduate_programs, \
            persons["graduate_program"] == graduate_programs["id"], "inner") \
            .orderBy("name")

join_result.explain(mode="extended")            
join_result.explain(mode="formatted")            
