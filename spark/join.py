from pyspark.sql import DataFrame

from src.spark.context import spark

person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")

graduate_program = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")

spark_status = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

join_expression = person["graduate_program"] == graduate_program["id"]


def inner_join() -> DataFrame:
    """
    Keeps rows with keys that exists in left and right datasets
    """
    person.join(graduate_program, join_expression, "inner")\
        .show()


def outer_join():
    """
    Keeps rows with keys in either left or right datasets
    If there is no equivalent row in either left or right, inserts null
    """
    person.join(graduate_program, join_expression, "outer") \
        .show()


def left_outer_join():
    """
    Keeps rows with keys in left dataset
    """
    person.join(graduate_program, join_expression, "left_outer") \
        .show()


def right_outer_join():
    """
    Keeps rows with keys right dataset
    """
    person.join(graduate_program, join_expression, "right_outer") \
        .show()


def left_semi_joins():
    pass


def left_anti_joins():
    pass


def natural_join():
    pass


def cross_join():
    """
    Cartesian
    Match every row in left dataset with row in right dataset
    """
    person.crossJoin(graduate_program).show()


if __name__ == '__main__':
    print("*" * 10, "Inner join", "*" * 10)
    inner_join()
    print("*" * 10, "Outer join", "*" * 10)
    outer_join()
    print("*" * 10, "Left outer join", "*" * 10)
    left_outer_join()
    print("*" * 10, "Right outer join", "*" * 10)
    right_outer_join()
    print("*" * 10, "Natural join", "*" * 10)
    natural_join()
    print("*" * 10, "Cross join", "*" * 10)
    cross_join()

