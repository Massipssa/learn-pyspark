from src.spark.context import sc


def count_word(file_path, word):
    return sc.textFile(file_path) \
        .filter(lambda line: word in line.lower()) \
        .count()


def odds_numbers(numbers):
    return sc.parallelize(numbers, 2) \
        .filter(lambda item: item % 2 != 0)
