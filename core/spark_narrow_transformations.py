from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkNarrowTransformations(SparkBase):
    def __init__(self):
        super().__init__()

    # Applies an uppercase transformation to a column of a DataFrame
    def withColumn(self, df: DataFrame, col_name: str):
        return df.withColumn(col_name, upper(col(col_name)))

    # Filters rows based on a condition
    def filter(self, df: DataFrame, condition):
        return df.filter(condition)

    # Selects specific columns from a DataFrame
    def select(self, df: DataFrame, *cols):
        return df.select(*cols)

    # Applies a function to each partition
    def mapPartitions(self, df: DataFrame, func):
        return df.rdd.mapPartitions(func).toDF()

    # Returns distinct rows from the DataFrame
    def distinct(self, df: DataFrame):
        return df.distinct()

    # Limits the number of rows in the DataFrame
    def limit(self, df: DataFrame, num: int):
        return df.limit(num)
