from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkAction(SparkBase):
    def __init__(self):
        super().__init__()

    # Returns the first row of the DataFrame
    def first(self, df: DataFrame):
        return df.first()

    # Collects all rows in the DataFrame as a list
    def collect(self, df: DataFrame):
        return df.collect()

    # Counts the number of rows in the DataFrame
    def count(self, df: DataFrame):
        return df.count()

    # Shows the DataFrame content
    def show(self, df: DataFrame, num: int = 20, truncate: bool = True):
        df.show(num, truncate)

    # Returns the first 'n' rows as a list
    def take(self, df: DataFrame, num: int):
        return df.take(num)
