from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkWideTransformations(SparkBase):
    def __init__(self):
        super().__init__()

    # Groups DataFrame by column(s) and applies aggregation
    def groupBy(self, df: DataFrame, *cols):
        return df.groupBy(*cols)

    # Joins two DataFrames on a given column
    def join(self, df1: DataFrame, df2: DataFrame, on: str, how: str = "inner"):
        return df1.join(df2, on=on, how=how)

    # Orders DataFrame by column(s)
    def orderBy(self, df: DataFrame, *cols):
        return df.orderBy(*cols)

    # Aggregates data after grouping
    def agg(self, df: DataFrame, *exprs):
        return df.agg(*exprs)

    # Reduces data by key (RDD-like behavior)
    def reduceByKey(self, df: DataFrame, key: str, func):
        return df.rdd.map(lambda row: (row[key], row)).reduceByKey(func).toDF()
