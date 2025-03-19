from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import os
from spark_base import SparkBase
from pyspark.sql import DataFrame


class SparkTransformations(SparkBase):
    def __init__(self):
        super().__init__()

    def filter(self, df: DataFrame, condition):
        return df.filter(condition)

    def where(self, df: DataFrame, condition):
        return df.where(condition)

    def select(self, df: DataFrame, columns):
        return df.select(columns)

    def drop(self, df: DataFrame, columns):
        return df.drop(columns)
