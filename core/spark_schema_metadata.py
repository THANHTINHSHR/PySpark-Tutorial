from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkSchemaMetadata(SparkBase):
    def __init__(self):
        super().__init__()

    # Prints the schema of the DataFrame
    def print_schema(self, df: DataFrame):
        df.printSchema()

    # Returns a list of column names
    def get_columns(self, df: DataFrame):
        return df.columns

    # Returns the schema as a StructType
    def get_schema(self, df: DataFrame):
        return df.schema

    # Returns the data types of each column
    def get_dtypes(self, df: DataFrame):
        return df.dtypes

    # Returns summary statistics of the DataFrame
    def get_summary(self, df: DataFrame):
        return df.summary()

    # Returns basic statistics for numeric columns
    def describe(self, df: DataFrame):
        return df.describe()

    # Explains the logical and physical plan of the DataFrame
    def explain(self, df: DataFrame, extended: bool = False):
        return df.explain(extended)
