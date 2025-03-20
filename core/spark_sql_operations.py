from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


class SparkSQLOperations(SparkBase):
    def __init__(self):
        super().__init__()

    # Creates or replaces a temporary view for SQL queries
    def create_temp_view(self, df: DataFrame, view_name: str):
        df.createOrReplaceTempView(view_name)

    # Executes an SQL query and returns a DataFrame
    def execute_sql(self, spark: SparkSession, query: str):
        return spark.sql(query)

    # Shows the query execution plan
    def explain_query(self, df: DataFrame, extended: bool = False):
        return df.explain(extended)
