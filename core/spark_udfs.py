from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkUDFS(SparkBase):
    def __init__(self):
        super().__init__()

    # Example UDF to convert a string to lowercase
    def registerUDF(self, spark):
        to_lower_udf = udf(lambda s: s.lower() if s else None, StringType())
        spark.udf.register("to_lower", to_lower_udf)
        return to_lower_udf
