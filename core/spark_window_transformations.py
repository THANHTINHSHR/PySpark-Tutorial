from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper


class SparkWindowTransformations(SparkBase):
    def __init__(self):
        super().__init__()

    # Applies row number window function
    def rowNumber(self, df: DataFrame, partition_by: str, order_by: str):
        window_spec = Window.partitionBy(partition_by).orderBy(order_by)
        return df.withColumn("row_number", row_number().over(window_spec))

    # Applies rank window function
    def rank(self, df: DataFrame, partition_by: str, order_by: str):
        window_spec = Window.partitionBy(partition_by).orderBy(order_by)
        return df.withColumn("rank", rank().over(window_spec))

    # Applies dense rank window function
    def denseRank(self, df: DataFrame, partition_by: str, order_by: str):
        window_spec = Window.partitionBy(partition_by).orderBy(order_by)
        return df.withColumn("dense_rank", dense_rank().over(window_spec))

    # Applies lead window function
    def lead(self, df: DataFrame, partition_by: str, order_by: str, col_name: str):
        window_spec = Window.partitionBy(partition_by).orderBy(order_by)
        return df.withColumn("lead_value", lead(col(col_name)).over(window_spec))

    # Applies lag window function
    def lag(self, df: DataFrame, partition_by: str, order_by: str, col_name: str):
        window_spec = Window.partitionBy(partition_by).orderBy(order_by)
        return df.withColumn("lag_value", lag(col(col_name)).over(window_spec))
