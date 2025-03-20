from pyspark.sql import Row
from spark_base import SparkBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


class SparkCachingOptimization(SparkBase):
    def __init__(self):
        super().__init__()

    # Caching & Optimization

    # Caches the DataFrame in memory
    def cache(self, df: DataFrame):
        return df.cache()

    # Persists the DataFrame with a given storage level
    def persist(self, df: DataFrame, storage_level):
        return df.persist(storage_level)

    # Removes the DataFrame from memory and disk
    def unpersist(self, df: DataFrame):
        return df.unpersist()

    # Saves a checkpoint of the DataFrame
    def checkpoint(self, df: DataFrame, spark: SparkSession, eager: bool = True):
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        return df.checkpoint(eager)

    # Repartitions the DataFrame into the specified number of partitions
    def repartition(self, df: DataFrame, num_partitions: int):
        return df.repartition(num_partitions)

    # Reduces the number of partitions in a DataFrame
    def coalesce(self, df: DataFrame, num_partitions: int):
        return df.coalesce(num_partitions)

    # Broadcasts a DataFrame to all worker nodes
    def broadcast(self, df: DataFrame):
        return broadcast(df)
