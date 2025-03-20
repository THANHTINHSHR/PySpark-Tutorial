from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
import os


class ExerciseBase(ABC):
    def __init__(self):
        # Define SparkSession
        self.spark = (
            SparkSession.builder.appName("Exercise").master("local[*]").getOrCreate()
        )
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def get_spark_session(self):
        return self.spark_session
