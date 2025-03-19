from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import os
from abc import ABC, abstractmethod


class SparkBase(ABC):
    def __init__(self):
        # Define SparkSession
        self.spark_session = (
            SparkSession.builder.appName("HelloSpark")
            .master("local[*]")  # Chạy trên local, tận dụng tất cả CPU core
            .getOrCreate()
        )
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def get_spark_session(self):
        return self.spark_session
