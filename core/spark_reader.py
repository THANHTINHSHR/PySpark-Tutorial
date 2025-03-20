from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import Row
import os
from .spark_base import SparkBase  # Sử dụng import tương đối
from pyspark.sql import DataFrame


class SparkReader(SparkBase):
    def __init__(self):
        super().__init__()
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def read_csv(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        return self.spark_session.read.csv(relative_path, header=True, inferSchema=True)

    def read_parquet(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        return self.spark_session.read.parquet(relative_path)

    def read_json(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        return self.spark_session.read.json(relative_path)


if __name__ == "__main__":
    sr = SparkReader()
    csv_file = "CSV/player.csv"
    # parquet_file = "parquet/input.parquet"
    # json_file = "json/input_file.json"
    df = sr.read_json(csv_file)
    df.show(5)
    print("sssssssssssssssssssssssssss")
