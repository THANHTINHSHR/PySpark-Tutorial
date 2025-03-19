from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import os
from spark_base import SparkBase


class SparkReader(SparkBase):
    def __init__(self):
        super().__init__()

    def read_csv(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df = self.spark_session.read.csv(
            relative_path,
            header=True,
            inferSchema=True,
        )
        print(f"✅DEBUG path : {relative_path}")
        return df

    def read_parquet(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df = self.spark_session.read.parquet(
            relative_path,
            header=True,
            inferSchema=True,
        )
        print(f"✅DEBUG path : {relative_path}")
        return df

    def read_json(self, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df = self.spark_session.read.json(relative_path)
        print(f"✅DEBUG path : {relative_path}")
        return df


if __name__ == "__main__":
    sr = SparkReader()
    csv_file = "CSV/player.csv"
    parquet_file = "parquet/input.parquet"
    json_file = "json/input_file.json"
    df = sr.read_json(json_file)
    df.show(5)
