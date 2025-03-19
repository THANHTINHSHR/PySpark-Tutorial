from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import os
from spark_base import SparkBase
from pyspark.sql import DataFrame


class SparkWriter(SparkBase):
    def __init__(self):
        super().__init__()

    def write_csv(self, df: DataFrame, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df.write.mode("overwrite").csv(relative_path, header=True)

    def write_parquet(self, df: DataFrame, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df.write.mode("overwrite").parquet(relative_path)

    def write_json(self, df: DataFrame, path):
        relative_path = os.path.join(self.current_dir, "..", "files", path)
        df.write.mode("overwrite").json(relative_path)

    def write_sql(
        df: DataFrame,
        url: str,
        table: str,
        mode: str = "overwrite",
        properties: dict = None,
    ):
        """
        Write a PySpark DataFrame to a SQL database.

        :param df: The DataFrame to write
        :param url: JDBC URL của database (ví dụ: "jdbc:mysql://localhost:3306/mydb")
        :param table: Tên bảng trong database
        :param mode: Write mode ('overwrite', 'append', 'ignore', 'error')
        :param properties: Dictionary chứa thông tin kết nối như user, password, driver
        """
        df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
        print(f"✅ DataFrame has been written to table `{table}` in database `{url}`.")


if __name__ == "__main__":
    sr = SparkReader()
    sw = SparkWriter()
    csv_file = "CSV/player.csv"
    parquet_file = "parquet/input.parquet"
    json_file = "json/input_file.json"

    p_df = sr.read_parquet(parquet_file)
    sw.write_parquet(p_df, "parquet/player_parquet")

    c_df = sr.read_csv(csv_file)
    sw.write_csv(c_df, "csv/player_parquet.csv")

    j_df = sr.read_json(json_file)
    sw.write_json(j_df, "json/player_parquet.json")
