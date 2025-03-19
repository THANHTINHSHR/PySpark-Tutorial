from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
import os

# Tạo SparkSession
spark = (
    SparkSession.builder.appName("HelloSpark")
    .master("local[*]")  # Chạy trên local, tận dụng tất cả CPU core
    .getOrCreate()
)
spark.sparkContext.setLogLevel("DEBUG")  # Giảm log để dễ đọc

# Tạo DataFrame từ Row
df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)
df.show(2)

current_dir = os.path.dirname(
    os.path.abspath(__file__)
)  # Lấy đường dẫn thư mục hiện tại
player_path = os.path.join(
    current_dir, "..", "files", "CSV", "player.csv"
)  # Tạo đường dẫn file

df_player = spark.read.csv(player_path, header=True, inferSchema=True)
df_player.show(2)

# Ghi ra file Parquet
parquet_path = "../files/Parquet/input.parquet"
df_player.write.parquet(parquet_path, mode="overwrite")

# Đọc lại file Parquet
df_parquet = spark.read.parquet(parquet_path)
df_parquet.show(4)

# Chọn cột
df_parquet.select("Name", "Positions").show()

# Lọc dữ liệu
df_parquet.filter(df_parquet["Born"] > 2000).show()

# Nhóm và đếm
df_parquet.groupBy("Squad").count().show()

# Đọc file CSV khác
df_player = spark.read.csv("../files/CSV/player.csv", header=True, inferSchema=True)
df_player.show(2)
df_match = spark.read.csv("../files/CSV/input_file.csv", header=True, inferSchema=True)
df_match.show(4)

# Join DataFrames
df_match_player = df_player.join(
    df_match, df_player["Squad_ID"] == df_match["team_id"], "inner"
)
df_match_player.show(10)

# Lọc theo Squad
squad_id = "Juventus"
df_match_player.filter(df_match_player["Squad"] == squad_id).show()
