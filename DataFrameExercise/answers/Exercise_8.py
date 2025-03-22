# 🔹 Exercise 8: Caching & Optimization
# 1️⃣ Cache a large DataFrame and measure query time before and after caching
# 2️⃣ Use broadcast join to optimize a join operation between a small dataset (player_raw) and a large dataset (match_raw)

from .Exercise_Base import ExerciseBase as Ex
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from pyspark.sql import functions as F
import time
from pyspark.sql.functions import col


class Exercise8(Ex):
    def __init__(self):
        super().__init__()

    def measure_query_time(self, df: DataFrame):
        start = time.time()
        df_filtered = df.filter(col("attendance") > 10000)
        end = time.time()
        print(f"Time taken to query (before cache): {end - start} seconds")

        df.cache()  # Save df to RAM
        start = time.time()
        df_filtered = df.filter(col("attendance") > 10000)
        end = time.time()
        print(f"Time taken to query (after cache): {end - start} seconds")

        df.unpersist()  # Remove df from RAM

    def broadcast_join(self, df1_big: DataFrame, df2_small: DataFrame):
        df1_big = df1_big.withColumnRenamed("match report", "match_report")
        df2_small = df2_small.withColumnRenamed("match id", "match_id")

        df_joined = df1_big.join(
            F.broadcast(df2_small), col("match_report") == col("match_id"), "inner"
        )
        return df_joined


if __name__ == "__main__":
    ex1 = Exercise1()
    ex8 = Exercise8()

    input_file_big = "score_pp.csv"
    input_file_small = "match_pp.csv"

    ex8_df_big = ex1.read_exercise_file(input_file_big)
    ex8_df_small = ex1.read_exercise_file(input_file_small)

    # Test method measure_query_time
    ex8.measure_query_time(ex8_df_big)

    # Test method broadcast_join
    broadcast_join = ex8.broadcast_join(ex8_df_big, ex8_df_small)
    broadcast_join.show()
