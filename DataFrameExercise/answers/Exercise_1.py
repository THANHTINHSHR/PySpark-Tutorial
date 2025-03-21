# 🔹 Exercise 1: Read and Inspect Data
# 1️⃣ Read the CSV files into DataFrames with the following conditions:

# Include header
# Automatically infer schema (inferSchema=True)
# Display the first 5 rows
# 2️⃣ Inspect the DataFrame metadata:

# Print schema (printSchema())
# Get column names (columns)
# Check data types (dtypes)
# View data summary (describe())

from .Exercise_Base import ExerciseBase as Ex
import os
from pyspark.sql import DataFrame


class Exercise1(Ex):
    def __init__(self):
        super().__init__()

    def read_exercise_file(self, filename: str) -> DataFrame:
        """Read CSV file into a DataFrame with header and inferred schema."""
        file_path = os.path.join(self.current_dir, "..", "data_files", filename)
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def write_csv(self, df: DataFrame, filename):
        file_path = os.path.join(self.current_dir, "..", "data_files", filename)
        df.write.mode("overwrite").csv(file_path, header=True)

    def write_parquet(self, df: DataFrame, pafilenameth):
        file_path = os.path.join(self.current_dir, "..", "data_files", filename)
        df.write.mode("overwrite").parquet(file_path)

    def write_json(self, df: DataFrame, filename):
        file_path = os.path.join(self.current_dir, "..", "data_files", filename)
        df.write.mode("overwrite").json(file_path)


if __name__ == "__main__":
    ex1 = Exercise1()
    players_df = ex1.read_exercise_file("player_raw.csv")
    teams_df = ex1.read_exercise_file("score_raw.csv")

    players_df.show(5)
    teams_df.show(5)
    # Print Schema
    print("Player schema")
    players_df.printSchema()
    # Get column names
    player_columns = players_df.columns
    print("Player Columns:", player_columns)
    # Check data types
    players_df.dtypes
    # View data summary
    players_df.describe().show()
