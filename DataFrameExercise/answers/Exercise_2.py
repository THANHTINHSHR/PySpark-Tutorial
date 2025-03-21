# 🔹 Exercise 2: Data Cleaning
# 1️⃣ Remove rows with missing values
# 2️⃣ Standardize data types (convert numbers to Integer, dates to Date, etc.)
# 3️⃣ Trim extra spaces in player names and team names

from .Exercise_Base import ExerciseBase as Ex
import os
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from pyspark.sql.functions import regexp_replace


# Work with score_raw.csv
class Exercise2(Ex):
    def __init__(self):
        super().__init__()

    # Remove rows with missing values
    def remove_rows_with_missing_values(self, df: DataFrame) -> DataFrame:
        df = df.na.drop()
        return df

    # 2️⃣ Standardize data types (convert numbers to Integer, dates to Date, etc.)
    def standardize_data_types(self, df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            if "season" in col_name.lower() or "attendance" in col_name.lower():
                df = df.withColumn(
                    col_name, regexp_replace(df[col_name], ",", "").cast("int")
                )
        return df

    # 3️⃣ Trim extra spaces in score, remove special characters
    def text_standardization(self, df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            df = df.withColumn(col_name, trim(df[col_name]))
        return df


if __name__ == "__main__":
    ex1 = Exercise1()
    ex2 = Exercise2()
    score_df = ex1.read_exercise_file("score_raw.csv")
    ex2_df = ex2.remove_rows_with_missing_values(score_df)
    ex1.write_csv(ex2_df, "score_cleaned.csv")
