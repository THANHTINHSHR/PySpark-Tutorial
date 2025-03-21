# 🔹 Exercise 3: Narrow Transformations
# 1️⃣ Add new columns (withColumn)
# Add a Date column with value current timestamp to all datasets
# Convert player and team names to uppercase
# 2️⃣ Filter data (filter / where)
# Get a list of matches with more than 50,000 spectators
# 3️⃣ Select and rename columns (select / alias)
# Extract Match Report and rename it to match_report
from .Exercise_Base import ExerciseBase as Ex
import os
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from .Exercise_2 import Exercise2
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format
from pyspark.sql.functions import current_timestamp


# Work with score_raw.csv
class Exercise3(Ex):
    def __init__(self):
        super().__init__()

    # 1️⃣ Add new columns (withColumn)
    # Add a Date column with value current timestamp to all datasets
    def addNewDateColumn(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "Date", date_format(current_timestamp(), "yyyy-MM-dd-HH:mm:ss")
        )

    # 2️⃣ Filter data (filter / where)
    # Get a list of matches with more than 50,000 spectators
    def filterMatches(self, df: DataFrame) -> DataFrame:
        return df.filter(df["Attendance"] > 50000)

    # 3️⃣ Select and rename columns (select / alias)
    # Extract Match Report and rename it to match_report
    def selectColumns(self, df: DataFrame) -> DataFrame:
        return df.select(df["Match Report"].alias("match_report"))

    def renameColumns(self, df: DataFrame) -> DataFrame:
        return df.withColumnRenamed("Match Report", "match_report")


if __name__ == "__main__":
    ex1 = Exercise1()
    ex2 = Exercise2()
    ex3 = Exercise3()
    score_df = ex1.read_exercise_file("score_raw.csv")
    ex2_df = ex2.standardize_data_types(score_df)
    ex2_df = ex3.addNewDateColumn(ex2_df)
    ex2_df = ex3.filterMatches(ex2_df)
    ex1.write_csv(ex2_df, "score_cleaned.csv")
    score_cleaned = ex1.read_exercise_file("score_cleaned.csv")
    score_cleaned.show(5)
