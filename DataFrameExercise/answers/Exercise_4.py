# 🔹 Exercise 4: Wide Transformations
# 1️⃣ Aggregate data (groupBy + agg)
# Calculate the total number of goals per team
# 2️⃣ Join datasets (join)
# Merge match with score using match_id

from .Exercise_Base import ExerciseBase as Ex
import os
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from .Exercise_2 import Exercise2
from .Exercise_3 import Exercise3
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format
from pyspark.sql.functions import current_timestamp


# Work with match_pp.csv, score_pp.csv
class Exercise4(Ex):
    def __init__(self):
        super().__init__()

    # 1️⃣ Aggregate data (groupBy + agg)
    # Calculate the total number of goals per team
    def mapScore(self, df: DataFrame) -> DataFrame:
        return df.groupBy("team_id").agg({"goals": "sum"})

    # 2️⃣ Join datasets (join)
    # Merge match with score using match_id
    def joinMatches(self, match: DataFrame, score: DataFrame) -> DataFrame:
        match = match.alias("m")
        score = score.alias("s")
        return match.join(score, match["match_id"] == score["match_report"], "inner")


if __name__ == "__main__":
    ex1 = Exercise1()
    ex4 = Exercise4()
    score_df = ex1.read_exercise_file("score_pp.csv")
    match_df = ex1.read_exercise_file("match_pp.csv")
    match_join_score = ex4.joinMatches(match_df, score_df)
    select = match_join_score.select("match_id", "team_id", "goals", "attendance")
    w_f = ex1.write_csv(select, "match_join_score.csv")
    rs = ex1.read_exercise_file("score_cleaned.csv")
    rs.show(5)
