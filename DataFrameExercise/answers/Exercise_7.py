# 🔹 Exercise 7: SQL Operations
# 1️⃣ Register all DataFrames as SQL tables (createOrReplaceTempView)
# 2️⃣ Write SQL queries:
# Retrieve the players with the highest goal count
# Get the top 5 teams with the most played match
# Calculate the average number of matches each team plays
from .Exercise_Base import ExerciseBase as Ex
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from pyspark.sql import functions as F


class Exercise7(Ex):
    def __init__(self):
        super().__init__()

    # Retrieve the players with the highest goal count

    def retrieve_players(self, df: DataFrame) -> DataFrame:
        max_goals = df.agg(F.max("goals")).collect()[0][0]
        player_sql_df = df.select("player_id", "goals").filter(
            F.col("goals") == max_goals
        )
        return player_sql_df

    # Get the top 5 teams with the most played match
    def top_5_teams_with_most_matches(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("Name")
            .agg(F.sum("Matches Played").alias("total_matches"))
            .orderBy(F.desc("total_matches"))
            .limit(5)
        )


if __name__ == "__main__":
    ex1 = Exercise1()
    ex7 = Exercise7()
    input_file = "match_pp.csv"
    output_file = "match_goals.csv"
    ex7_df = ex1.read_exercise_file(input_file)

    # toDo: rank players by goal count within each team
    ex7_df = ex7.retrieve_players(ex7_df)
    # save the result to a new file
    ex1.write_csv(ex7_df, output_file)
    # read and show the result
    rs = ex1.read_exercise_file(output_file)
    rs.show(10)
    input_file_2 = "squad_pp.csv"
    ex7_df_2 = ex1.read_exercise_file(input_file_2)
    print("Top 5 teams with most matches played")
    top_5_m = ex7.top_5_teams_with_most_matches(ex7_df_2)
    top_5_m.show(5)
