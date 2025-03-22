# 🔹 Exercise 6: Window Functions
# 1️⃣ Rank players by goal count within each team (rank())
# 2️⃣ Calculate the total goals per team across different match rounds (sum() + partitionBy())

from .Exercise_Base import ExerciseBase as Ex
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import desc


class Exercise6(Ex):
    def __init__(self):
        super().__init__()

    # 1️⃣ Rank players by goal count within each team (rank())
    def rank_players(self, df: DataFrame):
        window_spec = Window.partitionBy("team_id").orderBy(desc("goals"))
        return df.withColumn("rank", dense_rank().over(window_spec))


if __name__ == "__main__":
    ex1 = Exercise1()
    ex6 = Exercise6()
    input_file = "match_pp.csv"
    output_file = "match_goals.csv"
    ex6_df = ex1.read_exercise_file(input_file)

    # toDo: rank players by goal count within each team
    ex6_df = ex6.rank_players(ex6_df)
    # save the result to a new file
    ex1.write_csv(ex6_df, output_file)
    # read and show the result
    rs = ex1.read_exercise_file(output_file)
    rs.show(10)
