# 🔹 Exercise 5: UDFs (User Defined Functions)
# 2️⃣ Create a UDF to classify players based on their goal count:
from .Exercise_Base import ExerciseBase as Ex
from pyspark.sql import DataFrame
from .Exercise_1 import Exercise1
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class Exercise5(Ex):
    def __init__(self):
        super().__init__()

    @staticmethod
    def goals_scored(goals):
        if goals == 0:
            return "No Goal"
        elif goals == 1:
            return "Goal"
        elif goals == 2:
            return "Double Goal"
        elif goals == 3:
            return "Triple Goal"
        elif goals > 3:
            return "Hat Trick"
        return "Unknown"


udf_goals_scored = udf(Exercise5.goals_scored, StringType())
if __name__ == "__main__":
    ex1 = Exercise1()
    ex5 = Exercise5()
    input_file = "match_pp.csv"
    output_file = "match_goals.csv"
    ex5_df = ex1.read_exercise_file(input_file)

    # toDo: Add a new column "goals_scored" to the DataFrame
    ex5_df = ex5_df.withColumn("goals_scored", udf_goals_scored("goals"))
    # save the result to a new file
    ex1.write_csv(ex5_df, output_file)
    # read and show the result
    rs = ex1.read_exercise_file(output_file)
    rs.show(5)
