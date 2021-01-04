import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service

# Assumes df is base dataframe returned from Reader object
class InstructorPreprocessService(Service):
    def removeNullStructs(self, df):
        return df.select("instructor").where(df["instructor"].isNotNull())

    def addPrimaryKey(self, df):
        instructorIdWindow = Window.orderBy(F.col("instructor.first_name"))
        return df.withColumn("id", F.row_number().over(instructorIdWindow))\
                 .select("id", "instructor")


    def run(self):
        return self.addPrimaryKey(self.removeNullStructs(self.df))
