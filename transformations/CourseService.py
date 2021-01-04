import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# Assumes df is the dataframe returned from Reader object
class CourseService(Service):
    def removeDuplicates(self, df):
        return self.df.select("department", "number", "name").dropDuplicates()

    def addPrimaryKey(self, df):
        courseIdWindow = Window.orderBy(F.col("name"))
        return df.withColumn("courseId", F.row_number().over(courseIdWindow))\
                        .select("courseId", "department", F.col("number").alias("courseNo"), "name")

    def run(self):
        return self.addPrimaryKey(self.removeDuplicates(self.df))