import pyspark
from pyspark.sql import functions as F, DataFrame
from pyspark.sql import Window
from .Service import Service
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# Assumes df is the dataframe returned from Reader object
class CourseService(Service):
    def removeDuplicates(self, df: DataFrame):
        return self.df.dropDuplicates(["department", "number"])

    def generatePrimaryKey(self, df: DataFrame):
        courseWindow = Window.orderBy(F.col("department"))
        return df.withColumn("id", F.row_number().over(courseWindow))

    def grabColumns(self, df: DataFrame):
        coursesCols = {
            "id": "id",
            "department": "department",
            "number": "courseNo",
            "name": "name",
            "general_education.C": "c",
            "general_education.DIV": "div",
            "general_education.IS": "indsoc",
            "general_education.NW": "nw",
            "general_education.QSR": "qsr",
            "general_education.VLPA": "vlpa",
            "general_education.W": "w"
        }
        return df.select([F.col(jsonField).alias(coursesCols[jsonField]) for jsonField in coursesCols])

    def run(self):
        return self.grabColumns(self.generatePrimaryKey(self.removeDuplicates(self.df)))
