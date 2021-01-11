import pyspark
from pyspark.sql import functions as F, DataFrame
from pyspark.sql import Window
from pyspark.sql.types import IntegerType
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# df should be the dataframe returned from OfferingPreprocessService
class OfferingService():
    def __init__(self, baseDf: DataFrame, courseDf: DataFrame):
        self.baseDf = baseDf
        self.courseDf = courseDf

    def distinguishIdCols(self, courseDf: DataFrame) -> DataFrame:
        return courseDf.withColumnRenamed("id", "cId")

    def joinDataFrames(self, df: DataFrame, toJoinDf: DataFrame) -> DataFrame:
        return df.join(toJoinDf, (df.department == toJoinDf.department) & (df.number == toJoinDf.courseNo))

    def grabColumns(self, df: DataFrame) -> DataFrame:
        offeringCols = {
            "id": "id",
            "cId": "cId",
            "quarter": "quarter",
            "email": "email",
            "year": "year",
            "section": "section",
            "description": "description",
            "add_code_required": "addCodeRequired",
            "current_size": "curSize",
            "max_size": "maxSize",
            "type": "type",
            "lower_credits": "lowerCreds",
            "upper_credits": "upperCreds",
            "sln": "sln"
        }

        return df.withColumn("email", df.instructor.email[0])\
                 .withColumn("year", df.year.cast(IntegerType()))\
                 .withColumn("current_size", df.current_size.cast(IntegerType())) \
                 .withColumn("max_size", df.max_size.cast(IntegerType())) \
                 .withColumn("lower_credits", df.lower_credits.cast(IntegerType())) \
                 .withColumn("upper_credits", df.upper_credits.cast(IntegerType())) \
                 .select([F.col(jsonField).alias(offeringCols[jsonField]) for jsonField in offeringCols])


    def run(self) -> DataFrame:
        return self.grabColumns(self.joinDataFrames(self.baseDf, self.distinguishIdCols(self.courseDf)))