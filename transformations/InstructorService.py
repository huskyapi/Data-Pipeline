import pyspark
from pyspark.sql import functions as F
from .Service import Service
from .helper.filters import removeNullFields
from .helper.formatters import formatBlanks

# Assumes df is dataframe returned from InstructorPreprocessService
class InstructorService(Service):
    def grabColumns(self, df):
        instructorCols = {
            "id": "id",
            "instructor.first_name": "fName",
            "instructor.last_name": "lName",
            "instructor.middle_name": "mName",
        }

        return df.select([F.col(jsonField).alias(instructorCols[jsonField]) for jsonField in instructorCols])

    def cleanse(self, df):
        return formatBlanks(removeNullFields(df, "mName"), "mName")

    def run(self):
        return self.cleanse(self.grabColumns(self.df))
