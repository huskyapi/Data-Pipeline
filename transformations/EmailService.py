import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# Assumes df is dataframe returned from InstructorPreprocessService
class EmailService(Service):
    def run(self):
        return removeBlankFields(self.df.withColumn("email", F.explode("instructor.email"))
                                         .select(F.col("id").alias("instructorId"), "email"), "email")