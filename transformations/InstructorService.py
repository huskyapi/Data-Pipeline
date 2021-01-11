import pyspark
from pyspark.sql import functions as F, DataFrame
from .Service import Service
from .helper.filters import removeNullFields
from .helper.formatters import formatBlanks

# Assumes df is dataframe returned from Reader
class InstructorService(Service):
    def process(self, df: DataFrame):
        return df.withColumn("email", df.instructor.email[0])\
                      .withColumn("phoneNo", df.instructor.phone_number[0])\
                      .dropDuplicates(["email"])\
                      .where(F.col("email").isNotNull())\
                      .where(F.col("email") != "")\
                      .select("email", F.col("instructor.first_name").alias("fName"),
                                       F.col("instructor.last_name").alias("lName"),
                                       F.col("instructor.middle_name").alias("mName"),
                                       "phoneNo")

    def run(self):
        return formatBlanks(self.process(self.df), "mName")
