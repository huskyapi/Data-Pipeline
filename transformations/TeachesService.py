import pyspark
from pyspark.sql import functions as F
from .Service import Service

# df should be the DataFrame returned from Reader
class TeachesService(Service):
    def run(self):
        return self.df.withColumn("email", self.df.instructor.email[0]) \
                      .withColumn("sId", self.df.id) \
                      .select("email", "sId") \
                      .where(F.col("email").isNotNull()) \
                      .where(F.col("email") != "")