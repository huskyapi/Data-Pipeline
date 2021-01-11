from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

class Reader:
    def __init__(self, filePath):
        spark = SparkSession.builder \
                            .getOrCreate()

        df = spark.read.json(filePath)

        self.df = df.withColumn("number", F.col("number").cast(IntegerType())).dropDuplicates()

    def getDf(self):
        return self.df
