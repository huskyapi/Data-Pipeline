import pyspark
from pyspark.sql import functions as F

def formatBlanks(df, columnPath):
    return df.withColumn(columnPath, F.when(F.col(columnPath) == '', None).otherwise(F.col(columnPath)))