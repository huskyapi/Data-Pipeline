import pyspark
from pyspark.sql import functions as F

def removeNullFields(df, columnPath):
    return df.where(F.col(columnPath).isNotNull())

def removeBlankFields(df, columnPath):
    return df.where(F.col(columnPath) != '')

