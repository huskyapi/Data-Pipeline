from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import os
from dotenv import load_dotenv

class Writer:
    def __init__(self, df):
        self.df = df

    def save(self, tableName):
        load_dotenv()

        url = "jdbc:postgresql://" + os.getenv("ENDPOINT") + ":" + os.getenv("PORT") + "/" + os.getenv("DB_NAME")
        properties = {
            "driver": "org.postgresql.Driver",
            "user": os.getenv("USER"),
            "password": os.getenv("PASSWORD")
        }


        self.df.write.jdbc(url=url, table=tableName, mode="append", properties=properties)