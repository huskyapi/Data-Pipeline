import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service
from pyspark.sql.types import IntegerType
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# Assumes df is the dataframe returned from Reader
class PreprocessService(Service):
    def adjust(self, df):
        return df.withColumn("number", F.col("number").cast(IntegerType())).dropDuplicates()

    def generatePrimaryKey(self, df):
        offeringWindow = Window.orderBy(F.col("name"))
        return df.withColumn("id", F.row_number().over(offeringWindow))
    
    def removeNullInstructors(self, df):
        return df.where(F.col("instructor").isNotNull())
        
    def run(self):
        return self.removeNullInstructors(self.generatePrimaryKey(self.adjust(self.df)))