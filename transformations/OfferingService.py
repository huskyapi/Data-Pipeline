import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# df should be the dataframe returned from OfferingPreprocessService
class OfferingService(Service):
    def run(self):
        offeringTableColumns = {
            "courseId": "cId",
            "quarter": "quarter",
            "year": "oYear",
            "section": "oSection",
            "description": "description",
            "add_code_required": "addCodeRequired",
            "current_size": "curSize",
            "max_size": "maxSize",
            "type": "oType",
            "lower_credits": "lowerCreds",
            "upper_credits": "upperCreds",
            "sln": "sln",
            "general_education.C": "c",
            "general_education.DIV": "div",
            "general_education.IS": "indsoc",
            "general_education.NW": "nw",
            "general_education.QSR": "qsr",
            "general_education.VLPA": "vlpa",
            "general_education.W": "w"
        }

        return self.df.select([F.col(jsonPath).alias(offeringTableColumns[jsonPath]) for jsonPath in offeringTableColumns])