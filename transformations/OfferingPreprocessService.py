import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .helper.filters import removeBlankFields
from .helper.formatters import formatBlanks

# Assumes readerDf is the df returned from Reader and courseDf is the df returned from CourseService
class OfferingPreprocessService():
    def __init__(self, leftDf, rightDf):
        self.readerDf = leftDf
        self.courseDf = rightDf

    def run(self):
        return self.readerDf.join(self.courseDf, (self.readerDf.department == self.courseDf.department) & (self.readerDf.number == self.courseDf.courseNo))
