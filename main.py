import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Folder Imports
from input.Reader import Reader
from transformations.InstructorPreprocessService import InstructorPreprocessService
from transformations.InstructorService import InstructorService
from transformations.EmailService import EmailService
from transformations.NumberService import NumberService
from transformations.CourseService import CourseService
from transformations.OfferingPreprocessService import OfferingPreprocessService
from transformations.OfferingService import OfferingService
from transformations.MeetingService import MeetingService

df = Reader("C:\PythonScripts\examples.json").getDf()

instructorPreprocessedDf = InstructorPreprocessService(df).run()
instructorTableDf = InstructorService(instructorPreprocessedDf).run()
emailTableDf = EmailService(instructorPreprocessedDf).run()
numberTableDf = NumberService(instructorPreprocessedDf).run()
courseTableDf = CourseService(df).run()
offeringPreprocessedDf = OfferingPreprocessService(df, courseTableDf).run()
offeringTableDf = OfferingService(offeringPreprocessedDf).run()
meetingTableDf = MeetingService(offeringPreprocessedDf).run()

meetingTableDf.show(20, False)


