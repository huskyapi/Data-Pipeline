import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Folder Imports
from input.Reader import Reader
from output.Writer import Writer
from transformations.PreprocessService import PreprocessService
from transformations.CourseService import CourseService
from transformations.InstructorService import InstructorService
from transformations.MeetingService import MeetingService
from transformations.OfferingService import OfferingService
from transformations.TeachesService import TeachesService

df = Reader("C:\PythonScripts\examples.json").getDf()

preprocessedDf = PreprocessService(df).run()

# Perform Data Transformations
instructorTableDf = InstructorService(preprocessedDf).run()
courseTableDf = CourseService(preprocessedDf).run()
offeringTableDf = OfferingService(preprocessedDf, courseTableDf).run()
teachesTableDf = TeachesService(preprocessedDf).run()
meetingTableDf = MeetingService(preprocessedDf).run()

instructorWriter = Writer(instructorTableDf)
courseWriter = Writer(courseTableDf)
offeringWriter = Writer(offeringTableDf)
teachesWriter = Writer(teachesTableDf)
meetingWriter = Writer(meetingTableDf)

# Generate Tables
instructorWriter.save("instructor")
courseWriter.save("course")
offeringWriter.save("offering")
teachesWriter.save("teaches")
meetingWriter.save("meeting")



