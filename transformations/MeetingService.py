import pyspark
from pyspark.sql import functions as F
from pyspark.sql import Window
from .Service import Service

# df should be the dataframe returned from OfferingPreprocessService
class MeetingService(Service):
    def run(self):
        meetingColumns = {
            "cId": "cId",
            "meetings.start_time": "sTime",
            "meetings.end_time": "eTime",
            "meetings.meeting_days": "days",
            "meetings.room_building": "rBuilding",
            "meetings.room_number": "rNo"
        }

        return self.df.select(F.col("courseId").alias("cId"), "meetings") \
                      .withColumn("meetings", F.explode("meetings")) \
                      .select([F.col(jsonPath).alias(meetingColumns[jsonPath]) for jsonPath in meetingColumns])