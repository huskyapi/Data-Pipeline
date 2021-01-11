import pyspark
from pyspark.sql import functions as F, DataFrame
from pyspark.sql import Window
from .Service import Service

# df should be the dataframe returned from OfferingPreprocessService
class MeetingService(Service):
    def splitMeetings(self, df: DataFrame) -> DataFrame:
        return df.withColumn("meetings", F.explode("meetings"))

    def grabColumns(self, df: DataFrame) -> DataFrame:
        meetingCols = {
            "id": "sId",
            "meetings.start_time": "sTime",
            "meetings.end_time": "eTime",
            "meetings.meeting_days": "days",
            "meetings.room_building": "rBuilding",
            "meetings.room_number": "rNo"
        }

        return df.select([F.col(jsonField).alias(meetingCols[jsonField]) for jsonField in meetingCols])

    def generatePrimaryKey(self, df: DataFrame):
        meetingWindow = Window.orderBy(F.col("sId"))
        return df.withColumn("id", F.row_number().over(meetingWindow)) \
                 .select("id", "sId", "sTime", "eTime", "days", "rBuilding", "rNo")

    def run(self) -> DataFrame:
        return self.generatePrimaryKey(self.grabColumns(self.splitMeetings(self.df)))