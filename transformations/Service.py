import pyspark
from abc import ABC, abstractmethod
from pyspark.sql import functions as F, DataFrame


class Service(ABC):

    def __init__(self, df: DataFrame):
        self.df = df
        super().__init__()

    @abstractmethod
    def run(self):
        pass