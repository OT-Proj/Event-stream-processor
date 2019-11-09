from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import os
from Consts import *
from Queries import *


class EventStreamProcessor:
    # this class offers Spark queries to process an event stream.
    # queries were made to run independently.
    # currently, our spark DataFrame is single node implementation.
    # please refer to the documentation for each query's description.

    def __init__(self, input_stream):
        # initialize environment variable HADOOP_HOME
        os.environ["HADOOP_HOME"] = os.getcwd()

        # initialize spark session instance
        self.spark = SparkSession \
            .builder \
            .appName("Elementor assignment") \
            .getOrCreate()

        # build a dataframe based on the input_stream
        self.spark.read.json(input_stream, multiLine=True).createOrReplaceTempView(EVENT_STREAM_DATAFRAME_NAME)

    def top10Convertors(self, out_file):
        query = self.spark.sql(TOP_10_CONVERSERS_QUERY)
        query.write.mode("overwrite").json(out_file)

    def fastConvertingUsers(self, out_file):
        query = self.spark.sql(FAST_CONVERTING_USERS)
        query.write.mode("overwrite").json(out_file)

    def avgConversionDistance(self, out_file):
        query = self.spark.sql(AVG_CONVERSION_DISTANCE)
        query.write.mode("overwrite").json(out_file)

    def unsuccessfulConversionRatio(self, out_file):
        query = self.spark.sql(UNSUCCESSFUL_CONVERSIONS_RATIO)
        query.write.mode("overwrite").json(out_file)

    def patternRecognition(self, out_file, pattern_lst):
        # convert pattern into string
        pattern_str = ""
        for url in pattern_lst:
            pattern_str += url + ","
        pattern_str = pattern_str[0:-1]  # remove last ","

        # verify event stream order, and filter only "in_page" events
        temp_df = self.spark.sql(PATTERN_RECOGNITION_PREPARATION)

        # aggregate travel path strings from every session
        temp_df = temp_df.groupBy("session_id", "user_id").agg(
            sf.concat_ws(",", sf.collect_list(sf.col("url"))).alias("travel_pattern"))

        # filter only travel patterns that contain our searched pattern
        result = temp_df.filter(sf.col("travel_pattern").contains(pattern_str))
        result.write.mode("overwrite").json(out_file)



