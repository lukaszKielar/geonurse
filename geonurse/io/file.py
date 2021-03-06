import fiona
from pyspark.sql import SparkSession
from geonurse.geordd import GeoRDD


def read_file(spark_session: SparkSession, filename: str, *args, **kwargs):
    with fiona.open(filename, 'r') as f:
        return GeoRDD(spark_session.sparkContext.parallelize(f))
