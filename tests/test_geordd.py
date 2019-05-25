from geonurse.geordd import GeoRDD


class Test_GeoRDD:

    def test_init(self, spark_context, test_data_geordd):
        rdd = spark_context.parallelize(test_data_geordd)
        geoRdd = GeoRDD(rdd)

        assert isinstance(geoRdd, GeoRDD)
