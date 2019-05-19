from geonurse.base import GeoRDD


class Test_GeoRDD:

    def test_init(self,
                  spark_context,
                  test_data_conversion_fiona):
        rdd = spark_context.parallelize(test_data_conversion_fiona)
        geoRdd = GeoRDD(rdd)
        assert geoRdd.count() == 462
