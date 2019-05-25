import geonurse
import geonurse.geordd


# this test may take a while
def test_read_file(spark_session, test_data_path):

    for filename in test_data_path:
        rdd = geonurse.read_file(spark_session, filename)

        assert rdd.count() > 0
        assert isinstance(rdd, geonurse.geordd.GeoRDD)
