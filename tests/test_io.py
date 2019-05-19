import geonurse


def test_read_file(spark_session, test_data_conversion_path):
    rdd = geonurse.read_file(
        spark_session,
        test_data_conversion_path
    )
    assert rdd.count() == 462
