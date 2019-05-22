import geopandas
import geonurse
from geonurse.geordd import GeoRDD
from geonurse.geodataframe import GeoDataFrame


class Test_GeoDataFrame:

    def test_init(self, spark_session, test_data_conversion_path):
        geoDf = geonurse.read_file(spark_session, test_data_conversion_path).toGeoDF()

        assert geoDf.count() == 462
        assert isinstance(geoDf, GeoDataFrame)

    def test_geoRdd(self, spark_session, test_data_conversion_path):
        geoRdd = (
            geonurse.read_file(spark_session, test_data_conversion_path)
                .toGeoDF()
                .geoRdd
        )

        assert geoRdd.count() == 462
        assert isinstance(geoRdd, GeoRDD)

    def test_toGeoPandas(self, spark_session, test_data_conversion_path):
        gdf = (
            geonurse.read_file(spark_session, test_data_conversion_path)
                .toGeoDF()
                .toGeoPandas()
        )

        assert len(gdf) == 462
        assert isinstance(gdf, geopandas.GeoDataFrame)
