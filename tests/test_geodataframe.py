import pytest
import geopandas

import geonurse
from geonurse.geordd import GeoRDD
from geonurse.geodataframe import GeoDataFrame


class Test_GeoDataFrame:

    def test_init(self, spark_session, test_data_geodataframe_path):
        geoDf = geonurse.read_file(spark_session, test_data_geodataframe_path).toGeoDF()
        assert geoDf.count() > 0
        assert isinstance(geoDf, GeoDataFrame)

    def test_geoRdd(self, input_polygon_geodf_wkt, input_polygon_geodf_wkb):
        geoRdd = input_polygon_geodf_wkt.geoRdd
        assert geoRdd.count() > 0
        assert geoRdd.count() == input_polygon_geodf_wkt.count()
        assert isinstance(geoRdd, GeoRDD)

        geoRdd = input_polygon_geodf_wkb.geoRdd
        assert geoRdd.count() > 0
        assert geoRdd.count() == input_polygon_geodf_wkb.count()
        assert isinstance(geoRdd, GeoRDD)

    def test_toGeoPandas(self, input_polygon_geodf_wkt, input_polygon_geodf_wkb):
        gdf = input_polygon_geodf_wkt.toGeoPandas('wkt')
        assert len(gdf) == input_polygon_geodf_wkt.count()
        assert len(gdf) > 0
        assert isinstance(gdf, geopandas.GeoDataFrame)

        gdf = input_polygon_geodf_wkb.toGeoPandas('wkb')
        assert len(gdf) == input_polygon_geodf_wkb.count()
        assert len(gdf) > 0
        assert isinstance(gdf, geopandas.GeoDataFrame)

    def test_toGeoPandas_ValueError(self, input_polygon_geodf_wkt):
        with pytest.raises(ValueError, match='method is not available'):
            input_polygon_geodf_wkt.toGeoPandas('invalid_method')
