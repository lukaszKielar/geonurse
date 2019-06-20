import pytest
from shapely.geometry import Polygon, MultiPolygon
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from geonurse.geordd import GeoRDD
from geonurse.geodataframe import GeoDataFrame


class Test_GeoRDD:

    def test_init(self, spark_context, test_data_geordd):
        rdd = spark_context.parallelize(test_data_geordd)
        geoRdd = GeoRDD(rdd)
        assert geoRdd.count() > 0
        assert geoRdd.count() == rdd.count()
        assert isinstance(geoRdd, GeoRDD)
        assert issubclass(GeoRDD, RDD)

    def test__properties(self, input_polygon_geordd):
        properties = input_polygon_geordd._properties
        assert properties.count() > 0
        assert properties.count() == input_polygon_geordd.count()
        assert isinstance(properties, PipelinedRDD)
        assert isinstance(properties.collect(), list)
        assert all(isinstance(x, dict) for x in properties.collect())

    def test__geometries(self, input_polygon_geordd):
        geometries = input_polygon_geordd._geometries
        # extract first element from the list of geometries
        first_geometry = geometries.take(1)[0]
        assert 'type' in first_geometry
        assert 'coordinates' in first_geometry
        assert geometries.count() > 0
        assert geometries.count() == input_polygon_geordd.count()
        assert isinstance(geometries, PipelinedRDD)
        assert isinstance(geometries.collect(), list)
        assert all(isinstance(x, dict) for x in geometries.collect())

    def test__as_geoJson(self, input_polygon_geordd):
        _geometries = input_polygon_geordd._geometries
        _as_geoJson = input_polygon_geordd._as_geoJson
        assert _geometries.collect() == _as_geoJson.collect()
        assert _geometries.count() == _as_geoJson.count()

    def test__as_shapely(self, input_polygon_geordd):
        shapely = input_polygon_geordd._as_shapely
        assert shapely.count() > 0
        assert shapely.count() == input_polygon_geordd.count()
        assert isinstance(shapely, PipelinedRDD)
        assert isinstance(shapely.collect(), list)
        assert all(isinstance(x, (Polygon, MultiPolygon)) for x in shapely.collect())

    def test__as_wkt(self, input_polygon_geordd):
        wkt = input_polygon_geordd._as_wkt
        assert wkt.count() > 0
        assert wkt.count() == input_polygon_geordd.count()
        assert isinstance(wkt, PipelinedRDD)
        assert isinstance(wkt.collect(), list)
        assert all(isinstance(x, str) for x in wkt.collect())

    def test__as_wkb(self, input_polygon_geordd):
        wkb = input_polygon_geordd._as_wkb
        assert wkb.count() > 0
        assert wkb.count() == input_polygon_geordd.count()
        assert isinstance(wkb, PipelinedRDD)
        assert isinstance(wkb.collect(), list)
        assert all(isinstance(x, bytes) for x in wkb.collect())

    @pytest.mark.parametrize('geometry_type,features_type',[('geoJson', dict),
                                                            ('shapely', (Polygon, MultiPolygon)),
                                                            ('wkt', str),
                                                            ('wkb', bytes)])
    def test_geometries(self, input_polygon_geordd, geometry_type, features_type):
        geometries = input_polygon_geordd.geometries(geometry_type=geometry_type)
        assert all(isinstance(x, features_type) for x in geometries.collect())

    def test_geometries_assertion(self, input_polygon_geordd):
        with pytest.raises(NotImplementedError, match='Unknown return geometry_type:'):
            input_polygon_geordd.geometries(geometry_type='call assertion')

    def test_properties(self, input_polygon_geordd):
        properties = input_polygon_geordd.properties()
        assert properties.count() == input_polygon_geordd.count()

    @pytest.mark.parametrize('method', ['wkt', 'wkb'])
    def test__geometry_df(self, input_polygon_geordd, method):
        df = input_polygon_geordd._geometry_df(method=method)
        schema = df.schema
        assert df.count() > 0
        assert df.count() == input_polygon_geordd.count()
        assert isinstance(schema.fields[0].dataType, StringType)
        assert schema.fields[0].name == 'geometry'
        assert df.columns == ['geometry']
        assert isinstance(df, DataFrame)

    def test__geometry_df_ValueError(self, input_polygon_geordd):
        with pytest.raises(ValueError, match='method is not available'):
            input_polygon_geordd._geometry_df(method='invalid_method')

    def test__property_df(self, input_polygon_geordd):
        df = input_polygon_geordd._property_df()
        assert df.count() > 0
        assert df.count() == input_polygon_geordd.count()
        assert isinstance(df, DataFrame)

    @pytest.mark.parametrize('method', ['wkt', 'wkb'])
    def test_toGeoDF(self, input_polygon_geordd, method):
        geoDf = input_polygon_geordd.toGeoDF(method=method)
        assert geoDf.count() > 0
        assert geoDf.count() == input_polygon_geordd.count()
        assert 'geometry' in geoDf.columns
        assert isinstance(geoDf, GeoDataFrame)
