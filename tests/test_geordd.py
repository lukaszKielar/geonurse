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
        assert isinstance(geoRdd, GeoRDD)
        assert issubclass(GeoRDD, RDD)

    def test__properties(self, input_polygon_geordd):
        properties = input_polygon_geordd._properties

        assert properties.count() > 0
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
        assert isinstance(shapely, PipelinedRDD)
        assert isinstance(shapely.collect(), list)
        assert all(isinstance(x, (Polygon, MultiPolygon)) for x in shapely.collect())

    def test__as_wkt(self, input_polygon_geordd):
        wkt = input_polygon_geordd._as_wkt

        assert wkt.count() > 0
        assert isinstance(wkt, PipelinedRDD)
        assert isinstance(wkt.collect(), list)
        assert all(isinstance(x, str) for x in wkt.collect())

    @pytest.mark.parametrize('geometry_type,features_type',[('geoJson', dict),
                                                            ('shapely', (Polygon, MultiPolygon)),
                                                            ('wkt', str)])
    def test_geometries(self, input_polygon_geordd, geometry_type, features_type):
        geometries = input_polygon_geordd.geometries(geometry_type=geometry_type)

        assert all(isinstance(x, features_type) for x in geometries.collect())

    def test_geometries_assertion(self, input_polygon_geordd):
        with pytest.raises(NotImplementedError, match='Unknown return geometry_type:'):
            input_polygon_geordd.geometries(geometry_type='call assertion')

    # TODO fix what is the best way to test this method
    def test_properties(self):
        pass

    def test__geometry_df(self, input_polygon_geordd):
        df = input_polygon_geordd._geometry_df
        schema = df.schema

        assert df.count() > 0
        assert df.count() == input_polygon_geordd.count()
        assert isinstance(schema.fields[0].dataType, StringType)
        assert schema.fields[0].name == 'geometry'
        assert isinstance(df, DataFrame)

    def test__property_df(self, input_polygon_geordd):
        df = input_polygon_geordd._property_df

        assert df.count() > 0
        assert df.count() == input_polygon_geordd.count()
        assert isinstance(df, DataFrame)

    def test_toGeoDF(self, input_polygon_geordd):
        geoDf = input_polygon_geordd.toGeoDF()
        schema = geoDf.schema

        assert geoDf.count() > 0
        assert geoDf.count() == input_polygon_geordd.count()
        assert 'geometry' in schema.names
        assert isinstance(geoDf, GeoDataFrame)

