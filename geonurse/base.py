import shapely.geometry

from pyspark.rdd import RDD
from pyspark.sql.types import StringType
import pyspark.sql.functions as F


class GeoRDD(RDD):
    
    def __init__(self, rdd: RDD) -> None:
        super().__init__(jrdd=rdd._jrdd, ctx=rdd.ctx, jrdd_deserializer=rdd._jrdd_deserializer)

    @property
    def _properties(self):
        # get properties and unpack fiona OrderedDict
        return self.map(lambda x: x.get('properties')).map(lambda x: dict(x))

    @property
    def _geometries(self):
        return self.map(lambda x: x.get('geometry'))
    
    @property
    def _as_geoJson(self):
        return self._geometries

    @property
    def _as_shapely(self):
        return self._as_geoJson.map(lambda x: shapely.geometry.shape(x))

    @property
    def _as_wkt(self):
        return self._as_shapely.map(lambda x: x.wkt)

    def geometries(self, geometry_type: str = 'geoJson'):
        if geometry_type == 'geoJson':
            return self._as_geoJson
        elif geometry_type == 'shapely':
            return self._as_shapely
        elif geometry_type == 'wkt':
            return self._as_wkt
        raise NotImplementedError("Unknown return geometry_type: %s.", geometry_type)

    def properties(self):

        def _replace_none_dict_values(in_dict):
            return {k:(v if v is not None else 'No Data') for k, v in in_dict.items()}
        # replace any None values in the output dict
        return self._properties.map(lambda x: _replace_none_dict_values(x))

    @property
    def _geometry_df(self):
        return self.geometries('wkt').toDF(StringType()).selectExpr('value as geometry')

    @property
    def _property_df(self):
        return self.properties().toDF()

    def toGeoDF(self):
        geometries_df = self._geometry_df.withColumn('id', F.monotonically_increasing_id())
        properties_df = self._property_df.withColumn('id', F.monotonically_increasing_id())

        return (
            properties_df.join(geometries_df, properties_df.id == geometries_df.id, 'inner')
                .drop(geometries_df.id)
                .sort(F.col('id').asc())  # pylint: disable=no-member
        )
