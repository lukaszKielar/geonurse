import shapely.geometry
import shapely.wkb
import pyspark.rdd
import pyspark.sql.functions as F
from pyspark.sql import Row
import geonurse.geodataframe


class GeoRDD(pyspark.rdd.RDD):
    
    def __init__(self, rdd: pyspark.rdd.RDD) -> None:
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

    @property
    def _as_wkb(self):
        return self._as_shapely.map(lambda x: shapely.wkb.dumps(x))

    def geometries(self, geometry_type: str = 'geoJson'):
        if geometry_type == 'geoJson':
            return self._as_geoJson
        elif geometry_type == 'shapely':
            return self._as_shapely
        elif geometry_type == 'wkt':
            return self._as_wkt
        elif geometry_type == 'wkb':
            return self._as_wkb
        raise NotImplementedError("Unknown return geometry_type: %s.", geometry_type)

    def properties(self):
        def _replace_none_dict_values(in_dict):
            return {k: (v if v is not None else 'No Data') for k, v in in_dict.items()}
        # replace any None values in the output dict
        return self._properties.map(lambda x: _replace_none_dict_values(x))

    def _geometry_df(self, method='wkt'):
        if method == 'wkt':
            return (self.geometries('wkt')
                        .map(Row)
                        .toDF(['geometry']))
        elif method == 'wkb':
            return (self.geometries('wkb')
                        .map(lambda x: bytearray(x))
                        .map(Row)
                        .toDF(['geometry']))
        raise ValueError("{} method is not available".format(method))

    def _property_df(self):
        return self.properties().toDF()

    def toGeoDF(self, method='wkt') -> geonurse.geodataframe.GeoDataFrame:
        # TODO replace F.monotonically_increasing_id() with zipWithIndex()
        geometries_df = self._geometry_df(method=method).withColumn('id', F.monotonically_increasing_id())
        properties_df = self._property_df().withColumn('id', F.monotonically_increasing_id())
        df = (properties_df
                  .join(geometries_df, properties_df.id == geometries_df.id, 'inner')
                  .drop(geometries_df.id))
        return geonurse.geodataframe.GeoDataFrame(df)
