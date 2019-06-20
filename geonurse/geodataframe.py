import geopandas
import pyspark.sql
import shapely.wkt
import shapely.wkb
import geonurse.geordd


class GeoDataFrame(pyspark.sql.DataFrame):

    def __init__(self, df: pyspark.sql.DataFrame) -> None:
        super().__init__(df._jdf, df.sql_ctx)

    @property
    def geoRdd(self):
        return geonurse.geordd.GeoRDD(self.rdd)

    def toGeoPandas(self, method='wkt'):
        df = self.toPandas()
        if method == 'wkt':
            df.geometry = df.geometry.apply(lambda x: shapely.wkt.loads(x))
            return geopandas.GeoDataFrame(df)
        elif method == 'wkb':
            df.geometry = df.geometry.apply(lambda x: bytes(x)).apply(lambda x: shapely.wkb.loads(x))
            return geopandas.GeoDataFrame(df)
        raise ValueError("{} method is not available".format(method))
