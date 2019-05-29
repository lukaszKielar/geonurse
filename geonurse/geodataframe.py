import geopandas
import pyspark.sql
import shapely.wkt
import geonurse.geordd


class GeoDataFrame(pyspark.sql.DataFrame):

    def __init__(self, df: pyspark.sql.DataFrame) -> None:
        super().__init__(df._jdf, df.sql_ctx)

    @property
    def geoRdd(self):
        return geonurse.geordd.GeoRDD(self.rdd)

    def toGeoPandas(self):
        df = self.toPandas()
        df.geometry = df.geometry.apply(lambda x: shapely.wkt.loads(x))
        return geopandas.GeoDataFrame(df)
