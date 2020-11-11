from pyspark.sql.types import *

def getMovieSchema():
    movieSchema = StructType([StructField("movieId",IntegerType(),False),
                            StructField("title",StringType(),False),
                            StructField("genres",StringType(),False)])
    return movieSchema

def getRatingSchema():
    ratingSchema = StructType([StructField("userId",IntegerType(),False),
                            StructField("movieId",IntegerType(),False),
                            StructField("rating",DecimalType(10,2),False),
                            StructField("timestamp",LongType(),False)])
    return ratingSchema
