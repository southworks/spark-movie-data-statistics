package com.southworks.demos.config

import org.apache.spark.sql.types.{DataTypes, StructType}

class SchemaLoader {

  private val movieSchema = new StructType()
    .add("movieId", DataTypes.IntegerType, false)
    .add("title", DataTypes.StringType, false)
    .add("genres", DataTypes.StringType, false)

  private val ratingSchema = new StructType()
    .add("userId", DataTypes.IntegerType, false)
    .add("movieId", DataTypes.IntegerType, false)
    .add("rating", DataTypes.createDecimalType(10, 2), false)
    .add("timestamp", DataTypes.LongType, false)

  def getMovieSchema: StructType = movieSchema

  def getRatingSchema: StructType = ratingSchema
}