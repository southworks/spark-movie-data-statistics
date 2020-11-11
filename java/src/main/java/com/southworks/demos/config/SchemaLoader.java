package com.southworks.demos.config;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SchemaLoader {

    private static final StructType movieSchema = new StructType()
            .add("movieId", DataTypes.IntegerType, false)
            .add("title", DataTypes.StringType, false)
            .add("genres", DataTypes.StringType, false);

    private static final StructType ratingSchema = new StructType()
            .add("userId", DataTypes.IntegerType, false)
            .add("movieId", DataTypes.IntegerType, false)
            .add("rating", DataTypes.createDecimalType(10, 2), false)
            .add("timestamp", DataTypes.LongType, false);

    public static StructType getMovieSchema() {
        return movieSchema;
    }

    public static StructType getRatingSchema() {
        return ratingSchema;
    }
}
