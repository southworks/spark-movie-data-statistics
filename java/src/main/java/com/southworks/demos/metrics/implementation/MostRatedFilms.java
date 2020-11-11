package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * Finds the 10 movies having the greatest amount of ratings.
 */
public class MostRatedFilms implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {

        // First we find the ten movies with the greatest amount of ratings.
        Dataset<Row> mostRatedTen = ratingsDataset
                .groupBy("movieId")
                .count()
                .orderBy(col("count").desc())
                .limit(10);

        // Considering only the ten movies we need, we get all the fields to display.
        Dataset<Row> fullDataMostRatedTen = moviesDataset
                .join(mostRatedTen, moviesDataset.col("movieId").equalTo(mostRatedTen.col("movieId")))
                .select("count", "title")
                .orderBy(col("count").desc())
                .withColumnRenamed("count", "times rated");

        return fullDataMostRatedTen;
    }
}
