package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 * Finds films with a greater overall rating within those with 5000 ratings or more.
 */
public class BestFilmsByOverallRating implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {

        // First we find which movies have been rated 5000 or more times.
        Dataset<Row> qualifyingMovies = ratingsDataset
                .groupBy("movieId")
                .agg(avg("rating"), count("rating"))
                .filter("count(rating) >= 5000");

        // Within the filtered movies we query the 10 with a greatest average rating.
        Dataset<Row> tenGreatestMoviesByAverageRating = moviesDataset
                .join(qualifyingMovies, moviesDataset.col("movieId").equalTo(qualifyingMovies.col("movieId")))
                .select("avg(rating)", "title")
                .orderBy(col("avg(rating)").desc())
                .limit(10)
                .withColumnRenamed("avg(rating)", "average score");

        return tenGreatestMoviesByAverageRating;
    }
}
