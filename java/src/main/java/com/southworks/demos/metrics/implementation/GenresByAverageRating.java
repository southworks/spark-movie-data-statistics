package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GenresByAverageRating implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {
        // Explode categories into a row per movie - category combination.
        // Join movies and ratings datasets.
        // Group by categories and get the average rating.

        Dataset<Row> overallGenresByAverageRating = moviesDataset
                .withColumn("genres", split(col("genres"), "\\|"))
                .select(col("movieId"), explode(col("genres")))
                .join(ratingsDataset, moviesDataset.col("movieId").equalTo(ratingsDataset.col("movieId")))
                .groupBy(col("col"))
                .agg(avg("rating"))
                .withColumnRenamed("col", "genre")
                .withColumnRenamed("avg(rating)", "average rating")
                .orderBy(col("average rating").desc())
                .select("average rating", "genre");

        return overallGenresByAverageRating;
    }
}
