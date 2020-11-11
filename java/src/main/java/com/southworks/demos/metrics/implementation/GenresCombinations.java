package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GenresCombinations implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {

        // Get a genres list.
        Dataset<Row> firstGenresList = moviesDataset
                .withColumn("genres", split(col("genres"), "\\|"))
                .select(col("movieId"), explode(col("genres")))
                .withColumnRenamed("col", "genre");

        // Get a second list of genres with different column names for same-table join.
        Dataset<Row> secondGenresList = firstGenresList
                .withColumnRenamed("genre", "r_genre")
                .withColumnRenamed("movieId", "r_movieId");

        // Join both genre lists to get every combination. Group by both genres in the combination. Count repetitions and filter the greatest for each one.
        Dataset<Row> greatestCombinationByGenre = firstGenresList
                .join(secondGenresList, firstGenresList.col("genre").notEqual(secondGenresList.col("r_genre")))
                .where(firstGenresList.col("movieId").equalTo(secondGenresList.col("r_movieId")))
                .groupBy("genre", "r_genre")
                .count()
                .orderBy(col("count").desc())
                .groupBy("genre")
                .agg(first("r_genre").as("most related genre"),
                        max("count").as("times related"))
                .orderBy("genre")
                .select("times related", "genre", "most related genre");

        return greatestCombinationByGenre;
    }
}
