package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GlobalRatingsGivenByEachUserPerCategory implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {

        // Find the users that rated at least 250 movies.
        Dataset<Row> qualifyingUsers = ratingsDataset
                .groupBy("userId")
                .count()
                .filter("count >= 250")
                .withColumnRenamed("userId", "r_userId");

        // Find all the movies rated by qualifying users.
        Dataset<Row> moviesAndQualifyingUsersRating = ratingsDataset
                .join(qualifyingUsers, ratingsDataset.col("userId").equalTo(qualifyingUsers.col("r_userId")))
                .select("userId", "movieId", "rating")
                .withColumnRenamed("movieId", "r_movieId");

        // Explode categories into a row per movie - category combination.
        // Group the categories by userId and get each average rating.
        Dataset<Row> explodedCategoriesAggregatedByAverageRatingPerUser = moviesDataset
                .join(moviesAndQualifyingUsersRating, moviesDataset.col("movieId").equalTo(moviesAndQualifyingUsersRating.col("r_movieId")))
                .withColumn("genres", split(col("genres"), "\\|"))
                .select(col("userId"),
                        col("movieId"),
                        col("rating"),
                        explode(col("genres")))
                .withColumnRenamed("col", "genre")
                .groupBy("userId", "genre")
                .agg(avg("rating"))
                .orderBy(col("userId").asc())
                .select("userId", "avg(rating)", "genre")
                .withColumnRenamed("avg(rating)", "average score")
                .limit(20);

        return explodedCategoriesAggregatedByAverageRatingPerUser;
    }
}
