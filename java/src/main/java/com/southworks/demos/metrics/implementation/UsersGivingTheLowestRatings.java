package com.southworks.demos.metrics.implementation;

import com.southworks.demos.metrics.MetricsSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class UsersGivingTheLowestRatings implements MetricsSolver {

    @Override
    public Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset) {

        // Group by userId and get average ratings for users with at least 250 ratings.
        Dataset<Row> usersWithLowestAverageRatings = ratingsDataset
                .groupBy("userId")
                .agg(count("rating"), avg("rating"))
                .filter("count(rating) >= 250")
                .orderBy(col("avg(rating)").asc())
                .withColumnRenamed("avg(rating)", "average rating")
                .withColumnRenamed("count(rating)", "movies rated")
                .limit(10);

        return usersWithLowestAverageRatings;
    }
}
