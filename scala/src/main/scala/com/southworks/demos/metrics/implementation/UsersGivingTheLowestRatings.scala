package com.southworks.demos.metrics.implementation

import com.southworks.demos.metrics.MetricsSolver
import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.spark.sql.{Dataset, Row}

class UsersGivingTheLowestRatings extends MetricsSolver {
  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // Group by userId and get average ratings for users with at least 250 ratings.
    val usersWithLowestAverageRatings = ratingsDataset
      .groupBy("userId")
      .agg(count("rating"), avg("rating"))
      .filter("count(rating) >= 250").orderBy(col("avg(rating)").asc)
      .withColumnRenamed("avg(rating)", "average rating")
      .withColumnRenamed("count(rating)", "movies rated")
      .limit(10)

    usersWithLowestAverageRatings
  }
}
