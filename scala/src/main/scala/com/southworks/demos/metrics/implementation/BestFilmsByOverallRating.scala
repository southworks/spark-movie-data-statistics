package com.southworks.demos.metrics.implementation

import com.southworks.demos.metrics.MetricsSolver
import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.spark.sql.{Dataset, Row}

/**
 * Finds films with a greater overall rating within those with 5000 ratings or more.
 */
class BestFilmsByOverallRating extends MetricsSolver {
  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // First we find which movies have been rated 5000 or more times.
    val qualifyingMovies = ratingsDataset
      .groupBy("movieId")
      .agg(avg("rating"), count("rating"))
      .filter("count(rating) >= 5000")
    // Within the filtered movies we query the 10 with a greatest average rating.
    val tenGreatestMoviesByAverageRating = moviesDataset
      .join(qualifyingMovies, moviesDataset.col("movieId").equalTo(qualifyingMovies.col("movieId")))
      .select("avg(rating)", "title")
      .orderBy(col("avg(rating)").desc)
      .limit(10)
      .withColumnRenamed("avg(rating)", "average score")

    tenGreatestMoviesByAverageRating
  }
}