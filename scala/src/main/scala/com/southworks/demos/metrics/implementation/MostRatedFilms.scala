package com.southworks.demos.metrics.implementation

import com.southworks.demos.metrics.MetricsSolver
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}

/**
 * Finds the 10 movies having the greatest amount of ratings.
 */
class MostRatedFilms extends MetricsSolver {
  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // First we find the ten movies with the greatest amount of ratings.
    val mostRatedTen = ratingsDataset
      .groupBy("movieId")
      .count
      .orderBy(col("count").desc)
      .limit(10)

    // Considering only the ten movies we need, we get all the fields to display.
    val fullDataMostRatedTen = moviesDataset
      .join(mostRatedTen, moviesDataset.col("movieId").equalTo(mostRatedTen.col("movieId")))
      .select("count", "title")
      .orderBy(col("count").desc)
      .withColumnRenamed("count", "times rated")

    fullDataMostRatedTen
  }
}
