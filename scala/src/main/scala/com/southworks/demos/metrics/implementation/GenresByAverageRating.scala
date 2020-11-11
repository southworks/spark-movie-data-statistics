package com.southworks.demos.metrics.implementation

import com.southworks.demos.metrics.MetricsSolver
import org.apache.spark.sql.functions.{avg, col, explode, split}
import org.apache.spark.sql.{Dataset, Row}

class GenresByAverageRating extends MetricsSolver {
  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // Explode categories into a row per movie - category combination.
    // Join movies and ratings datasets.
    // Group by categories and get the average rating.
    val overallGenresByAverageRating = moviesDataset
      .withColumn("genres", split(col("genres"), "\\|"))
      .select(col("movieId"), explode(col("genres")))
      .join(ratingsDataset, moviesDataset.col("movieId").equalTo(ratingsDataset.col("movieId")))
      .groupBy(col("col")).agg(avg("rating"))
      .withColumnRenamed("col", "genre")
      .withColumnRenamed("avg(rating)", "average rating")
      .orderBy(col("average rating").desc)
      .select("average rating", "genre")

    overallGenresByAverageRating
  }
}