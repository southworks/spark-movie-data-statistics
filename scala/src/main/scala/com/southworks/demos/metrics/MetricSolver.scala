package com.southworks.demos.metrics

import org.apache.spark.sql.{Dataset, Row}

trait MetricsSolver {
  def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row]
}