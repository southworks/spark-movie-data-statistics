package com.southworks.demos.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface MetricsSolver {

    Dataset<Row> run(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset);
}
