package com.southworks.demos;

import com.southworks.demos.config.SchemaLoader;
import com.southworks.demos.metrics.implementation.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.time.Duration;
import java.text.DecimalFormat;
import java.util.*;

public class MoviesSparkDemo {
    private static final String LOG_LEVEL = "logLevel";
    private static final String METRICS_TO_OBTAIN = "metricsToObtain";
    private static final String MOVIES_CSV_FILE = "/dataset/movies.csv";
    private static final String RATINGS_CSV_FILE = "/dataset/ratings.csv";
    private static final String SPARK_APP_NAME = "MoviesSparkDemo";

    /**
     * Application entry point. Responsible for initialization issues, like parameters handling.
     * No more behavior should be placed here.
     *
     * @param args Application parameters. Accepted: [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]
     */
    public static void main(String... args) {
        Map<String, Object> parameters = parseArguments(args);
        String sLogLevel = (String) parameters.get(LOG_LEVEL);
        List<String> metricsToObtain = (List<String>) parameters.get(METRICS_TO_OBTAIN);
        new MoviesSparkDemo().run(sLogLevel, metricsToObtain);
    }

    private static Map<String, Object> parseArguments(String... parameters) {
        Map<String, Object> argsMap = new HashMap<>();
        // If no log level parameter is received, take WARN as default.
        argsMap.put(LOG_LEVEL, "WARN");

        // If no metrics parameter is received, process every metric.
        argsMap.put(METRICS_TO_OBTAIN, Arrays.asList("1", "2", "3", "4", "5", "6"));

        // This iteration allows us to make parameters optional and position-agnostic.
        for (String sParameter : parameters) {
            if (sParameter.toUpperCase().contains("LOGLEVEL=")) {
                argsMap.put(LOG_LEVEL, sParameter.split("=")[1].toUpperCase());
            }

            if (sParameter.toUpperCase().contains("METRICS=")) {
                argsMap.put(METRICS_TO_OBTAIN, Arrays.asList(sParameter.split("=")[1].split(",")));
            }
        }

        return argsMap;
    }

    /**
     * Run and display the specified metrics.
     */
    public void run(String sLogLevel, List<String> metrics) {
        // Build spark session
        SparkSession spark = SparkSession
                .builder()
                .appName(SPARK_APP_NAME)
                .getOrCreate();

        spark.sparkContext().setLogLevel(sLogLevel);

        // Read initial datasets as non-streaming DataFrames
        Dataset<Row> moviesDataset = readCsvIntoDataframe(
                spark, MOVIES_CSV_FILE, SchemaLoader.getMovieSchema()
        );

        Dataset<Row> ratingsDataset = readCsvIntoDataframe(
                spark, RATINGS_CSV_FILE, SchemaLoader.getRatingSchema()
        );

        metrics.forEach((mId) -> {
            Long startTime = System.nanoTime();
            List<Row> colRows = runMetric(moviesDataset, ratingsDataset, mId);
            Long endTime = System.nanoTime();

            printRows(colRows, startTime, endTime);
        });
    }

    /**
     * Instantiate specified metrics solver class and trigger the evaluation.
     *
     * @param moviesDataset  Dataset containing the movies information.
     * @param ratingsDataset Dataset containing the ratings information.
     * @param sMetric        The metric ID to evaluate.
     * @return Dataset collected into a List.
     */
    private List<Row> runMetric(Dataset<Row> moviesDataset, Dataset<Row> ratingsDataset, String sMetric) {
        Dataset<Row> metricResult;

        switch (sMetric.trim()) {
            case "1":
                System.out.println("\nBest films by overall rating");
                return new BestFilmsByOverallRating().run(moviesDataset, ratingsDataset).collectAsList();
            case "2":
                System.out.println("\nMost rated films");
                return new MostRatedFilms().run(moviesDataset, ratingsDataset).collectAsList();
            case "3":
                System.out.println("\nGlobal ratings given by each user per category");
                return new GlobalRatingsGivenByEachUserPerCategory().run(moviesDataset, ratingsDataset).collectAsList();
            case "4":
                System.out.println("\nUsers giving the lowest ratings");
                return new UsersGivingTheLowestRatings().run(moviesDataset, ratingsDataset).collectAsList();
            case "5":
                System.out.println("\nGenres by average rating");
                return new GenresByAverageRating().run(moviesDataset, ratingsDataset).collectAsList();
            case "6":
                System.out.println("\nGenres combinations");
                return new GenresCombinations().run(moviesDataset, ratingsDataset).collectAsList();
            default:
                String sExceptionMessage = "Tried to obtain an unavailable metric: " + sMetric.trim();
                System.out.println(sExceptionMessage);
                throw new RuntimeException(sExceptionMessage);
        }
    }

    /**
     * Read file into dataframe using an existing schema.
     *
     * @param s        SparkSession.
     * @param filename path to the file.
     * @param schema   dataframe schema.
     */
    private static Dataset<Row> readCsvIntoDataframe(SparkSession s, String filename, StructType schema) {
        String fullPath = System.getProperty("user.dir").concat(filename);
        return s.read()
                .format("csv")
                .option("header", "true").schema(schema)
                .load(fullPath);
    }

    private void printRows(List<Row> results, long startTime, long endTime) {
        double duration = (double) ((endTime - startTime) / (double)1_000_000_000);
        System.out.println("Results obtained in " +  new DecimalFormat("#.##").format(duration) + "s\n");

        String[] aFieldNames = results.get(0).schema().fieldNames();
        System.out.println(String.join("\t - \t", aFieldNames));

        results.forEach(row -> System.out.println(row.mkString("\t - \t")));
    }
}