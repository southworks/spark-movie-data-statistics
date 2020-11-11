package com.southworks.demos

import java.text.DecimalFormat
import java.util
import java.util.function.Consumer
import java.util.{List, Map}

import com.southworks.demos.config.SchemaLoader
import com.southworks.demos.metrics.implementation._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object MoviesSparkDemo {

  private val LOG_LEVEL = "logLevel"
  private val METRICS_TO_OBTAIN = "metricsToObtain"
  private val MOVIES_CSV_FILE = "/dataset/movies.csv"
  private val RATINGS_CSV_FILE = "/dataset/ratings.csv"
  private val SPARK_APP_NAME = "MoviesSparkDemo"

  /**
   * Application entry point. Responsible for initialization issues, like parameters handling.
   * No more behavior should be placed here.
   *
   * @param args Application parameters. Accepted: [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]
   */
  def main(args: Array[String]): Unit = {
    val parameters: Map[String, AnyRef] = parseArguments(args)
    val sLogLevel: String = parameters.get(LOG_LEVEL).asInstanceOf[String]
    val metricsToObtain: List[String] = parameters.get(METRICS_TO_OBTAIN).asInstanceOf[List[String]]
    run(sLogLevel, metricsToObtain)
  }

  private def parseArguments(parameters: Array[String]) = {
    val argsMap = new util.HashMap[String, AnyRef]
    // If no log level parameter is received, take WARN as default.
    argsMap.put(LOG_LEVEL, "WARN")
    // If no metrics parameter is received, process every metric.
    argsMap.put(METRICS_TO_OBTAIN, util.Arrays.asList("1", "2", "3", "4", "5", "6"))
    // This iteration allows us to make parameters optional and position-agnostic.
    for (sParameter <- parameters) {
      if (sParameter.toUpperCase.contains("LOGLEVEL=")) argsMap.put(LOG_LEVEL, sParameter.split("=")(1).toUpperCase)
      if (sParameter.toUpperCase.contains("METRICS=")) argsMap.put(METRICS_TO_OBTAIN, util.Arrays.asList(sParameter.split("=")(1).split(",")))
    }
    argsMap
  }

  /**
   * Run and display the specified metrics.
   */
  def run(sLogLevel: String, metrics: util.List[String]): Unit = {
    // Build spark session
    val spark = SparkSession
      .builder
      .appName(SPARK_APP_NAME)
      .getOrCreate

    spark.sparkContext.setLogLevel(sLogLevel)

    // Read initial datasets as non-streaming DataFrames
    val schemaLoader = new SchemaLoader()
    val moviesDataset = readCsvIntoDataframe(spark, MOVIES_CSV_FILE, schemaLoader.getMovieSchema)
    val ratingsDataset = readCsvIntoDataframe(spark, RATINGS_CSV_FILE, schemaLoader.getRatingSchema)

    metrics.forEach(toJavaConsumer((mId: String) => {
      val startTime = System.nanoTime
      val colRows = runMetric(moviesDataset, ratingsDataset, mId)
      val endTime = System.nanoTime
      printRows(colRows, startTime, endTime)
    }))
  }

  /**
   * Instantiate specified metrics solver class and trigger the evaluation.
   *
   * @param moviesDataset  Dataset containing the movies information.
   * @param ratingsDataset Dataset containing the ratings information.
   * @param sMetric        The metric ID to evaluate.
   * @return Dataset collected into a List.
   */
  private def runMetric(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row], sMetric: String) = {
    val metricResult = null
    sMetric.trim match {
      case "1" =>
        System.out.println("\nBest films by overall rating")
        new BestFilmsByOverallRating().run(moviesDataset, ratingsDataset).collectAsList
      case "2" =>
        System.out.println("\nMost rated films")
        new MostRatedFilms().run(moviesDataset, ratingsDataset).collectAsList
      case "3" =>
        System.out.println("\nGlobal ratings given by each user per category")
        new GlobalRatingsGivenByEachUserPerCategory().run(moviesDataset, ratingsDataset).collectAsList
      case "4" =>
        System.out.println("\nUsers giving the lowest ratings")
        new UsersGivingTheLowestRatings().run(moviesDataset, ratingsDataset).collectAsList
      case "5" =>
        System.out.println("\nGenres by average rating")
        new GenresByAverageRating().run(moviesDataset, ratingsDataset).collectAsList
      case "6" =>
        System.out.println("\nGenres combinations")
        new GenresCombinations().run(moviesDataset, ratingsDataset).collectAsList
      case _ =>
        val sExceptionMessage = "Tried to obtain an unavailable metric: " + sMetric.trim
        System.out.println(sExceptionMessage)
        throw new RuntimeException(sExceptionMessage)
    }
  }

  /**
   * Read file into dataframe using an existing schema.
   *
   * @param s        SparkSession.
   * @param filename path to the file.
   * @param schema   dataframe schema.
   */
  private def readCsvIntoDataframe(s: SparkSession, filename: String, schema: StructType) = {
    val fullPath = System.getProperty("user.dir").concat(filename)
    s.read
      .format("csv")
      .option("header", "true")
      .schema(schema).load(fullPath)
  }

  private def printRows(results: util.List[Row], startTime: Long, endTime: Long): Unit = {
    val duration = ((endTime - startTime) / 1000000000.toDouble).toDouble
    println("Results obtained in " + new DecimalFormat("#.##").format(duration) + "s\n")
    val fieldNames = results.get(0).schema.fieldNames
    println(fieldNames.mkString("\t - \t"))
    results.forEach(toJavaConsumer((row: Row) => println(row.mkString("\t - \t"))))
  }

  def toJavaConsumer[T](consumer: (T) => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(t: T): Unit = {
        consumer(t)
      }
    }
  }
}
