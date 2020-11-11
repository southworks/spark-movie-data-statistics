import sys
import os
import time
from pyspark.sql import SparkSession
import Metrics
import SchemaLoader

MOVIES_CSV_FILE = "../dataset/movies.csv"
RATINGS_CSV_FILE = "../dataset/ratings.csv"
SPARK_APP_NAME = "MoviesSparkDemo"

# Application entry point. Responsible for initialization issues, like parameters handling.
# No more behavior should be placed here.
# @param args Application parameters. Accepted: [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]
def main():
    parameters = obtainApplicationArguments(sys.argv)
    sLogLevel = parameters["logLevel"]
    metricsToObtain = parameters["metricsToObtein"]
    run(sLogLevel, metricsToObtain)

# Run and display the specified metrics.
def run(sLogLevel, metrics):
    #Build spark session
    spark = SparkSession \
        .builder \
        .appName(SPARK_APP_NAME) \
        .getOrCreate()

    spark._sc.setLogLevel(sLogLevel)

    #Read initial datasets as non-streaming DataFrames
    moviesDataset = readCsvIntoDataframe(
        spark, MOVIES_CSV_FILE, SchemaLoader.getMovieSchema())
    ratingsDataset = readCsvIntoDataframe(
        spark, RATINGS_CSV_FILE, SchemaLoader.getRatingSchema())

    for metric in metrics:
        dStart = time.time()
        results = runMetric(moviesDataset, ratingsDataset, metric)
        dEnd = time.time()

        printResults(results, dStart, dEnd)

def obtainApplicationArguments(parameters):
    argsMap = {
        "logLevel": "WARN",
        "metricsToObtein": ["1", "2", "3", "4", "5", "6"]
    }

    for sParameter in parameters:
        if (sParameter.upper().find('LOGLEVEL=') != -1):
            argsMap["logLevel"] = sParameter.split("=")[1].upper()

        elif (sParameter.upper().find("METRICS=") != -1):
            argsMap["metricsToObtein"] = sParameter.split("=")[1].split(",")

    return argsMap

# Instantiate every metrics solver class and trigger the evaluations.
#
# @param moviesDataset  Dataset containing the movies information.
# @param ratingsDataset Dataset containing the ratings information.
# @param metrics        The metric ID to evaluate.
def runMetric(moviesDataset, ratingsDataset, metric):
    print("\n")

    if metric == '1':
        print("Best films by overall rating")
        return Metrics.BestFilmsByOverallRating(moviesDataset, ratingsDataset).collect()
    elif metric == '2':
        print("Most rated films")
        return Metrics.MostRatedFilms(moviesDataset, ratingsDataset).collect()
    elif metric == '3':
        print("Global ratings given by each user per category")
        return Metrics.GlobalRatingsGivenByEachUserPerCategory(moviesDataset, ratingsDataset).collect()
    elif metric == '4':
        print("Users giving the lowest ratings")
        return Metrics.UsersGivingTheLowestRatings(moviesDataset, ratingsDataset).collect()
    elif metric == '5':
        print("Genres by average rating")
        return Metrics.GenresByAverageRating(moviesDataset, ratingsDataset).collect()
    elif metric == '6':
        print("Genres combinations")
        return Metrics.GenresCombinations(moviesDataset, ratingsDataset).collect()
    else:
        raise Exception("Tried to obtain an unavailable metric: " + metric)

# Read file into dataframe using an existing schema.
#
# @param s        SparkSession.
# @param filename path to the file.
# @param schema   dataframe schema.
def readCsvIntoDataframe(s, filename, schema):
    csvPath = os.path.join(os.path.dirname(__file__), filename)
    df = s.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(csvPath)

    return df

def printResults(results, dStart, dEnd):
    duration = dEnd - dStart
    print("Results obtained in {:.2f}s\n".format(duration))

    fieldNames = results[0].__fields__
    print("\t - \t".join((str(aFieldName) for aFieldName in fieldNames)))

    for row in results:
        print("\t - \t".join((str(r) for r in row)))


if __name__ == "__main__":
    main()
