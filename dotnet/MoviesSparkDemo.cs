namespace Southworks.Demos.Spark
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.Spark.Sql;
    using Microsoft.Spark.Sql.Types;
    using Southworks.Demos.Spark.Metrics;

    public static class MoviesSparkDemo
    {
        private const string MetricsToObtain = "metricsToObtain";
        private const string LogLevel = "logLevel";
        private const string MoviesCsvFile = "dataset/movies.csv";
        private const string RatingsCsvFile = "dataset/ratings.csv";
        private const string SparkAppName = "MoviesSparkDemo";

        public static void Main(string[] args)
        {
            var parameters = ParseArguments(args);
            var logLevel = (string) parameters[LogLevel];
            var metricsToObtain = (List<string>) parameters[MetricsToObtain];
            Run(logLevel, metricsToObtain);
        }

        private static Dictionary<string, object> ParseArguments(IEnumerable<string> args)
        {
            var argsMap = new Dictionary<string, object>
            {
                // Default parameters: logLevel=WARN, metrics = all.
                {LogLevel, "WARN"}, {MetricsToObtain, new List<string>(new[] {"1", "2", "3", "4", "5", "6"})}
            };

            // This iteration allows us to make parameters optional and position-agnostic.
            foreach (var parameter in args)
            {
                if (parameter.ToUpper().Contains("LOGLEVEL="))
                {
                    argsMap[LogLevel] = parameter.Split('=')[1].ToUpper();
                }

                if (parameter.ToUpper().Contains("METRICS="))
                {
                    argsMap[MetricsToObtain] = new List<string>(parameter.Split('=')[1].Split(','));
                }
            }

            return argsMap;
        }

        private static void Run(string logLevel, List<string> metricsToObtain)
        {
            var spark = SparkSession
                .Builder()
                .AppName(SparkAppName)
                .GetOrCreate();

            spark.SparkContext.SetLogLevel(logLevel);

            // Read initial dataframes as non-streaming DataFrames
            var moviesDataFrame = ReadCsvIntoDataframe(spark, MoviesCsvFile, SchemaLoader.MovieSchema);

            // Read initial dataframes as non-streaming DataFrames
            var ratingsDataFrame = ReadCsvIntoDataframe(spark, RatingsCsvFile, SchemaLoader.RatingSchema);

            foreach (var metric in metricsToObtain)
            {
                var watch = new Stopwatch();
                watch.Start();
                var colRows = RunMetric(moviesDataFrame, ratingsDataFrame, metric);
                watch.Stop();

                PrintRows(colRows.ToList(), watch.Elapsed.TotalSeconds);
            }
        }

        private static IEnumerable<Row> RunMetric(DataFrame moviesDataFrame, DataFrame ratingsDataFrame, string metric)
        {
            switch (metric)
            {
                case "1":
                    Console.WriteLine("\nBest films by overall rating");
                    return new BestFilmsByOverallRating().Run(moviesDataFrame, ratingsDataFrame).ToLocalIterator();
                case "2":
                    Console.WriteLine("\nMost rated films");
                    return new MostRatedFilms().Run(moviesDataFrame, ratingsDataFrame).ToLocalIterator();
                case "3":
                    Console.WriteLine("\nGlobal ratings given by each user per category");
                    return new GlobalRatingsGivenByEachUserPerCategory().Run(moviesDataFrame, ratingsDataFrame)
                        .ToLocalIterator();
                case "4":
                    Console.WriteLine("\nUsers giving the lowest ratings");
                    return new UsersGivingTheLowestRatings().Run(moviesDataFrame, ratingsDataFrame).ToLocalIterator();
                case "5":
                    Console.WriteLine("\nGenres by average rating");
                    return new GenresByAverageRating().Run(moviesDataFrame, ratingsDataFrame).ToLocalIterator();
                case "6":
                    Console.WriteLine("\nGenres combinations");
                    return new GenresCombinations().Run(moviesDataFrame, ratingsDataFrame).ToLocalIterator();
                default:
                    var sExceptionMessage = "Tried to obtain an unavailable metric: " + metric.Trim();
                    Console.WriteLine(sExceptionMessage);
                    throw new Exception(sExceptionMessage);
            }
        }

        private static DataFrame ReadCsvIntoDataframe(SparkSession sparkSession, string filename, StructType schema)
        {
            return sparkSession.Read()
                .Format("csv")
                .Option("header", "true").Schema(schema)
                .Load(filename);
        }

        private static void PrintRows(IReadOnlyList<Row> results, double elapsed)
        {
            var elapsedTime = string.Format("{0:0.00}", elapsed);
            Console.WriteLine("Results obtained in " + elapsedTime + "s\n");

            var aFieldNames = results[0].Schema.Fields;
            var headers = new List<string>();
            foreach (var s in aFieldNames)
            {
                headers.Add(s.Name);
            }

            Console.WriteLine(string.Join("\t - \t", headers));

            foreach (var row in results)
            {
                Console.WriteLine(string.Join("\t - \t", row.Values));
            }
        }
    }
}