namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class MostRatedFilms : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // First we find the ten movies with the greatest amount of ratings.
            var mostRatedTen = ratingsDataFrame
                .GroupBy("movieId")
                .Count()
                .OrderBy(Col("count").Desc())
                .Limit(10);

            // Considering only the ten movies we need, we get all the fields to display.
            var fullDataMostRatedTen = moviesDataFrame
                .Join(mostRatedTen, moviesDataFrame.Col("movieId").EqualTo(mostRatedTen.Col("movieId")))
                .Select("count", "title")
                .OrderBy(Col("count").Desc())
                .WithColumnRenamed("count", "times rated");

            return fullDataMostRatedTen;
        }
    }
}