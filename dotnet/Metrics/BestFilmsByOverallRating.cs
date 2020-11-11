namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class BestFilmsByOverallRating : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // First we find which movies have been rated 5000 or more times.
            var qualifyingMovies = ratingsDataFrame
                .GroupBy("movieId")
                .Agg(Avg("rating"), Count("rating"))
                .Filter("count(rating) >= 5000");

            // Within the filtered movies we query the 10 with a greatest average rating.
            var tenGreatestMoviesByAverageRating = moviesDataFrame
                .Join(qualifyingMovies, moviesDataFrame.Col("movieId").EqualTo(qualifyingMovies.Col("movieId")))
                .Select("avg(rating)", "title")
                .OrderBy(Col("avg(rating)").Desc())
                .Limit(10)
                .WithColumnRenamed("avg(rating)", "average score");

            return tenGreatestMoviesByAverageRating;
        }
    }
}