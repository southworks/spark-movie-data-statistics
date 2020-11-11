namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class GenresByAverageRating : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // Explode categories into a row per movie - category combination.
            // Join movies and ratings dataframes.
            // Group by categories and get the average rating.

            var overallGenresByAverageRating = moviesDataFrame
                .WithColumn("genres", Split(Col("genres"), "\\|"))
                .Select(Col("movieId"), Explode(Col("genres")))
                .Join(ratingsDataFrame, moviesDataFrame.Col("movieId").EqualTo(ratingsDataFrame.Col("movieId")))
                .GroupBy(Col("col"))
                .Agg(Avg("rating"))
                .WithColumnRenamed("col", "genre")
                .WithColumnRenamed("avg(rating)", "average rating")
                .OrderBy(Col("average rating").Desc())
                .Select("average rating", "genre");

            return overallGenresByAverageRating;
        }
    }
}