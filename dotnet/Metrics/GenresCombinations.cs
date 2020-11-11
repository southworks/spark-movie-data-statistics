namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class GenresCombinations : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // Get a genres list.
            var firstGenresList = moviesDataFrame
                .WithColumn("genres", Split(Col("genres"), "\\|"))
                .Select(Col("movieId"), Explode(Col("genres")))
                .WithColumnRenamed("col", "genre");

            // Get a second list of genres with different column names for same-table join.
            var secondGenresList = firstGenresList
                .WithColumnRenamed("genre", "r_genre")
                .WithColumnRenamed("movieId", "r_movieId");

            // Join both genre lists to get every combination. Group by both genres in the combination. Count repetitions and filter the greatest for each one.
            var greatestCombinationByGenre = firstGenresList
                .Join(secondGenresList, firstGenresList.Col("genre").NotEqual(secondGenresList.Col("r_genre")))
                .Where(firstGenresList.Col("movieId").EqualTo(secondGenresList.Col("r_movieId")))
                .GroupBy("genre", "r_genre")
                .Count()
                .OrderBy(Col("count").Desc())
                .GroupBy("genre")
                .Agg(First("r_genre").As("most related genre"), Max("count").As("times related"))
                .OrderBy("genre")
                .Select("times related", "genre", "most related genre");

            return greatestCombinationByGenre;
        }
    }
}