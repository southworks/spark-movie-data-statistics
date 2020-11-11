namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class GlobalRatingsGivenByEachUserPerCategory : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // Find the users that rated at least 250 movies.
            var qualifyingUsers = ratingsDataFrame
                .GroupBy("userId")
                .Count()
                .Filter("count >= 250")
                .WithColumnRenamed("userId", "r_userId");

            // Find all the movies rated by qualifying users.
            var moviesAndQualifyingUsersRating = ratingsDataFrame
                .Join(qualifyingUsers, ratingsDataFrame.Col("userId").EqualTo(qualifyingUsers.Col("r_userId")))
                .Select("userId", "movieId", "rating")
                .WithColumnRenamed("movieId", "r_movieId");

            // Explode categories into a row per movie - category combination.
            // Group the categories by userId and get each average rating.
            var explodedCategoriesAggregatedByAverageRatingPerUser = moviesDataFrame
                .Join(moviesAndQualifyingUsersRating,
                    moviesDataFrame.Col("movieId").EqualTo(moviesAndQualifyingUsersRating.Col("r_movieId")))
                .WithColumn("genres", Split(Col("genres"), "\\|"))
                .Select(Col("userId"),
                    Col("movieId"),
                    Col("rating"),
                    Explode(Col("genres")))
                .WithColumnRenamed("col", "genre")
                .GroupBy("userId", "genre")
                .Agg(Avg("rating"))
                .OrderBy(Col("userId").Asc())
                .Select("userId", "avg(rating)", "genre")
                .WithColumnRenamed("avg(rating)", "average score")
                .Limit(20);

            return explodedCategoriesAggregatedByAverageRatingPerUser;
        }
    }
}