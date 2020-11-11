namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;
    using static Microsoft.Spark.Sql.Functions;

    public class UsersGivingTheLowestRatings : IMetricsSolver
    {
        public DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame)
        {
            // Group by userId and get average ratings for users with at least 250 ratings.
            var usersWithLowestAverageRatings = ratingsDataFrame
                .GroupBy("userId")
                .Agg(Count("rating"), Avg("rating"))
                .Filter("count(rating) >= 250")
                .OrderBy(Col("avg(rating)").Asc())
                .WithColumnRenamed("avg(rating)", "average rating")
                .WithColumnRenamed("count(rating)", "movies rated")
                .Limit(10);

            return usersWithLowestAverageRatings;
        }
    }
}