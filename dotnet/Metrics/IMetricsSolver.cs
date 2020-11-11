namespace Southworks.Demos.Spark.Metrics
{
    using Microsoft.Spark.Sql;

    public interface IMetricsSolver
    {
        DataFrame Run(DataFrame moviesDataFrame, DataFrame ratingsDataFrame);
    }
}