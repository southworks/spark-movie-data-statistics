namespace Southworks.Demos.Spark
{
    using Microsoft.Spark.Sql.Types;

    public static class SchemaLoader
    {
        public static StructType MovieSchema => new StructType(new[]
        {
            new StructField("movieId", new IntegerType(), false),
            new StructField("title", new StringType(), false),
            new StructField("genres", new StringType(), false)
        });


        public static StructType RatingSchema => new StructType(new[]
        {
            new StructField("userId", new IntegerType(), false),
            new StructField("movieId", new IntegerType(), false),
            new StructField("rating", new DecimalType(10, 2), false),
            new StructField("timestamp", new LongType(), false)
        });
    }
}