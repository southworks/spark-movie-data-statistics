# Spark demo using a movies and reviews dataset

## Running solution locally

In order to run the solution locally, you first need to make sure to meet the following requirements:

- [**Java 8**](https://openjdk.java.net/install/)
- [**Maven 3.x**](https://maven.apache.org/download.cgi)
- [**SBT**](https://www.scala-sbt.org/1.x/docs/Setup.html) (only for Scala sample)
- [**.NET Core Framework 3.x**](https://dotnet.microsoft.com/download/dotnet-core) (only for .NET sample)
- **Spark 3.0.x** (with Hadoop 3.2 libraries) ([link](https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz))

    > Installing Spark consists of only extracting the tarball, setting `SPARK_HOME` environment variable and finally adding `$SPARK_HOME/bin` to `PATH` variable (`%SPARK_HOME%\bin` on Windows).

    > **Note**: On Windows platform it requires to also perform the following steps:
    >
    > - Download `winutils` and `hadoop.dll` for Hadoop 3.2.x from [this link](https://github.com/cdarlint/winutils)
    > - Set `HADOOP_HOME` environment variable pointing to the directory where these 2 files have been downloaded and add `%HADOOP_HOME%\bin` to `PATH` variable.
    > - Copy `hadoop.dll` to `System32` directory
    >

## Pre-requisites

Run `prerequisites.sh` to download the MovieLens Dataset[^1] and set everything ready to submit the job to Spark.

The dataset consists of two csv files:

- `movies.csv`: 62K rows containing movie identifier, title and genres
- `ratings.csv`: 25M rows containing user identifier, rated film, score given and timestamp

## Running the jobs

 From the root folder execute:
 
### Java

**Compile**

`mvn package -f java/pom.xml` 

**Run**

`spark-submit --master local[*] --class com.southworks.demos.MoviesSparkDemo ./java/target/MoviesSparkDemo-0.0.1-SNAPSHOT.jar [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]`

### Python

`spark-submit --master local[*] ./python/MoviesSparkDemo.py [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]`

### Scala

**Compile**

`cd scala`

`sbt package`

`cd ..`

**Run**

`spark-submit --master local[*] --class com.southworks.demos.MoviesSparkDemo ./scala/target/scala-2.12/scala_2.12-0.1.jar [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]`

### .NET

**Compile**

`dotnet build ./dotnet`

**Run**

`spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local[*] ./dotnet/bin/Debug/netcoreapp3.1/microsoft-spark-3-0_2.12-1.0.0.jar ./dotnet/bin/Debug/netcoreapp3.1/Southworks.Demos.Spark.exe [logLevel=< INFO|ERROR|WARN|FINE >] [metrics=< 1|2|3|4|5|6 >[,1|2|3|4|5|6]]`


To make easier individual metrics execution, we added the metrics program argument. It can receive a comma-separated list of metric numbers. If no metrics argument given, every metric will be obtained.

### Metrics
 
- 1: Best films by overall rating
- 2: Most rated films
- 3: Global ratings given by each user per category
- 4: Users giving the lowest ratings
- 5: Overall most loved and hated genres
- 6: Genres combinations

## **References**

[^1]: F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 19:1–19:19. <https://doi.org/10.1145/2827872>
 
