import pyspark.sql.dataframe
from pyspark.sql.functions import col, count, avg, split, explode, first, max

# Finds films with a greater overall rating within those with 5000 ratings or more.


def BestFilmsByOverallRating(moviesDataset, ratingsDataset):

    # First we find which movies have been rated 5000 or more times.
    qualifyingMovies = ratingsDataset \
        .groupBy("movieId") \
        .agg(avg("rating"), count("rating")) \
        .filter("count(rating) >= 5000")

    # Within the filtered movies we query the 10 with a greatest average rating.
    tenGreatestMoviesByAverageRating = moviesDataset \
        .join(qualifyingMovies, moviesDataset.movieId == qualifyingMovies.movieId) \
        .select("avg(rating)", "title") \
        .orderBy(col("avg(rating)").desc()) \
        .limit(10) \
        .withColumnRenamed("avg(rating)", "average score")

    return tenGreatestMoviesByAverageRating


def GenresByAverageRating(moviesDataset, ratingsDataset):
    # Explode categories into a row per movie - category combination.
    # Join movies and ratings datasets.
    # Group by categories and get the average rating.

    overallGenresByAverageRating = moviesDataset \
        .withColumn("genres", split(col("genres"), "\\|")) \
        .select(col("movieId"), explode(col("genres"))) \
        .join(ratingsDataset, moviesDataset.movieId == ratingsDataset.movieId) \
        .groupBy(col("col")) \
        .agg(avg("rating")) \
        .withColumnRenamed("col", "genre") \
        .withColumnRenamed("avg(rating)", "average rating") \
        .orderBy(col("average rating").desc()) \
        .select("average rating", "genre")

    return overallGenresByAverageRating


def GenresCombinations(moviesDataset, ratingsDataset):

    # Get a genres list.
    firstGenresList = moviesDataset \
        .withColumn("genres", split(col("genres"), "\\|")) \
        .select(col("movieId"), explode(col("genres"))) \
        .withColumnRenamed("col", "genre")

    # Get a second list of genres with different column names for same-table join.
    secondGenresList = firstGenresList \
        .withColumnRenamed("genre", "r_genre") \
        .withColumnRenamed("movieId", "r_movieId")

    # Join both genre lists to get every combination. Group by both genres in the combination. Count repetitions and filter the greatest for each one.
    greatestCombinationByGenre = firstGenresList \
        .join(secondGenresList, firstGenresList.genre != secondGenresList.r_genre) \
        .where(firstGenresList.movieId == secondGenresList.r_movieId) \
        .groupBy("genre", "r_genre") \
        .count() \
        .orderBy(col("count").desc()) \
        .groupBy("genre") \
        .agg(first("r_genre").alias("most related genre"), max("count").alias("times related")) \
        .orderBy("genre") \
        .select("times related", "genre", "most related genre")

    return greatestCombinationByGenre


def GlobalRatingsGivenByEachUserPerCategory(moviesDataset, ratingsDataset):
    # Find the users that rated at least 250 movies.
    qualifyingUsers = ratingsDataset \
        .groupBy("userId") \
        .count() \
        .filter("count >= 250") \
        .withColumnRenamed("userId", "r_userId")

    # Find all the movies rated by qualifying users.
    moviesAndQualifyingUsersRating = ratingsDataset \
        .join(qualifyingUsers, ratingsDataset.userId == qualifyingUsers.r_userId) \
        .select("userId", "movieId", "rating") \
        .withColumnRenamed("movieId", "r_movieId")

    # Explode categories into a row per movie - category combination.
    # Group the categories by userId and get each average rating.
    explodedCategoriesAggregatedByAverageRatingPerUser = moviesDataset \
        .join(moviesAndQualifyingUsersRating, moviesDataset.movieId == moviesAndQualifyingUsersRating.r_movieId) \
        .withColumn("genres", split(col("genres"), "\\|")) \
        .select(col("userId"),
                col("movieId"),
                col("rating"),
                explode(col("genres"))) \
        .withColumnRenamed("col", "genre") \
        .groupBy("userId", "genre") \
        .agg(avg("rating")) \
        .orderBy(col("userId").asc()) \
        .select("userId", "avg(rating)", "genre") \
        .withColumnRenamed("avg(rating)", "average score") \
        .limit(20)

    return explodedCategoriesAggregatedByAverageRatingPerUser

# Finds the 10 movies having the greatest amount of ratings.
def MostRatedFilms(moviesDataset, ratingsDataset):

    # First we find the ten movies with the greatest amount of ratings.
    mostRatedTen = ratingsDataset \
        .groupBy("movieId") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(10)

    # Considering only the ten movies we need, we get all the fields to display.
    fullDataMostRatedTen = moviesDataset \
        .join(mostRatedTen, moviesDataset.movieId == mostRatedTen.movieId) \
        .select("count", "title") \
        .orderBy(col("count").desc()) \
        .withColumnRenamed("count", "times rated")

    return fullDataMostRatedTen


def UsersGivingTheLowestRatings(moviesDataset, ratingsDataset):
    # Group by userId and get average ratings for users with at least 250 ratings.
    usersWithLowestAverageRatings = ratingsDataset \
        .groupBy("userId") \
        .agg(count("rating"), avg("rating")) \
        .filter("count(rating) >= 250") \
        .orderBy(col("avg(rating)").asc()) \
        .withColumnRenamed("avg(rating)", "average rating") \
        .withColumnRenamed("count(rating)", "movies rated") \
        .limit(10)

    return usersWithLowestAverageRatings
