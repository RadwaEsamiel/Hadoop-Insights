from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row

def parse_ratings(line):
    # Split by the delimiter used in your ratings data, for example, '|'
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), rating_time=int(fields[3]))

def parse_movies(line):
    # Split by the delimiter used in your movies data, for example, '|'
    fields = line.split('|')
    return Row(movie_id=int(fields[0]), movie_title=fields[1], release_date=fields[2], release_video=fields[3], imdb_link=fields[4])

if __name__ == "__main__":
    # Initialize Spark session with Cassandra connection details
    spark = SparkSession.builder \
        .appName("Cassandra Integration with Movie Ratings") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    # Read ratings data from HDFS and parse it
    ratings_data = spark.sparkContext.textFile("hdfs:///user/root/data/movies_data")
    ratings_rdd = ratings_data.map(parse_ratings)
    ratings_df = spark.createDataFrame(ratings_rdd)

    # Write ratings data into Cassandra table `movies_ratings_info`
    ratings_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movies_ratings_info", keyspace="movielens") \
        .save()

    # Read movies data from HDFS and parse it
    movies_data = spark.sparkContext.textFile("hdfs:///user/root/data/movies_items")
    movies_rdd = movies_data.map(parse_movies)
    movies_df = spark.createDataFrame(movies_rdd)

    # Write movies data into Cassandra table `movies_names`
    movies_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movies_names", keyspace="movielens") \
        .save()

    # Query the data from the Cassandra tables
    ratings_from_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movies_ratings_info", keyspace="movielens") \
        .load()

    movies_from_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movies_names", keyspace="movielens") \
        .load()

    # Perform the aggregation on the ratings data
    ratings_movies_grouped = ratings_from_cassandra.groupby("movie_id").agg(
        F.count("rating").alias("rating_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating")
    )
    
    popularMoviesDF = ratings_movies_grouped.filter(ratings_movies_grouped.rating_count > 10)

    # Join with movie metadata to get movie titles
    joinedDF = popularMoviesDF.join(movies_from_cassandra, "movie_id") \
        .select("movie_id", "movie_title", "avg_rating", "rating_count")

    # Create temporary views for SQL queries
    joinedDF.createOrReplaceTempView("TOP_movies")

    # Query for top 10 movies
    topMoviesDF = spark.sql("SELECT * FROM TOP_movies ORDER BY avg_rating DESC LIMIT 10")
    print("Top 10 Movies:")
    topMoviesDF.show()

    # Query for bottom 10 movies
    worstMoviesDF = spark.sql("SELECT * FROM TOP_movies ORDER BY avg_rating ASC LIMIT 10")
    print("Bottom 10 Movies:")
    worstMoviesDF.show()

    # Stop the Spark session
    spark.stop()
