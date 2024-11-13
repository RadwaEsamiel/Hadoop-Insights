from pyspark import SparkContext
from pyspark.sql import SparkSession

# Initialize a Spark context and session
sc = SparkContext(appName="Movie Ratings Analysis")
spark = SparkSession(sc)

# Load the user ratings data into an RDD
movies_ratings_rdd = sc.textFile("hdfs:///user/root/data/movies_data").map(lambda line: line.split())

# Create RDD with (user_id, movie_id, rating, rating_time)
movies_ratings_rdd = movies_ratings_rdd.map(lambda x: (int(x[0]), int(x[1]), float(x[2]), int(x[3])))

# Load the movies names data into an RDD
movies_names_rdd = sc.textFile("hdfs:///user/root/data/movies_items") \
    .map(lambda line: line.split("|")) \
    .map(lambda x: (int(x[0]), x[1]))  # (movie_id, movie_title)

# Group by movie_id and calculate rating count and average rating
rating_count_avg_rdd = (
    movies_ratings_rdd.map(lambda x: (x[1], (1, x[2])))  # Map to (movie_id, (1, rating))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Reduce to (movie_id, (rating_count, total_rating))
    .map(lambda x: (x[0], x[1][0], x[1][1] / x[1][0]))  # Map to (movie_id, rating_count, avg_rating)
)

# Filter to get movies with rating count > 10
filtered_movies_rdd = rating_count_avg_rdd.filter(lambda x: x[1] > 10)

# Sort all filtered movies by avg_rating in ascending order
sorted_movies_rdd = filtered_movies_rdd.sortBy(lambda x: x[2])

# Convert to DataFrame for easier display and to join with movie titles
movies_df = spark.createDataFrame(sorted_movies_rdd, ["movie_id", "rating_count", "avg_rating"])

# Join with movies names RDD
movies_with_names_df = movies_df.join(
    spark.createDataFrame(movies_names_rdd, ["movie_id", "movie_title"]),
    on="movie_id",
    how="left"
)

# Show the result ordered by avg_rating from lowest to highest
movies_with_names_df.orderBy("avg_rating").show()

# Stop the Spark context
sc.stop()
