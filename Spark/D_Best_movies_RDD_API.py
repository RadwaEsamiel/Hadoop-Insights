from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()


data = "hdfs:///user/root/data/movies_data"
items = "hdfs:///user/root/data/movies_items"

# Load the data as RDDs
movies_data_rdd = spark.sparkContext.textFile(data).map(lambda line: line.split('\t')).map(
    lambda row: (int(row[1]), (float(row[2]), 1))
)

movies_names_rdd = spark.sparkContext.textFile(items).map(lambda line: line.split('|')).map(
    lambda row: (int(row[0]), row[1])  # (movie_id, movie_title)
)

# Reduce by key to sum ratings and counts
ratings_sum_count_rdd = movies_data_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Filter movies with more than 10 ratings and calculate average rating
most_rated_movies_rdd = ratings_sum_count_rdd.filter(lambda x: x[1][1] > 10)

# Calculate average rating (movie_id, avg_rating)
best_rated_movies_rdd = most_rated_movies_rdd.map(
    lambda x: (x[0], x[1][0] / x[1][1])
)

# Join with movie names
best_rated_movies_with_names = best_rated_movies_rdd.join(movies_names_rdd)

# Sort by avg_rating in descending order for best-rated movies
best_rated_movies_sorted = best_rated_movies_with_names.sortBy(lambda x: -x[1][0])

# Collect and print results
for movie in best_rated_movies_sorted.collect():
    print(movie)

# Stop the Spark session
spark.stop()
