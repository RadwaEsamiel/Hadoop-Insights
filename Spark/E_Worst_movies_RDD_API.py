from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

data = "hdfs:///user/root/data/movies_data"
items = "hdfs:///user/root/data/movies_items"

# Load the data as RDDs
movies_data_rdd = spark.sparkContext.textFile(data).map(lambda line: line.split('\t')).map(
    lambda row: (int(row[1]), (float(row[2]), 1)))


movies_names_rdd = spark.sparkContext.textFile(items).map(lambda line: line.split('|')).map(
    lambda row: (int(row[0]), row[1])  
)

# Reduce by key to sum ratings and counts
ratings_sum_count_rdd = movies_data_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Filter movies with more than 10 ratings and calculate average rating
most_rated_movies_rdd = ratings_sum_count_rdd.filter(lambda x: x[1][1] > 10)

most_lowest_rated_movies_rdd = most_rated_movies_rdd.map(
    lambda x: (x[0], x[1][0] / x[1][1])  # (movie_id, avg_rating)
)

most_lowest_rated_movies_withnames = most_lowest_rated_movies_rdd.join(movies_names_rdd)

most_lowest_rated_movies_withnames_sorted = most_lowest_rated_movies_withnames.sortBy(lambda x : x[1][0])

# Collect and print results
for movie in most_lowest_rated_movies_withnames_sorted.collect():
    print(movie)

spark.stop()
