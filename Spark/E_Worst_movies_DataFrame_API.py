from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, round,col

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

data = "hdfs:///user/root/data/movies_data"
items = "hdfs:///user/root/data/movies_items"

movies_data_rdd = spark.read.csv(data, sep = '\t' , header = False , inferSchema=True).toDF("user_id", "movie_id", "rating", "rating_time")

movies_names_rdd = spark.read.csv(items, sep = '|' , header = False , inferSchema=True)

movies_names_rdd = movies_names_rdd.select(movies_names_rdd[0].alias("movie_id") , movies_names_rdd[1].alias("movie_name"))


ratings_movies_grouped = movies_data_rdd.groupby("movie_id").agg(
    count("rating").alias("rating_count"),
    round(avg("rating"), 2).alias("avg_rating")
).orderBy("avg_rating", ascending=True)


movies_data = ratings_movies_grouped.join(movies_names_rdd, on = "movie_id" , how = "left" )


most_and_lowest_rated_movies_data = movies_data.filter(col("rating_count") > 10)

most_and_lowest_rated_movies_data.show()

spark.stop()
