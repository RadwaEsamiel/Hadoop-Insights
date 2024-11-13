from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

movies_ratings_df = spark.read.csv(
    "hdfs:///user/root/data/movies_data", 
    sep="\t",  
    header=False, 
    inferSchema=True  
).toDF("user_id", "movie_id", "rating", "rating_time")

movies_names= spark.read.csv(
    "hdfs:///user/root/data/movies_items", 
    sep="|",  
    header=False, 
    inferSchema=True  
)


movies_names_df = movies_names.select([movies_names[0].alias("movie_id"),movies_names[1].alias("movie_title")])


movies_count_avg = movies_ratings_df.groupBy("movie_id").agg(
    count("rating").alias("rating_count"),
    avg("rating").alias("avg_rating")
).orderBy(col("avg_rating").desc())


highest_rated_moveies = movies_count_avg.filter(col("rating_count") > 10)


top_highest_rated_moveies = highest_rated_moveies.join(movies_names_df, on = "movie_id", how = "left")
top_highest_rated_moveies.show()

spark.stop()