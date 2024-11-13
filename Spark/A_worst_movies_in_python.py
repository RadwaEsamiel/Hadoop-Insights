import pandas as pd

# Load the file directly into a DataFrame
movies_ratings_df = pd.read_csv("../data/u.data", delim_whitespace=True, names=["user_id", "movie_id", "rating", "rating_time"])


column_names = ["movie_id", "movie_title"]
movies_names_df = pd.read_csv(
    "../data/u.item",  
    delimiter='|', 
    names=column_names,
    usecols=[0, 1],      
    encoding='ISO-8859-1'
)

# Group by 'movie_id' and calculate both count and average rating, then sort by rating count
movies_count_avg = movies_ratings_df.groupby("movie_id").agg(
    rating_count=("rating", "count"),   
    avg_rating=("rating", "mean")
).sort_values(by="avg_rating", ascending=True)


# Display the top 10 movies by rating count
filtered_lowest_rated_movies = movies_count_avg[movies_count_avg.rating_count > 10]
lowest_rated_movies = filtered_lowest_rated_movies.head(10)

top10_lowest_rated_movies = lowest_rated_movies.merge(
    movies_names_df[['movie_id', 'movie_title']], 
    on='movie_id', 
    how='left'
)
print(top10_lowest_rated_movies)
