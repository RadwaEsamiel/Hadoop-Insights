--Top Rated Movies
SELECT t.movieID, n.movie_title, AVG(t.rating) AS avg_rating, COUNT(t.userID) AS ratingCount 
FROM movies_data t JOIN movies_items n ON t.movieID = n.movie_id GROUP BY t.movieID , n.movie_title HAVING COUNT(t.userID) > 10 ORDER BY avg_rating DESC LIMIT 10;

--lawest Rated Movies
SELECT t.movieID, n.movie_title, AVG(t.rating) AS avg_rating, COUNT(t.userID) AS ratingCount 
FROM movies_data t JOIN movies_items n ON t.movieID = n.movie_id GROUP BY t.movieID , n.movie_title ORDER BY avg_rating LIMIT 10;

--Most Rated Movies
SELECT t.movieID, n.movie_title, AVG(t.rating) AS avg_rating, COUNT(t.userID) AS ratingCount 
FROM movies_data t JOIN movies_items n ON t.movieID = n.movie_id GROUP BY t.movieID , n.movie_title ORDER BY ratingCount desc LIMIT 10;

--oldest movies 
SELECT t.movieID, n.movie_title,n.release_date, AVG(t.rating) AS avg_rating, COUNT(t.userID) AS ratingCount FROM movies_data t JOIN movies_items n ON t.movieID = n.movie_id 
WHERE n.release_date = (SELECT MIN(release_date) FROM movies_items)GROUP BY t.movieID , n.movie_title,n.release_date ;

--newest movies
SELECT t.movieID, n.movie_title,n.release_date, AVG(t.rating) AS avg_rating, COUNT(t.userID) AS ratingCount FROM movies_data t JOIN movies_items n ON t.movieID = n.movie_id 
WHERE n.release_date = (SELECT max(release_date) FROM movies_items)GROUP BY t.movieID , n.movie_title,n.release_date ;