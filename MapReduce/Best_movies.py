from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesAvgRating(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_calculate_avg),
            MRStep(reducer=self.Best_movies)
        ]

    def mapper_get_ratings(self, _, line):
        # Parse the input file assuming tab-separated values
        user_id, movie_id, rating, rating_time = line.strip().split('\t')
        yield movie_id, float(rating)

    def reducer_calculate_avg(self, movie_id, ratings):
        # Sum up the ratings and count them
        ratings_list = list(ratings)
        rating_sum = sum(ratings_list)
        rating_count = len(ratings_list)

        # Only yield movies with more than 10 ratings
        if rating_count > 10:
            avg_rating = round((rating_sum / rating_count),2)
            yield None, (avg_rating, rating_count, movie_id)

    
    def Best_movies(self, _, movie_data):
        # Sort movies by average rating in descending order (best to worst)
        sorted_movies = sorted(movie_data, reverse=True)
        for avg_rating, rating_count, movie_id in sorted_movies:
            yield movie_id, (avg_rating, rating_count)

if __name__ == '__main__':
    MoviesAvgRating.run()
