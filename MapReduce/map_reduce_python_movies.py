from mrjob.job import MRJob
from mrjob.step import MRStep

class Movies_Rating_Count(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapping,
                   reducer=self.reduce),
                   MRStep(reducer= self.reducer_sorter)
        ]

    def mapping(self, _, line):  
        (user_id, movie_id, rating, rating_time) = line.strip().split('\t')
        yield movie_id, 1 

    def reduce(self, key, values):
        yield str(sum(values)).zfill(5),key

    def reducer_sorter(self, counts, movies):
        for movie in movies :
            yield movie, counts

if __name__ == '__main__':
    Movies_Rating_Count.run() 
