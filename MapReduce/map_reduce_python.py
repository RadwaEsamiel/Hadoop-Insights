from mrjob.job import MRJob
from mrjob.step import MRStep

class Rating_Count(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapping,
                   reducer=self.reduce)
        ]

    def mapping(self, _, line):  
        (user_id, movie_id, rating, rating_time) = line.strip().split('\t')
        yield rating, 1 

    def reduce(self, key, values):
        yield key, sum(values)  

if __name__ == '__main__':
    Rating_Count.run() 
