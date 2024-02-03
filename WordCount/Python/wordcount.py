# importing the library of mrjob to work with MapReduce
from mrjob.job import MRJob

# Defining a class that will inherit from MRJob class
class WordCount(MRJob):
    # Definning the mapper for the WordCount
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield (word.lower(), 1)

    # Definning the Reducer for the WordCount
    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    # Executing the MapReduce Runner
    WordCount.run()
