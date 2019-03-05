# this magic command allows saving output of python to folder
from mrjob.job import MRJob


class MRJobCategoryCost(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            date, time, store, category, cost, payment = data  # unpacking the data
            yield (category, float(cost))  # key pair to send to reducer
        
    def reducer(self, category,cost):
        yield category, sum(cost)  # reducer return

        
if __name__ == '__main__':  
    MRJobCategoryCost.run()  # where MRJobCategoryCost is your job class