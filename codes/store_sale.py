# this magic command allows saving output of python to folder
from mrjob.job import MRJob


class MRJobStoreSales(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            date, time, store, category, cost, payment = data  # unpacking the data
            if store in ['Reno','Toledo','Chandler']:  # we are only interested in these cities
                yield (store, float(cost))  # key pair to send to reducer
        
    def reducer(self, category,cost):
        yield category, max(cost)  # reducer return

        
if __name__ == '__main__':  
    MRJobStoreSales.run()  # where MRJobCategoryCost is your job class