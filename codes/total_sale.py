# this magic command allows saving output of python to folder
from mrjob.job import MRJob


class MRJobTotalSale(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            cost = data[4]  # unpacking the data
            yield (None, float(cost))  # no key availale
        
    def reducer(self, category,cost):
        yield 'Total = ', round(sum(cost),2)  # reducer return

        
if __name__ == '__main__':  
    MRJobTotalSale.run()  # where MRJobCategoryCost is your job class