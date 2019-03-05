# this magic command allows saving output of python to folder
from mrjob.job import MRJob


class MRJobTransNum(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            yield (None, 1)  # no key availale
        
    def reducer(self, category,cost):
        yield 'Total Number = ', sum(cost) # reducer return

        
if __name__ == '__main__':  
    MRJobTransNum.run()  # where MRJobCategoryCost is your job class