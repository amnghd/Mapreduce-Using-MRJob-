# this magic command allows saving output of python to folder
from mrjob.job import MRJob
from datetime import datetime

class MRJobWeeklyTotals(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            date = data[0]
            cost = data[4]
            day = datetime.strptime(date, '%Y-%m-%d').weekday()
            weekmap = {0:'Mon', 1:'Tue', 2:'Wed', 3:'Thu', 4:'Fri', 5:'Sat', 6:'Sun'}
            yield (weekmap[day], float(cost))  # no key availale
    
    def combiner(self, day, cost): # performing sum aggregation locally
        yield day, sum(cost)
        
    def reducer(self, day, cost):
        yield day, sum(cost) # reducer return

        
if __name__ == '__main__':  
    MRJobWeeklyTotals.run()  # where MRJobCategoryCost is your job class