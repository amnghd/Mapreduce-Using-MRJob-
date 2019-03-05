# this magic command allows saving output of python to folder
from mrjob.job import MRJob
from datetime import datetime

class MRJobWeeklySales(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # delete whitespace and tokenize
        if len(data) == 6:
            date = data[0]
            cost = data[4]
            day = datetime.strptime(date, '%Y-%m-%d').weekday()
            weekmap = {0:'Mon', 1:'Tue', 2:'Wed', 3:'Thu', 4:'Fri', 5:'Sat', 6:'Sun'}
            
            yield (weekmap[day], float(cost))  # no key availale
                
    def reducer(self, day, cost):
        n = total = 0
        
        for c in cost:
            n += 1
            total += c
        yield day, total / n # reducer return

        
if __name__ == '__main__':  
    MRJobWeeklySales.run()  # where MRJobCategoryCost is your job class