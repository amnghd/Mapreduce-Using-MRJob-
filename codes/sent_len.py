# this magic command allows saving output of python to folder
from mrjob.job import MRJob

class MRJobSentLen(MRJob):
    
    def mapper(self,_,line):
        yield None, len(line)
        
    def reducer(self, _, lens):
        n = total = 0
        for l in lens:
            n += 1
            total += l
            
        yield "Sentence length average:", total / n
        
    
       
if __name__ == '__main__':  
    MRJobSentLen.run()  # where MRJobCategoryCost is your job class