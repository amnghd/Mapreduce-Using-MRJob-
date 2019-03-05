# this magic command allows saving output of python to folder
from mrjob.job import MRJob

address = '/assets/js/the-associates.js'

class MRJobAssociates(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split(' ')  # delete whitespace and tokenize
        if len(data) == 10:
            request = data[6]  # unpacking the data
            if address in request:
                yield (address, 1)  # key pair to send to reducer
        
    def reducer(self, category,hits):
        yield address, sum(hits)  # reducer return

        
if __name__ == '__main__':  
    MRJobAssociates.run()  # where MRJobCategoryCost is your job class