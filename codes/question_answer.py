# this magic command allows saving output of python to folder
from mrjob.job import MRJob

class MRJobQACorr(MRJob):
    
    
    def mapper(self,_,line):
        data = line.strip().split(',')  # getting each line of the data into a list
        node_id = data[0]
        body = data[4]
        node_type = data[5]
        yield node_id, [len(body), node_type]    

if __name__ == '__main__':  
    MRJobQACorr.run()  