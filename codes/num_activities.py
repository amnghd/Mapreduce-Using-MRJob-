# this magic command allows saving output of python to folder
from mrjob.job import MRJob

ip = '10.99.99.186'

class MRJobIP(MRJob):
    
    def mapper(self,_,line):
        data = line.strip().split(' ')  # delete whitespace and tokenize
        if len(data) == 10:
            ip_address = data[0]  # unpacking the data
            if ip_address == ip:
                yield (ip, 1)  # key pair to send to reducer
        
    def reducer(self, ip_address,hits):
        yield ip_address, (hits)  # reducer return

        
if __name__ == '__main__':  
    MRJobIP.run()  # where MRJobCategoryCost is your job class