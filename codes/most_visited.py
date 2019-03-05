# this magic command allows saving output of python to folder
from mrjob.job import MRJob
from mrjob.step import MRStep
prefix = "http://www.the-associates.co.uk"
class MRJobMostFreqRequest(MRJob):
    
    def mapper_get_addresses(self,_,line):
        data = line.strip().split(' ')  # delete whitespace and tokenize
        if len(data) == 10:
            request = data[6]  # unpacking the data
            if prefix in request:
                request = request[len(prefix):] # deleting prefix from addresses
            yield (request, 1)  # key pair to send to reducer
    
    def combiner_count_hits(self, request, hits):
        yield (request, sum(hits)) # sum the request seen so far
    
    def reducer_count_hits(self, request,hits):
        yield None, (sum(hits),request)  # reducer return all the couts with the same key
    
    def reducer_find_max_request(self, _, request_hit_pairs):
        yield max(request_hit_pairs) # max function returns the row with highest first element

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_addresses,
                  combiner=self.combiner_count_hits,
                  reducer=self.reducer_count_hits),
            MRStep(reducer=self.reducer_find_max_request)
        ]
if __name__ == '__main__':  
    MRJobMostFreqRequest.run()  # where MRJobCategoryCost is your job class