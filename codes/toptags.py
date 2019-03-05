# this magic command allows saving output of python to folder
from mrjob.job import MRJob
from mrjob.step import MRStep
class MRJobTags(MRJob):
    def mapper(self,_,line):
        data = line.strip().split('\t')  # getting each line of the data into a list
        if len(data) == 19 and line[1] != 'i':
            node_type = data[5]
            tagnames = data[2][1:-1] # tagnames
            tag_list = tagnames.strip().split(' ')
            if 'q' in node_type:  # question
                for tag in tag_list:
                    yield tag, 1
    def combiner(self, tag, count)                    :
        yield tag, sum(count)
        
    def reducer_counter(self, tag, sum_count):
        yield None, (sum(sum_count),tag)  # reducer return all the couts with the same key
        
    def reducer_final(self, _, count_tag_pack):
        self.toplist = []
        for pack in count_tag_pack:
            self.toplist.append(pack)
        pack_list = sorted(self.toplist, reverse=True)[:10]
        for p in pack_list:
            yield p[1], p[0]
            
            
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  combiner=self.combiner,
                  reducer=self.reducer_counter),
            MRStep(reducer=self.reducer_final)
        ]
if __name__ == '__main__':  
    MRJobTags.run()  # where  is your job class                    