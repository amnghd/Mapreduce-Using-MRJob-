# this magic command allows saving output of python to folder
from mrjob.job import MRJob
#from mr3px.csvprotocol import CsvProtocol # to output in csv format

class MRJobQA(MRJob):
    #OUTPUT_PROTOCOL = CsvProtocol    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # getting each line of the data into a list
        if len(data) == 19 and line[1] != 'i':
            node_id = data[0]
            body = data[4]
            node_type = data[5]
            parent_id = data[6]
            if 'q' in node_type:  # question
                yield node_id, [len(body), node_type]  
            elif 'a' in node_type:
                yield parent_id, [len(body), node_type] 
        
    def reducer(self, node_id, length_type_pack):
        self.length_list = [0,[]]
        for pack in length_type_pack:
            pack[1] = pack[1][1:-1] # remove additional "s
            if pack[1] == 'question':
                self.length_list[0] = pack[0]
            if pack[1] == 'answer':
                self.length_list[1].append(pack[0])
            averages = 0 if len(self.length_list[1]) == 0 else sum(self.length_list[1])/len(self.length_list[1])
        yield None, (node_id[1:-1], self.length_list[0], averages)
        
if __name__ == '__main__':  
    MRJobQA.run()  # where MRJobCategoryCost is your job class