#!/usr/bin/python

# this magic command allows saving output of python to folder
from mrjob.job import MRJob
#from mr3px.csvprotocol import CsvProtocol # to output in csv format

class MRJobJoin(MRJob):
    #OUTPUT_PROTOCOL = CsvProtocol
    def mapper(self,_,line):
        data = line.split("\t")
        if len(data) == 5 and data[0] != "user_ptr_id": # making sure we are skipping the header
            user, reputation, gold, silver, bronze = data # unpacking the data
            yield user, ["U", reputation, gold, silver, bronze]  # U is used to flag user database
        elif len(data) == 19 and data[0] != "id": # making sure we are skipping the header
            id_tag = data[0]
            title = data[1]
            tagnames = data[2]
            user = data[3]
            node_type = data[5]
            parent_id = data[6]
            abs_parent_id = data[7]
            added_at = data[8]
            score = data[9]
            yield user, ["N", id_tag, title, tagnames, node_type, parent_id, abs_parent_id, added_at, score]
            
    def reducer(self, user, packed_values):
        self.outlist = [None for _ in range(13)]  # empty list to keep the joined data
        self.outlist[3] = user  # user id is the key
        for line in packed_values:
            if line[0] == 'U': # coming from the user file
                self.outlist[9] = line[1] # reputation
                self.outlist[10] = line[2] # gold
                self.outlist[11] = line[3] # silver
                self.outlist[12] = line[4] # bronze
            elif line[0] == 'N':  # coming from the node file
                self.outlist[0] = line[1]
                self.outlist[1] = line[2]
                self.outlist[2] = line[3]      
                self.outlist[4] = line[4]
                self.outlist[5] = line[5]
                self.outlist[6] = line[6]
                self.outlist[7] = line[7]      
                self.outlist[8] = line[8]
                yield None, self.outlist
    
        
    
       
if __name__ == '__main__':  
    MRJobJoin.run()  # where MRJobCategoryCost is your job class