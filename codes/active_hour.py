# this magic command allows saving output of python to folder
from mrjob.job import MRJob
from datetime import datetime
from collections import Counter

class MRJobActiveHour(MRJob):
    
    
    def mapper(self,_,line):
        data = line.strip().split('\t')  # getting each line of the data into a list
        if len(data) == 19 and data[0] != "id": # making sure we are skipping the header
            user = data[3]
            added_at = data[8]
            date_at = added_at[1:20]
            if len(date_at) == 19:
                date_at = datetime.strptime(date_at, "%Y-%m-%d %H:%M:%S")
                hour = date_at.hour
                yield user[1:-1], hour
            
        
    def reducer(self, user, hour):
        hour_list = list(hour)
        hour_freq = Counter(hour_list)
        max_rep = hour_freq.most_common(1)[0][1] #  getting the hour with highest number of activities
        for h, repitition in hour_freq.items():
            if repitition == max_rep:
                yield user, h  # reports the time that has highest activities
        
    
       
if __name__ == '__main__':  
    MRJobActiveHour.run()  # where MRJobCategoryCost is your job class