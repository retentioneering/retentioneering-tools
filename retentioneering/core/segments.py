import pandas as pd

class Segments(object):

    def __init__(self, users):
        self.segments = pd.DataFrame({'user_col': users}) 
        self.segments['initial_dataset_users.0']=1
        #self.segments.set_index('user_col')

    def show_segments(self):
        return self.segments
    
    def get_users(self):
        return self.segments['user_col']
    
    def add_segment(self, segment_name,users_of_segment):
        segment_name = str(segment_name)+'.0'
        users_of_segment = pd.Series(users_of_segment).unique()
        
        #new_segment = pd.DataFrame({'user_col': users_of_segment})
        #new_segment[segment_name]=True
        #self.segments=pd.merge(left=self.segments, right=new_segment,how='left', left_on='user_col', right_on='user_col',indicator=False)
        #self.segments=self.segments.join(pd.concat([new_segment], axis=1, keys=['user_col']))
        self.segments[segment_name]=0
        self.segments.loc[self.segments['user_col'].isin(users_of_segment), segment_name] = 1
        
    def add_segment_from_df(self, dfn):
        self.segments=pd.merge(left=self.segments, right=dfn,how='left', left_on='user_col', right_on='user_col',indicator=False)
        #self.segments=self.segments.join(pd.concat([dfn], axis=1, keys=['user_col']))