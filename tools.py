#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


class cdp:
    
    def __init__(self, input_df):
        self.input_df = input_df
        self.all_df = []
    
    def data_return(self, q_num, colnum, required_columns, rownum = None):
        
        input_df = self.input_df
        
        if rownum is not None:
            filt = (input_df['Question Number'] == str(q_num)) & (input_df['Column Number'] == int(colnum)) & (input_df['Row Number'] == int(rownum))
        else:
            filt = (input_df['Question Number'] == str(q_num)) & (input_df['Column Number'] == int(colnum))
        df = input_df[filt]
        df = df[required_columns]
        if rownum is not None:
            newname = list(set(input_df[filt]["Row Name"]))[0]
        else:
            newname = list(set(input_df[filt]["Column Name"]))[0]
        df.rename(columns={'Response Answer': newname}, inplace = True)
        df = df.set_index('Account Number')
        self.all_df.append(df)
        return df
        
    def queries(self, questions, colnums, required_columns, rownums = None):
        
        df_list_temp = []
        if rownums is not None:
            q = list(zip(questions, colnums, rownums))
            for item in q:
                temp = self.data_return(q_num = item[0], colnum = item[1], required_columns = required_columns, rownum = item[2])
                df_list_temp.append(temp)
        else:
            q = list(zip(questions, colnums))
            for item in q:
                temp = self.data_return(q_num = item[0], colnum = item[1], required_columns = required_columns)
                df_list_temp.append(temp)
        return df_list_temp
    
    def combine_df(self):
        
        all_df = self.all_df
        comb_df = all_df[0]
        
        for i in range(1, len(all_df)):
            comb_df = pd.merge(comb_df, all_df[i], left_index=True, right_index=True, how="outer")
        return comb_df 
            
    def process(self, addtional_columns):
        comb_df = self.combine_df()
        input_df = self.input_df
        input_df_short = input_df[addtional_columns]
        input_df_short = input_df_short.drop_duplicates(subset=['Account Number'])
        input_df_short = input_df_short.set_index('Account Number')
        final_df = input_df_short.join(comb_df)
        return final_df
    
            

