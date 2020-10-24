#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import pandas as pd
import datetime as dt
import os 
import shutil

# create folder
parent_dir = os.getcwd()


# In[ ]:


x = os.listdir(parent_dir + '/raw')
x = [k for k in x if not k.startswith('.')]
path = os.path.join(parent_dir + '/raw', x[0])
files = os.listdir(path)


# In[ ]:


def structJson(res):
    
    keys1 = ['ticker', 'tgt_median_price', 'tgt_high_price', 'tgt_low_price', 'current_price', 
         'current_ratio', 'free_cf', 'profit_margin', 'priceToBook', 'earningQtr_growth', 
         'pegRatio', 'fwd_pe', '52w_change', 'trailingEps', 'PE']

    
    # basic summary stats
    missingCol = list(set(keys1) - set(res.keys()))
    keys2 = set(keys1).intersection( set(res.keys()))
    tmp1 = pd.DataFrame([res[x] for x in keys2]).T
    tmp1.columns = keys2

    for c in missingCol:
        tmp1[c] = None
    
    # analyst trend
    tmp2 = pd.DataFrame()
    if 'analyst_trend_pcntBuy' in res.keys():
        tmp2 = pd.DataFrame(list(res['analyst_trend_pcntBuy'].values())).T
        tmp2.columns = list(res['analyst_trend_pcntBuy'].keys())
    
    
    # earnings
    tmp3 = pd.DataFrame()
    if 'current_earning' in res.keys():
        tmpDict = {}
        if res['current_earning']['earningsDate'] != []:
            tmpDict.update({'current_earning_d':res['current_earning']['earningsDate'][0]['fmt']})
            if len(res['current_earning']['earningsAverage']) > 0:
                tmpDict.update({'current_earning_avg':res['current_earning']['earningsAverage']['raw']})
            if len(res['current_earning']['earningsLow']) > 0:
                tmpDict.update({'current_earning_low':res['current_earning']['earningsLow']['raw']})
            if len(res['current_earning']['earningsHigh']) > 0:
                tmpDict.update({'current_earning_high':res['current_earning']['earningsHigh']['raw']})
            if len(res['current_earning']['revenueAverage']) > 0:
                tmpDict.update({'current_revenue_avg':res['current_earning']['revenueAverage']['raw']})
            if len(res['current_earning']['revenueHigh']) > 0:    
                tmpDict.update({'current_revenue_high':res['current_earning']['revenueHigh']['raw']})
            if len(res['current_earning']['revenueLow']) > 0:    
                tmpDict.update({'current_revenue_low':res['current_earning']['revenueLow']['raw']})
            
            tmp3 = pd.DataFrame(list(tmpDict.values())).T
            tmp3.columns = list(tmpDict.keys())
    
    # put together
    tmp = pd.concat([pd.concat([tmp1,tmp2],axis=1),tmp3],axis=1)
    
    
    # with time dims
    tmp1 = pd.DataFrame()
    tmp2 = pd.DataFrame()
    tmp_T = pd.DataFrame()
    if 'qtr_earning' in res.keys():
        if len(res['qtr_earning']) > 0:
            tmp1 = pd.DataFrame(res['qtr_earning'])
            if isinstance(res['qtr_earning'][0]['revenue'],dict):
                tmp1 = pd.concat([pd.concat([tmp1['date'],tmp1.revenue.apply(lambda x: x['raw'] if isinstance(x,dict) else x)],axis=1),
                                  tmp1.earnings.apply(lambda x: x['raw'] if isinstance(x,dict) else x)],axis=1)
            
    
    if 'eps_estimate_actual' in res.keys():
        tmp2 = pd.DataFrame(res['eps_estimate_actual'])
    
    if ('qtr_earning' in res.keys()) & ('eps_estimate_actual' in res.keys()):
        tmp_T = pd.merge(left = tmp1,
                     right = tmp2,
                     how = 'left',
                     on = 'date')
        tmp_T['ticker'] = res['ticker']
    
    return tmp, tmp_T


# In[ ]:


f = open(path + '/AAPL.json') 
res = json.load(f) 
out, out_T = structJson(res)


# In[ ]:


for f in files:
    if f != 'AAPL.json':
        p = open(path + '/' + f) 
        res = json.load(p) 
        tmp, tmp_T = structJson(res)
        
        out = out.append(tmp)
        out_T = out_T.append(tmp_T)
out['as_of_date'] = x[0]
out_T['as_of_date'] = x[0]


# In[ ]:


out = out[['ticker','as_of_date','-1m', '-2m', '-3m', '0m', '52w_change', 'PE', 'current_earning_avg',
       'current_earning_d', 'current_earning_high', 'current_earning_low',
       'current_price', 'current_ratio', 'current_revenue_avg',
       'current_revenue_high', 'current_revenue_low', 'earningQtr_growth',
       'free_cf', 'fwd_pe', 'pegRatio', 'priceToBook', 'profit_margin',
       'tgt_high_price', 'tgt_low_price', 'tgt_median_price','trailingEps']]


# In[ ]:


out['growthPotential'] = out['tgt_median_price'] / out['current_price'] - 1


# In[ ]:


inData = pd.read_csv(parent_dir + '/output/SP500_Consolidated.csv')
inData = inData.drop(['Unnamed: 0'],axis=1)
inData.columns = out.columns
out = inData.append(out,ignore_index=True)


# In[ ]:


out.to_csv(parent_dir + '/output/SP500_Consolidated.csv')


# In[ ]:


inData = pd.read_csv(parent_dir + '/output/SP500_Consolidated_T.csv')
inData = inData.drop(['Unnamed: 0'],axis=1)
inData.columns = out_T.columns
out_T = inData.append(out_T,ignore_index=True)


# In[ ]:


out_T.to_csv(parent_dir + '/output/SP500_Consolidated_T.csv')


# In[ ]:


shutil.move(path,parent_dir + '/processed/')

