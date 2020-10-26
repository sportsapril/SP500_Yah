#!/usr/bin/pyhon
# coding: utf-8

# In[1]:


from lxml import html  
import requests
from time import sleep
import json
import argparse
from collections import OrderedDict
from time import sleep
import pandas as pd
import datetime as dt
import os 
import shutil

parent_dir = os.getcwd()


#############################
#### Download from Yahoo ####
#############################
print(dt.datetime.now())
# create folder
# parent_dir = '/Users/aprilxu/Documents/GitHub/yah/raw/'
directory = str(dt.date.today())
path = os.path.join(parent_dir + '/raw', directory)
os.mkdir(path) 

# get a list of SP500 tickers
tickerL = pd.read_csv(parent_dir + '/SP500_Tickers.csv')


# In[2]:


# fetch data
for i,t in enumerate(tickerL.Symbol):
    if i >= 0:  # this is a breakpoint
        if i % 100 == 99:
            print('processing: ' + t)
        url = "http://finance.yahoo.com/quote/%s?p=%s"%(t,t)
        other_details_json_link = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents&corsDomain=finance.yahoo.com".format(t)
        summary_json_response = requests.get(other_details_json_link)
        json_loaded_summary = json.loads(summary_json_response.text)
        output = dict()
        output.update({'ticker':t})
        if len(json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetMedianPrice']) > 0:
            output.update({'tgt_median_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetMedianPrice']['raw']})
            output.update({'tgt_high_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetHighPrice']['raw']})
            output.update({'tgt_low_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetLowPrice']['raw']})

        output.update({'current_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']})

        if len(json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentRatio']) > 0:
            output.update({'current_ratio':json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentRatio']['raw']})

        if len(json_loaded_summary['quoteSummary']['result'][0]['financialData']['freeCashflow']) > 0:
            output.update({'free_cf':json_loaded_summary['quoteSummary']['result'][0]['financialData']['freeCashflow']['raw']})

        if 'defaultKeyStatistics' in json_loaded_summary['quoteSummary']['result'][0].keys():
            output.update({'profit_margin':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['profitMargins']['raw']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['priceToBook']) > 0:
                output.update({'priceToBook':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['priceToBook']['raw']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['earningsQuarterlyGrowth']) > 0:
                output.update({'earningQtr_growth':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['earningsQuarterlyGrowth']['raw']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['pegRatio']) > 0:
                output.update({'pegRatio':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['pegRatio']['raw']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardPE']) > 0:
                output.update({'fwd_pe':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardPE']['raw']})
            output.update({'52w_change':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['52WeekChange']['raw']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']) > 0:
                output.update({'trailingEps':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw']})
                x = json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentPrice']['raw'] / json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw']
                output.update({'PE':x})
        if 'earnings' in json_loaded_summary['quoteSummary']['result'][0].keys():
            output.update({'qtr_earning':json_loaded_summary['quoteSummary']['result'][0]['earnings']['financialsChart']['quarterly']})
            if len(json_loaded_summary['quoteSummary']['result'][0]['earnings']['earningsChart']['quarterly']) > 0:
                x = pd.DataFrame(json_loaded_summary['quoteSummary']['result'][0]['earnings']['earningsChart']['quarterly'])
                x = pd.concat([pd.concat([x['date'],x.actual.apply(lambda x: x['raw'] if isinstance(x,dict) else x)],axis=1),x.estimate.apply(lambda x: x['raw'] if isinstance(x,dict) else x)],axis=1)
                output.update({'eps_estimate_actual':x.to_dict()})


        if 'recommendationTrend' in json_loaded_summary['quoteSummary']['result'][0].keys():
            x = pd.DataFrame(json_loaded_summary['quoteSummary']['result'][0]['recommendationTrend']['trend'])
            x['Buy_Pcnt'] = (x['strongBuy'] + x['buy'])/x.sum(axis=1)
            output.update({'analyst_trend_pcntBuy':dict(x.pivot(index=None,columns='period',values='Buy_Pcnt').sum())})

        if 'calendarEvents' in json_loaded_summary['quoteSummary']['result'][0].keys():
            output.update({'current_earning':json_loaded_summary['quoteSummary']['result'][0]['calendarEvents']['earnings']})

        with open(path + '/' + t + '.json', 'w') as fp:
            json.dump(output, fp)


print('DONE: downloading')

#############################
######## Processing #########
#############################

print('Start: processing')

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
print('DONE: processing')

