#!/usr/bin/env python
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

parent_dir = os.getcwd()

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

