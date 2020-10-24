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

# create folder
parent_dir = '/Users/aprilxu/Documents/GitHub/yah/'
directory = str(dt.date.today())
path = os.path.join(parent_dir, directory)
os.mkdir(path) 

# get a list of SP500 tickers
tickerL = pd.read_csv(parent_dir + 'SP500_Tickers.csv')

# fetch data
for t in tickerL.Symbol:
    print('processing: ' + t)
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(t,t)
    other_details_json_link = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents&corsDomain=finance.yahoo.com".format(t)
    summary_json_response = requests.get(other_details_json_link)
    json_loaded_summary = json.loads(summary_json_response.text)
	output = dict()
	output.update({'ticker':t})
	output.update({'tgt_median_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetMedianPrice']['raw']})
	output.update({'tgt_high_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetHighPrice']['raw']})
	output.update({'tgt_low_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['targetLowPrice']['raw']})
	output.update({'current_price':json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']})
	output.update({'current_ratio':json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentRatio']['raw']})
	output.update({'free_cf':json_loaded_summary['quoteSummary']['result'][0]['financialData']['freeCashflow']['raw']})
	output.update({'profit_margin':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['profitMargins']['raw']})
	output.update({'priceToBook':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['priceToBook']['raw']})
	output.update({'earningQtr_growth':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['earningsQuarterlyGrowth']['raw']})
	output.update({'revenueQtr_growth':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['revenueQuarterlyGrowth']})
	output.update({'pegRatio':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['pegRatio']['raw']})
	output.update({'fwd_pe':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['forwardPE']['raw']})
	output.update({'52w_change':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['52WeekChange']['raw']})
	output.update({'trailingEps':json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw']})
	x = json_loaded_summary['quoteSummary']['result'][0]['financialData']['currentPrice']['raw'] / json_loaded_summary['quoteSummary']['result'][0]['defaultKeyStatistics']['trailingEps']['raw']
	output.update({'PE':x})
	output.update({'qtr_earning':json_loaded_summary['quoteSummary']['result'][0]['earnings']['financialsChart']['quarterly']})
	x = pd.DataFrame(json_loaded_summary['quoteSummary']['result'][0]['recommendationTrend']['trend'])
	x['Buy_Pcnt'] = (x['strongBuy'] + x['buy'])/x.sum(axis=1)
	output.update({'analyst_trend_pcntBuy':dict(x.pivot(index=None,columns='period',values='Buy_Pcnt').sum())})
	output.update({'current_earning':json_loaded_summary['quoteSummary']['result'][0]['calendarEvents']['earnings']})
	x = pd.DataFrame(json_loaded_summary['quoteSummary']['result'][0]['earnings']['earningsChart']['quarterly'])
	x = pd.concat([pd.concat([x['date'],x.actual.apply(lambda x: x['raw'])],axis=1),x.estimate.apply(lambda x: x['raw'])],axis=1)
	output.update({'eps_estimate_actual':x.to_dict()})
	with open(path + '/' + t + '.json', 'w') as fp:
	    json.dump(output, fp)
