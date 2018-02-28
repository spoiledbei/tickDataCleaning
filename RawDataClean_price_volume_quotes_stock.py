# ================================================================
# Created on Thu Dec 16 13:40:59 2017
#
# @author: congshanzhang
# Data Cleaning 
# High-frequency Quotes and Prices
# Resampling into 1min or 5 min research data
# There are 386 prices and 78 prices per trading day for 1min and
# 5min frequencies, respectively.
# ================================================================
import os

path = '~/Documents/Office/ResearchCode'
os.chdir(path)

import pandas as pd
import numpy as np
import time
#from multiprocessing import Pool, cpu_count
from NBBO import NBBO

directory = '~/Documents/Office/Data/stock_TickData/'
directory_save0 = '~/Documents/Office/Data/'
directory_save = '~/Documents/Office/Data/stock_RegularSpacedData/'

"""
step 0) Preliminary Cleaning
"""
# Read Price and volume. 
# Row 0 to be the header; Rename cols; Keep certain cols; Combine 'Date' and 'Time'
df001 = pd.read_csv(directory+'citi_trade(Jan2016).csv',header=0,\
                 names=['Date','Time','SYM','SYM_suffix','Size','Price'],\
                 usecols=['Date','Time','Size','Price'],\
                 parse_dates=[['Date', 'Time']])

# Only keep quotes at trading times
df001 = df001.set_index('Date_Time')
df001 = df001.between_time('9:30','16:00',include_start=True, include_end=True)
df001.head()

# Read Quotes. 
# Row 0 to be the header; Rename cols; Keep certain cols; Combine 'Date' and 'Time'
df002 = pd.read_csv(directory+'citi_quote(Jan2016).csv',header=0,\
                 names=['Date','Time','EX','SYM','SYM_suffix','Bid','BidSize','Ask','AskSize','QU_cond','BidEX','AskEx','NBBO_Ind'],\
                 usecols=['Date','Time','Bid','BidSize','Ask','AskSize','QU_cond','BidEX','AskEx'],\
                 parse_dates=[['Date', 'Time']])


# Only keep quotes at trading times
df002 = df002.set_index('Date_Time',drop=True)
df002.head()

# add columns at the end: DateTime and Date
time_start = time.time()
df002['DateTime'] = df002.index
print(time.time()-time_start)
        
time_start = time.time()
df002['Date'] = df002['DateTime'].apply(lambda x: x.strftime('%Y%m%d'))
print(time.time()-time_start)

# only consider trading hours
df002 = df002.between_time('9:25','16:00',include_start=True, include_end=True)


"""
step 1) Deriving NBBO from consolidated quotes
"""
# remove unqualified quotes
# Following "Deriving NBBO & Quote Bars from US CQS Level 1 Quote Data"
# and NBBO Derivation using SAS Data Views from WRDS
df002 = df002.loc[ df002['QU_cond'].isin(['A','B','E','F','H','O','R','W','Y']) ]
df002 = df002.loc[ (df002['Bid']>0.01) & (df002['Ask']>0) & (df002['BidSize']>0) & (df002['AskSize']>0) & (df002['Bid']<df002['Ask']) ]

# derive NBBO
Algorithm = NBBO(df002)
res = Algorithm.getNBBO()

# read NBBO file
df_NBBO = pd.read_csv(directory_save0+'uptoDate.csv')
df_NBBO.head()
df_NBBO.info()

# keep observations between 9:30-16:00
df_NBBO['Date_Time'] = pd.to_datetime(df_NBBO['Unnamed: 0'])
df_NBBO = df_NBBO.set_index('Date_Time')
df_NBBO = df_NBBO.between_time('9:30','16:00',include_start=True, include_end=True)
df_NBBO = df_NBBO[['BestBid','BestAsk','BestBidSize','BestAskSize','BestEx']]

"""
step 2) Merge quotes(NBBO), prices, volumes
"""
# outer merge on index (Date_Time)
dat = pd.merge(df001,df_NBBO,how='outer',left_index=True, right_index=True)

# forward fill bids and asks
dat[['BestBid','BestAsk','BestBidSize','BestAskSize','BestEx']] = dat[['BestBid',\
   'BestAsk','BestBidSize','BestAskSize','BestEx']].fillna(method='ffill')
dat = dat.dropna(axis=0, how='any')

# mid quote
dat['mid'] = (dat['BestBid'] + dat['BestAsk'])/2.0
   
# spread
dat['spread'] = dat['BestAsk'] - dat['BestBid']
dat = dat.loc[dat['spread']>=0]  # only keep nonnegative spread

# effective spread
#dat['effec_spread'] = 2.0*(dat['Price'] - dat['mid'])

dat.head()
dat.to_csv(directory+'citi_alltickNBBO(Jan2016).csv')


"""
step 3) Resample into 1-min data
"""
dat = pd.read_csv(directory+'citi_alltickNBBO(Jan2016).csv')


# generate Date variable
dat.reset_index(inplace=True)  
dat['Date'] = dat.iloc[:,0].apply(lambda x: x.strftime('%Y%m%d')) 

# set index back into Date_Time
dat = dat.set_index('Date_Time')

freq = '1min'

# an empty DataFrame
data1min = pd.DataFrame()

# grouped by date
grouped = dat.groupby(['Date'])

# Resample at 1-min frequency
# Take the last value in the left-closed time interval and label the time as the right open end
data1min = grouped.resample(freq, closed='left', label='right', how='last').fillna(method='ffill')

# save
data1min.to_csv(directory_save+'SPY_1min(Nov2016).csv')




















