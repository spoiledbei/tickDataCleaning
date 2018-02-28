#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 10:17:31 2018

@author: congshanzhang

Derive National Best Bid Offer (NBBO) from TAQ Consolidated Quotes

The Data should be panda.DataFrame, which must include columns (0-8) 
in the following order:
    index        0    1        2    3        4        5      6      7         8      
    Date_Time    Bid, BidSize, Ask, AskSize, QU_cond, BidEx, AskEx, DateTime, Date

    where Bid Exchange (BidEX), Ask Exchange (AskEX) and Quote_condition (QU_cond)

The function getNBBO() returns a numpy array with six columns:
    'Date_Time','BestBid','BestAsk','BestBidSize','BestAskSize','BestEx'.

NOTE: The Data passed should only contain qualified quotes for NBBO computation.
"""
import pandas as pd
import numpy as np
import time

class NBBO(object):
    
    def __init__(self,Data):
        self.ExchangeList = list(Data['BidEX'].unique())
        self.prevailingBid = dict.fromkeys(self.ExchangeList)
        self.prevailingAsk = dict.fromkeys(self.ExchangeList)
        
        # current best bid/ask
        self.BestBid = 0.0
        self.BestAsk = float("inf")
        self.BestEx = '.'
        self.BestBidSize = 0
        self.BestAskSize = 0
        self.Data = Data
        self.N = len(self.Data)
        
        
    def buildNewFields(self):
        self.prevailingBid = dict.fromkeys(self.ExchangeList)
        self.prevailingAsk = dict.fromkeys(self.ExchangeList)
        self.BestBid = 0.0
        self.BestAsk = float("inf")
        self.BestEx = '.'
        self.BestBidSize = 0
        self.BestAskSize = 0
    
    def getNBBO(self):
        # Create empty numpy array to hold all NBBO
        # columns are 'Date_Time','BestBid','BestAsk','BestBidSize','BestAskSize','BestEx'
        res = np.zeros((self.N, 6),dtype=object)
        
        # saving directory
        directory_save0 = '~/Documents/Office/Data/'
        
        Date0 = self.Data.iloc[0,8]
        
        i = 0
        while i < len(self.Data):
            start_time = time.time()
            a = self.Data.iloc[i,:] # take out the i-th quote            
            if (a[8] == Date0):             
                # Update prevailing lists. 
                # key-value pair:  BidExchange--(Bid, BidSize)
                self.prevailingBid[a[5]] = (a[0],a[1])
                self.prevailingAsk[a[6]] = (a[2],a[3])
                
                # Update NBBO when a new quote occurs
                if((a[0]>self.BestAsk) | (a[2]<self.BestBid)):
                    self.BestBid = a[0]
                    self.BestAsk = a[2]
                    self.BestBidSize = self.sumSize(self.prevailingBid,self.BestBid)
                    self.BestAskSize = self.sumSize(self.prevailingAsk,self.BestAsk)
                    self.BestEx = a[5]
                    
                else:
                    if(a[0]>self.BestBid):
                        self.BestBid = self.prevailingBid[a[5]][0]
                        self.BestBidSize = self.sumSize(self.prevailingBid,self.BestBid)
                        self.BestEx = a[5]
                    
                    if(self.prevailingAsk[a[6]][0]<self.BestAsk):
                        self.BestAsk = self.prevailingAsk[a[6]][0]
                        self.BestAskSize = self.sumSize(self.prevailingAsk,self.BestAsk)
                        self.BestEx = a[6]
                
                res[i][0] = a[7]
                res[i][1] = self.BestBid
                res[i][2] = self.BestAsk
                res[i][3] = self.BestBidSize
                res[i][4] = self.BestAskSize
                res[i][5] = self.BestEx
            
                i = i + 1
                
            else:
                # after each day, save result upto this point
                #result = pd.DataFrame(data = res[:,1:], index=res[:,0],columns = ['BestBid','BestAsk','BestBidSize','BestAskSize','BestEx']) 
                #result.to_csv(directory_save0+'uptoDate.csv')
                # update current date
                Date0 = a[8] 
                # renew the prevailing lists
                self.buildNewFields()
                
            print(time.time() - start_time)
            print(i/len(self.Data))
            
        # save result
        result = pd.DataFrame(data = res[:,1:], index=res[:,0],columns = ['BestBid','BestAsk','BestBidSize','BestAskSize','BestEx']) 
        result.to_csv(directory_save0+'uptoDate.csv')
        
        return res
        
    def sumSize(self,dictionary,val):
        """
        this function will sum sizes across exchanges at a particular price val
        """
        s = 0
        for ex in self.ExchangeList:
            if(dictionary[ex] != None):
                if(dictionary[ex][0] == val):
                    s += dictionary[ex][1]
        
        return s            


     
        


