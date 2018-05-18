#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Mon May  7 20:08:32 2018

@author: fhulufhelo
"""
import glob
import pandas as pd
import numpy as np
import argparse
import time
import os
from multiprocessing import Pool

parser=argparse.ArgumentParser(description='')
parser.add_argument('directory',type=str,help='directory of GDELT file')
parser.add_argument('latitude',type=int,help='latitude of a location')
parser.add_argument('longitude',type=int,help='longitute of a location')
parser.add_argument('distance',type=int,help='distance for the locations with')
parser.add_argument('number_proc',type=int,help='number of process to do the computation')
args=parser.parse_args()
path=args.directory

r=6371
def Haversine(l1,l2,lo1,lo2):
    d=2*r*np.arcsin((np.sin((l2-l1)/2))**2+np.cos(l1)*np.cos(l2)*(np.sin((lo2-lo1)/2))**2)
    return d

Event_ID_Goldstein_Score_Url=[]
empty=[]
Distance=[]


g=glob.glob(os.path.join(path, '*'))
with open('Output/results.csv','w') as h:
	h.write('GLOBALEVENTID'+'\t'+'GoldsteinScale'+'\t'+'SOURCEURL'+'\n')
	h.close()
def Files(g):
    with open('Output/results.csv','a') as h:
        for filename in g:
            column_names=['GLOBALEVENTID','SQLDATE','MonthYear','Year','FractionDate','Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code','Actor2Code','Actor2Name','Actor2CountryCode','Actor2KnownGroupCode','Actor2EthnicCode','Actor2Religion1Code','Actor2Religion2Code','Actor2Type1Code','Actor2Type2Code','Actor2Type3Code','IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone','Actor1Geo_Type','Actor1Geo_FullName','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID','Actor2Geo_Type','Actor2Geo_FullName','Actor2Geo_CountryCode','Actor2Geo_ADM1Code','Actor2Geo_Lat','Actor2Geo_Long','Actor2Geo_FeatureID','ActionGeo_Type','ActionGeo_FullName','ActionGeo_CountryCode','ActionGeo_ADM1Code','ActionGeo_Lat','ActionGeo_Long','ActionGeo_FeatureID','DATEADDED','SOURCEURL']
            # Reading the files one 
            data=pd.read_csv(filename,low_memory=False ,sep="\t",header=None,names = column_names)
            # to use 2013 and after format.
            data1=data[data.Year>=2013] 
            data1 = data1[np.isfinite(data1['ActionGeo_Lat'])]
            data1 = data1[np.isfinite(data1['ActionGeo_Long'])]
            data1['Haversine'] = Haversine(args.latitude,data1['ActionGeo_Lat'],args.longitude,data1['ActionGeo_Long'])
            data2= data1[data1.Haversine<=args.distance]
            data3= data2[['GLOBALEVENTID','GoldsteinScale','SOURCEURL']]
            for i in data2.index.values:
                h.write(str(data2.loc[i,'GLOBALEVENTID'])+'\t'+str(data2.loc[i,'GoldsteinScale'])+'\t'+str(data2.loc[i,'SOURCEURL'])+'\n')
                    
            time.sleep(0.1)
            print(data3.head())
        print('Process with the ID',os.getpid(), 'has done the computation of finding event within specified distance')
        h.close() 
    #return data3.head()
      



if __name__ == '__main__':
    chunks = [g[i::len(g)] for i in range(len(g))]
    p = Pool(args.number_proc)
    
    print(p.map(Files,chunks))    


