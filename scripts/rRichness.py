import pandas as pd
import psycopg2
import pickle
import numpy as np
import json
from collections import defaultdict
# counterS = 0
# global counterS
# global valGlob
# from sqlalchemy import create_engine

# -*- coding: utf-8 -*-
import os
import sys
import copy

def get_db_params():
    params = {
        'database': 'wikidb',
        'user': 'postgres',
        'password': 'postSonny175',
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**params)
    return conn

def queryexecutor():
    dictStats = {}
    conn = get_db_params()

    for i in range(13, 18):
        for j in range(1, 10):
            if i == 13:
                pass
            else:
                date = "20" + str(i) + "-0" + str(j) + "-01"
                print(date)

                dictStats[date] = {}

                queryRich = """WITH selItems AS (SELECT DISTINCT itemId FROM tempData WHERE  tS::timestamp < '""" + date +""" 00:00:00'::timestamp AND statproperty != 'P31' ),
                selClasses AS (SELECT DISTINCT statvalue FROM tempData WHERE  tS::timestamp < '""" + date +""" 00:00:00'::timestamp)
                SELECT itemid, statproperty, statvalue, statementid, revid, timestamp FROM statementDated WHERE  timestamp < '""" + date +""" 00:00:00'::timestamp
                AND (itemid IN (SELECT itemId FROM selItems) OR itemid IN (SELECT statvalue FROM selClasses));"""

                dfRich = pd.DataFrame()
                for chunk in pd.read_sql(queryRich, con=conn, chunksize=25000):
                    dfRich = dfRich.append(chunk)

                if not dfRich.empty:
                    dfRich = dfRich.loc[dfRich['statvalue'] != 'deleted',]
                    dfRich = dfRich.loc[dfRich['statvalue'] != 'novalue',]
                    dfRich = dfRich.loc[dfRich['statvalue'] != 'somevalue',]
                    idx = dfRich.groupby(['statementid'])['revid'].transform(max) == dfRich['revid']
                    dfRichClean = dfRich[idx]
                    richAll = dfRichClean.groupby('statproperty')['statvalue'].nunique()
                    try:
                        dictStats[date]['relRichness'] = (richAll.sum() - np.asscalar(richAll['P279']))/richAll.sum()
                        dictStats[date]['maxRichness'] = np.asscalar(richAll.max())
                        dictStats[date]['avgRichness'] = richAll.mean()
                        dictStats[date]['medianRichness'] = richAll.median()
                        dictStats[date]['quantileRichness'] = [qua for qua in list(richAll.quantile([.25, .5, .75]))]
                    except KeyError:
                        print('no P279')
                        dictStats[date]['relRichness'] = 1
                        dictStats[date]['maxRichness'] = np.asscalar(richAll.max())
                        dictStats[date]['avgRichness'] = richAll.mean()
                        dictStats[date]['medianRichness'] = richAll.median()
                        dictStats[date]['quantileRichness'] = [qua for qua in list(richAll.quantile([.25, .5, .75]))]
                else:
                    dictStats[date]['relRichness'] = 0
                    dictStats[date]['maxRichness'] = 0
                    dictStats[date]['avgRichness'] = 0
                    dictStats[date]['medianRichness'] = 0
                    dictStats[date]['quantileRichness'] = 0

                with open('WDataStats_RR.txt', 'a') as myfile:
                    myfile.write(json.dumps(dictStats))
                    myfile.close()

    for i in range(13, 18):
        for j in range(11, 13):
            date = "20" + str(i) + "-" + str(j) + "-01"
            print(date)

            dictStats[date] = {}

            queryRich = """WITH selItems AS (SELECT DISTINCT itemId FROM tempData WHERE  tS::timestamp < '""" + date +""" 00:00:00'::timestamp AND statproperty != 'P31'),
            selClasses AS (SELECT DISTINCT statvalue FROM tempData WHERE  tS::timestamp < '""" + date +""" 00:00:00'::timestamp)
            SELECT itemid, statproperty, statvalue, statementid, revid, timestamp FROM statementDated WHERE  timestamp < '""" + date +""" 00:00:00'::timestamp
            AND (itemid IN (SELECT itemId FROM selItems) OR itemid IN (SELECT statvalue FROM selClasses));"""

            dfRich = pd.DataFrame()
            for chunk in pd.read_sql(queryRich, con=conn, chunksize=25000):
                dfRich = dfRich.append(chunk)

            if not dfRich.empty:
                dfRich = dfRich.loc[dfRich['statvalue'] != 'deleted',]
                dfRich = dfRich.loc[dfRich['statvalue'] != 'novalue',]
                dfRich = dfRich.loc[dfRich['statvalue'] != 'somevalue',]
                idx = dfRich.groupby(['statementid'])['revid'].transform(max) == dfRich['revid']
                dfRichClean = dfRich[idx]
                richAll = dfRichClean.groupby('statproperty')['statvalue'].nunique()

                try:
                    dictStats[date]['relRichness'] = (richAll.sum() - np.asscalar(richAll['P279']))/richAll.sum()
                    dictStats[date]['maxRichness'] = np.asscalar(richAll.max())
                    dictStats[date]['avgRichness'] = richAll.mean()
                    dictStats[date]['medianRichness'] = richAll.median()
                    dictStats[date]['quantileRichness'] = [qua for qua in list(richAll.quantile([.25, .5, .75]))]
                except KeyError:
                    print('no P279')
                    dictStats[date]['relRichness'] = 1
                    dictStats[date]['maxRichness'] = np.asscalar(richAll.max())
                    dictStats[date]['avgRichness'] = richAll.mean()
                    dictStats[date]['medianRichness'] = richAll.median()
                    dictStats[date]['quantileRichness'] = [qua for qua in list(richAll.quantile([.25, .5, .75]))]
            else:
                dictStats[date]['relRichness'] = 0
                dictStats[date]['maxRichness'] = 0
                dictStats[date]['avgRichness'] = 0
                dictStats[date]['medianRichness'] = 0
                dictStats[date]['quantileRichness'] = 0

            with open('WDataStats_RR.txt', 'a') as myfile:
                myfile.write(json.dumps(dictStats))
                myfile.close()


def main():
    # create_table()
    queryexecutor()


if __name__ == "__main__":
    main()
