import pandas as pd
import psycopg2
import pickle
import numpy as np
# counterS = 0
# global counterS
# global valGlob
# from sqlalchemy import create_engine

# -*- coding: utf-8 -*-
import os
import sys
import copy

# fileName = '/Users/alessandro/Documents/PhD/OntoHistory/WDTaxo_October2014.csv'

# connection parameters
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



# create table
def create_table():
    ###statement table query
    query_table = """CREATE TABLE IF NOT EXISTS tempData AS (SELECT p.itemId, p.revId, (p.timestamp::timestamp) AS tS, t.statementId, t.statProperty, t.statvalue FROM
(SELECT itemId, revId, timestamp FROM revisionData_201710) p, (SELECT revId, statementId, statProperty, statvalue FROM statementsData_201710 WHERE statProperty = 'P279' OR statProperty = 'P31') t
WHERE p.revId = t.revId)"""

    queryStatData = """CREATE TABLE IF NOT EXISTS statementDated AS (SELECT p.itemid, p.statproperty, p.statvalue, p.statementid, p.revid, t.timestamp, t.username
    FROM statementsData_201710 p LEFT JOIN revisionData_201710 t ON p.revid::int = t.revid::int);"""


    conn = None

    try:
        conn = get_db_params()
        cur = conn.cursor()
        cur.execute(query_table)
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    conn = None

    try:
        conn = get_db_params()
        cur = conn.cursor()
        cur.execute(queryStatData)
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def queryexecutor():
    # dictStats = {}
    # conn = get_db_params()
    # cur = conn.cursor()
    npCoso = np.load('/data/wikidata-project/WDOntoHistory/automated_revs.npy')
    setCoso = set(npCoso)

    for i in range(13, 18):
        for j in range(1, 7):
            date = "20" + str(i) + "-0" + str(j) + "-01"
            if j == 1:
                yr = i-1
                datePrev = "20" + str(yr) + "-12-01"
            else:
                datePrev = "20" + str(i) + "-0" + str(j-1) + "-01"

            print(date)

            try:

                queryStart = """SELECT item_id AS itemid, rev_id AS revid, time_stamp AS timestamp, user_name AS username, automated_tool FROM revision_history_tagged WHERE (time_stamp > '"""+ datePrev + """ 00:00:00' AND  time_stamp < '"""+ date + """ 00:00:00');"""

                conn = get_db_params()
                cur = conn.cursor()
                cur.execute(queryStart)
                cur.close()
                conn.commit()

                # print(query)
                timetable_temp = pd.DataFrame()
                for chunk in pd.read_sql(queryStart, con=conn, chunksize=10000):
                    timetable_temp = timetable_temp.append(chunk)
                #columns:  itemid      revid      parid           timestamp     username


                noEdits = timetable_temp['username'].value_counts()
                noEdits = noEdits.reset_index()
                noEdits.columns = ['username', 'noEdits']
                noItems = timetable_temp.groupby('username')['itemid'].nunique()
                noItems = noItems.reset_index()
                noItems.columns = ['username', 'noItems']
                noEdits = noEdits.merge(noItems, how='left')

                timetable_temp.loc[timetable_temp['rev_id'].isin(setCoso),] = 'TRUE'
                noBatchEdits = timetable_temp['username'].loc[timetable_temp['automated_tool'] == 'TRUE', ].value_counts()
                if ~noBatchEdits.empty:
                    noBatchEdits = noBatchEdits.reset_index()
                    noBatchEdits.columns = ['username', 'noBatchEdits']
                    noEdits = noEdits.merge(noBatchEdits, how='left')
                else:
                    noEdits['noBatchEdits'] = 0

                print('batch edits')

                classesDataQuery = """SELECT statvalue FROM tempData WHERE ts < '"""+ date + """ 00:00:00';"""

                dfClasses = pd.read_sql(classesDataQuery, con=conn)

                classesDataQuery_b = """SELECT itemId FROM tempData WHERE statproperty != 'P31' AND ts < '"""+ date + """ 00:00:00';"""
                dfClasses_b = pd.read_sql(classesDataQuery_b, con=conn)

                dfClasses = list(dfClasses['statvalue']) + list(dfClasses_b['itemid'])
                dfClasses = list(set(dfClasses))

                ###add ontoedits
                noOntoEdits = timetable_temp.loc[timetable_temp['itemid'].isin(dfClasses)]['username'].value_counts()
                noOntoEdits = noOntoEdits.reset_index()
                noOntoEdits.columns = ['username', 'noOntoEdits']

                noEdits = noEdits.merge(noOntoEdits, how='left')

                ###add property edits
                noPropEdits = timetable_temp.loc[timetable_temp['itemid'].str.match('[pP][0-9]{1,}')]['username'].value_counts()
                noPropEdits = noPropEdits.reset_index()
                noPropEdits.columns = ['username', 'noPropEdits']

                noEdits = noEdits.merge(noPropEdits, how='left')
                print('propedits added')

                ###add community edits
                commQuery = """SELECT * FROM revision_pages_201710 WHERE (time_stamp > '"""+ datePrev + """ 00:00:00' AND  time_stamp < '"""+ date + """ 00:00:00') AND item_id !~* 'Property:P*';"""

                dfComm = pd.read_sql(commQuery, con=conn)
                if len(dfComm.index) != 0:
                    noCommEdits = dfComm['user_name'].value_counts()
                    noCommEdits  = noCommEdits.reset_index()
                    noCommEdits.columns = ['username', 'noCommEdits']
                    noEdits = noEdits.merge(noCommEdits, how='left')
                else:
                    noEdits['noCommEdits'] = 0
                print('comms added')

                taxoQuery = """SELECT username, statproperty, timestamp FROM statementDated WHERE (timestamp > '"""+ datePrev + """ 00:00:00' AND  timestamp < '"""+ date + """ 00:00:00')
                AND (statProperty = 'P31' or statProperty = 'P279');"""

                # dfTaxo = pd.DataFrame()
                # for chunk in pd.read_sql(taxoQuery, con=conn, chunksize=10000):
                #     dfTaxo = dfTaxo.append(chunk)
                dfTaxo = pd.read_sql(taxoQuery, con=conn)
                if len(dfTaxo.index) != 0:
                    noTaxoEdits = dfTaxo['username'].value_counts()
                    noTaxoEdits = noTaxoEdits.reset_index()
                    noTaxoEdits.columns = ['username', 'noTaxoEdits']
                    noEdits = noEdits.merge(noTaxoEdits, how='left')
                else:
                    noEdits['noTaxoEdits'] = 0
                print('taxo added')

                ###age of user
                ageQuery = """SELECT * FROM user_first_edit;"""
                dfAge = pd.read_sql(ageQuery, con=conn)
                dfAge.columns = ['username', 'minTime']
                dfAge['minTime'] = pd.to_datetime(dfAge['minTime'])
                noEdits = noEdits.merge(dfAge, how="left", validate="many_to_one", left_on='username', right_on='username')

                noEdits['timeframe'] = date
                noEdits['timeframe'] = pd.to_datetime(noEdits['timeframe'])
                noEdits['userAge'] = (noEdits['timeframe']-noEdits['minTime']).astype('timedelta64[h]')

                noEdits.fillna(0, inplace=True)
                fileName = "WDuserstats_new-" + date + ".csv"
                noEdits.to_csv(fileName, index=False)

            except Exception as e:
                print(e, "no df available")


    # try:
    #     pickle_out = open("WDdata.pickle", "wb")
    #     pickle.dump(dictStats, pickle_out)
    #     pickle_out.close()
    # except:
    #     print("suca")

def main():
    # create_table()
    queryexecutor()


if __name__ == "__main__":
    main()
