import pandas as pd
import psycopg2
import pickle
import numpy as np
from sklearn.cluster import KMeans
from sklearn import metrics
from sklearn.metrics import pairwise_distances
from sklearn import datasets
import glob
from scipy import stats
from sklearn.decomposition import PCA
import gapkmean


# -*- coding: utf-8 -*-
import os
import sys
import copy
import time

# Variation of information (VI)
#
# Meila, M. (2007). Comparing clusterings-an information
#   based distance. Journal of Multivariate Analysis, 98,
#   873-895. doi:10.1016/j.jmva.2006.11.013
#
# https://en.wikipedia.org/wiki/Variation_of_information

from math import log

def variation_of_information(X, Y):

    n = float(sum([len(x) for x in X]))
    sigma = 0.0
    for x in X:
        p = len(x) / n
        for y in Y:
            q = len(y) / n
            r = len(set(x) & set(y)) / n
            if r > 0.0:
                sigma += r * (log(r / p, 2) + log(r / q, 2))
    return abs(sigma)

# VI = 3.322 (maximum VI is log(N) = log(10) = 3.322)




def fileLoader(path, wait):
    time.sleep(int(wait))
    print('now we start')
    allFiles = glob.glob(path + "/WDuserstats_last*")
    # frame = pd.DataFrame()
    list_ = []

    # bots
    bot_list_file = path + '/bot_list.csv'
    bot_list = pd.read_csv(bot_list_file)

    # admin
    admin_list_file = path + '/admin_list.csv'
    admin_list = pd.read_csv(admin_list_file)
    admin_list.start_date = pd.to_datetime(admin_list.start_date)
    admin_list.end_date = pd.to_datetime(admin_list.end_date)

    for file_ in allFiles:
        df = pd.read_csv(file_, index_col=None, header=0)

        list_.append(df)
    frame = pd.concat(list_)
    frame.columns = ['username', 'noEdits', 'noItems', 'noOntoEdits', 'noPropEdits', 'noCommEdits', 'noTaxoEdits',
                     'noBatchEdits', 'minTime', 'timeframe', 'userAge']
    frame = frame.drop(['minTime'], axis=1)

    #read and prepare count of modifying edits
    mod_file = path + '/modified_count.csv'
    frame_mod = pd.read_csv(mod_file)
    frame_mod['timeframe'] = frame_mod['monthinfo'].apply(lambda x: x.replace(' 00:00:00', ''))
    frame_mod.timeframe = pd.to_datetime(frame_mod['timeframe'])
    frame_mod.timeframe = frame_mod.timeframe + pd.DateOffset(months=1)
    frame_mod.timeframe = frame_mod['timeframe'].apply(lambda x: x.strftime('%Y-%m-%d'))
    frame_mod.drop('monthinfo', axis=1, inplace=True)

    frame = frame.merge(frame_mod, how='left', on=['username', 'timeframe'])
    frame['editNorm'] = frame['noEdits']
    frame_anon = frame.loc[frame['username'].str.match(
        r'([0-9]{1,3}[.]){3}[0-9]{1,3}|(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])[.]){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])[.]){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))',
        case=False),]
    frame_bots = frame.loc[frame['username'].isin(bot_list['bot_name']),]

    frame = frame.loc[~frame['username'].isin(bot_list['bot_name']),]

    frame = frame.loc[~frame['username'].str.match(
        r'([0-9]{1,3}[.]){3}[0-9]{1,3}|(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])[.]){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])[.]){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))',
        case=False),]

    frame = frame.loc[~frame['username'].isin(bot_list['bot_name']),]
    frame = frame.set_index('username')
    colN = ['editNorm', 'noCommEdits', 'timeframe']
    normaliser = lambda x: x / x.sum()
    frame_norm = frame[colN].groupby('timeframe').transform(normaliser)
    frame_norm['timeframe'] = frame['timeframe']
    frame_norm['noItems'] = frame['noEdits'] / frame['noItems']
    frame_norm['userAge'] = frame['userAge'] / 360
    frame_norm['noBatchEdits'] = frame['noBatchEdits'] / frame['noEdits']
    frame_norm['noTaxoEdits'] = frame['noTaxoEdits'] / frame['noEdits']
    frame_norm['noOntoEdits'] = frame['noOntoEdits'] / frame['noEdits']
    frame_norm['noPropEdits'] = frame['noPropEdits'] / frame['noEdits']
    frame_norm['noeditsmonthly'] = frame['noeditsmonthly'] / frame['noEdits']
    frame_norm['noEdits'] = frame['noEdits']
    # frame_norm = frame_norm.loc[frame_norm['noEdits'] >= 5,]
    frame_norm.reset_index(inplace=True)
    frame_norm['admin'] = False
    frame_norm['admin'].loc[frame_norm['username'].isin(admin_list['user_name']),] = True

    # frame_norm.drop('noEdits', axis=1, inplace=True)

    # frame_norm = frame_norm.set_index('username')

    # zscore = lambda x: (x - x.mean()) / x.std()

    # colZ = ['noEdits', 'noOntoEdits', 'noPropEdits', 'noCommEdits', 'userAge',  'timeframe']
    # frame_norm = frame[colZ].groupby('timeframe').transform(zscore)
    frame_norm = frame_norm.loc[frame_norm['timeframe'] > '2013-02-01',]
    frame_clean = frame_norm[frame_norm.notnull()]
    frame_clean = frame_clean.replace([np.inf, -np.inf], np.nan)
    frame_clean = frame_clean.fillna(0)
    frame_clean['serial'] = range(1, len(frame_clean) + 1)
    # frame_clean.set_index('timeframe', inplace=True)
    # frame_clean.index = frame_clean['serial']
    colDropped = ['noEdits', 'serial', 'username', 'timeframe', 'userAge']
    print('dataset loaded')

    resultsKmeans = {}

    for n in range(2,9):
        label_array = []
        resultsAll = []
        for num in range(1, 10):
            labelSample = []
            frame_sample = frame_clean.sample(frac=0.8)
            kmeans = KMeans(n_clusters=n, n_init=10, n_jobs=-1).fit(frame_sample.drop(colDropped, axis=1))
            labels = kmeans.labels_
            frame_sample['labels'] = labels
            # frameTest = np.array(frame_sample.loc[frame_sample['labels'] == 0,]['noEdits'],
            #                       frame_sample.loc[frame_sample['labels'] == 1,]['noEdits'])
            # F, p = stats.f_oneway(frame_sample.loc[frame_sample['labels'] == 0,]['noBatchEdits'],
            #                       frame_sample.loc[frame_sample['labels'] == 1,]['noBatchEdits'], frame_sample.loc[frame_sample['labels'] == 2,]['noBatchEdits'],
            #                       frame_sample.loc[frame_sample['labels'] == 3,]['noBatchEdits'])
            for g in range(0, n):
                listSerials= frame_sample['serial'].loc[frame_sample['labels'] == g]
                labelSample.append(list(listSerials))
            label_array.append(labelSample)
        for i in label_array:
            for j in label_array:
                IV = variation_of_information(i, j)
                resultsAll.append(IV)
        resultsKmeans[str(n)] = resultsAll

    kAvg = {}
    for key in resultsKmeans:
        listres = resultsKmeans[key]
        res = np.mean(listres)
        rstd = np.std(listres)
        kAvg[key] = (res, rstd)

    print('VI computed')

    with open('kmeansAvg.txt', 'w') as f:
        f.write(str(kAvg))
        f.close()

    resultSscore ={}
    for n in range(2, 9):
        resultsAll = []
        for num in range(1, 15):
            labelSample = []
            kmeans = KMeans(n_clusters=n, n_init=10, n_jobs=-1).fit(frame_clean.drop(colDropped, axis=1))
            labels = kmeans.labels_
            try:
                sscore = metrics.silhouette_score(frame_clean.drop(colDropped, axis=1), labels, sample_size=20000, metric='euclidean')
            except ValueError:
                sscore = 'NA'
        # print(n, sscore)
            resultsAll.append(sscore)
        resultSscore[str(n)] = resultsAll

    with open('kmeansscore.txt', 'w') as f:
        f.write(str(resultSscore))
        f.close()

    print('sscore done')

    X = np.array(frame_clean.drop(colDropped, axis=1))
    gaps, s_k, K = gapkmean.gap_statistic(X, refs=None, B=150, K=range(2, 9), N_init=10)
    bestKValue, things = gapkmean.find_optimal_k(gaps, s_k, K)

    with open('allResults.txt', 'w') as f:
        f.write('VI scores')
        f.write('\n')
        for key in resultsKmeans:
            listres = resultsKmeans[key]
            f.write('{'+str(key) + ':'+str(listres)+'},')
        f.write('\n')
        f.write('silhouette scores')
        f.write('\n')
        f.write(str(resultSscore))
        f.write('\n')
        f.write('gaps: ' + str(gaps))
        f.write('\n')
        f.write(str(s_k))
        f.write('\n')
        f.write('best K: ' + str(bestKValue))
        f.write('\n')
        f.write(str(things))
        f.close()
    print('gap done')



# pca = PCA(n_components=2)
# pca.fit(frame_clean.drop('serial'))
# frame_pca = pca.fit_transform(frame_clean.drop('serial'))
# kmeans = KMeans(n_clusters=n, n_init=10, n_jobs=-1).fit(frame_pca)
# print(pca.explained_variance_ratio_)

def main():
    # create_table()
    # path = '/Users/alessandro/Documents/PhD/userstats'
    path = sys.argv[1]
    waitTime = sys.argv[2]
    fileLoader(path, waitTime)


if __name__ == "__main__":
    main()
