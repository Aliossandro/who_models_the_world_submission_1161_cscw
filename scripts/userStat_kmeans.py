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
from sklearn.cluster import AgglomerativeClustering
from math import log


def fileLoader(path):

    frame_clean = pd.read_csv(path)
    colDropped = ['serial', 'username', 'timeframe', 'noEdits']
    print('dataset loaded')   

    X = np.array(frame_clean.drop(colDropped, axis=1))
    gaps, s_k, K = gapkmean.gap_statistic(X, refs=None, B=150, K=range(2, 9), N_init=10)
    bestKValue, things = gapkmean.find_optimal_k(gaps, s_k, K)

    with open('allResults_new.txt', 'w') as f:        
        f.write('gaps: ' + str(gaps))
        f.write('\n')
        f.write(str(s_k))
        f.write('\n')
        f.write('best K: ' + str(bestKValue))
        f.write('\n')
        f.write(str(things))
        f.close()
    print('gap done')


def main():
    path = sys.argv[1]
    fileLoader(path)


if __name__ == "__main__":
    main()
