# -*- coding: utf-8 -*-
"""
===================================
Demo of DBSCAN clustering algorithm
===================================

Finds core samples of high density and expands clusters from them.

"""
print(__doc__)

import numpy as np
import sys

from sklearn import cluster, datasets
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler

datasetSize = int( sys.argv[1] )
outputfile = sys.argv[2]

##############################################################################
# Generate sample data

n_samples = datasetSize

noisy_moons = datasets.make_blobs(n_samples=n_samples, centers=10)

X, y = noisy_moons

f1=open(outputfile, 'w+')
i=0
for point in X:
	f1.write(`i`+"\t"+`point[0]`+"\t"+`point[1]`+"\n")
	i = i + 1
