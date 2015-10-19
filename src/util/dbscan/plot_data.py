print(__doc__)

import numpy as np
import sys


inputfile = sys.argv[1]

import matplotlib.pyplot as plt
import csv

data = np.genfromtxt(inputfile,delimiter="\t")

data[:,1] = [x*10000 for x in data[:,1]] 
#colormap = plt.get_cmap('jet')(data[:,1])
plt.scatter(data[:,2], data[:,3], s=10, c=data[:,1], edgecolors='none', cmap=plt.get_cmap('rainbow'))
#plt.scatter(data[:,2], data[:,3], s=1, color=colormap)
plt.show()

