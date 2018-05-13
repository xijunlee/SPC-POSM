from sklearn import ensemble
import numpy as np
from random import randint

from Customer import Customer
from Provider import Provider
from ProviderPlus import ProviderPlus
from PO import PO
from SwapChainSolver import SwapChainSolver

import copy

from sklearn.metrics import mean_squared_error
import math
class Surrogate:
    def __init__(self, dataPool):
        self.m_X = dataPool['X']
        self.m_Y = dataPool['Y']
        #self.m_SampleSize = sampleSize
        #self.m_Data = data
        self.m_Params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2,
                 'learning_rate': 0.01, 'loss': 'ls'}
        self.m_Regressor = ensemble.GradientBoostingRegressor()

    
    def calcMMD(self, geneSerial, data):
        customers = []
        for item in data.CUSTOMERS:
            tmp = Customer()
            tmp.x = copy.deepcopy(item.x)
            tmp.y = copy.deepcopy(item.y)
            tmp.demand = copy.deepcopy(item.demand)
            customers.append(tmp)
        providers = []
        sigmaCost = 0
        sigmaCapacity = 0
        sigmaDemand = 0
        mmd = -1000.00
        for i in range(0, len(geneSerial)):
            tmpProvider = Provider()
            tmpProvider.x = copy.deepcopy(data.PROVIDERS[i].x)
            tmpProvider.y = copy.deepcopy(data.PROVIDERS[i].y)
            tmpProvider.capacity = copy.deepcopy(data.PROVIDERS[i].capacity[geneSerial[i]])
            tmpProvider.cost = copy.deepcopy(data.PROVIDERS[i].cost[geneSerial[i]])
            sigmaCost = sigmaCost + tmpProvider.cost
            sigmaCapacity = sigmaCapacity + tmpProvider.capacity
            providers.append(tmpProvider)
        for item in customers:
            sigmaDemand = sigmaDemand + item.demand
        if sigmaCapacity >= sigmaDemand:
            swapchainsolver = SwapChainSolver(providers, customers)
            mmd = swapchainsolver.Solver()
        return mmd

    '''
    def genenrateData(self):
        print "generating surrogate model data ..."
        for _ in range(self.m_SampleSize):
            x = []
            for j in range(len(self.m_Data.PROVIDERS)):
                x.append(randint(0, self.m_Data.PROVIDERS[j].cnt - 1))
            # for test,will be deleted in real environment
            y = self.calcMMD(x, self.m_Data)
            self.m_X.append(x)
            self.m_Y.append(y)
    '''

    def trainModel(self):
            #self.genenrateData()
            X = np.array(self.m_X)
            Y = np.array(self.m_Y)
            offset = int(X.shape[0]*0.9)
            X_train, y_train = X[:offset,], Y[:offset]
            X_test, y_test = X[offset:,], Y[offset:]
            self.m_Regressor.fit(X_train, y_train)
            mse = mean_squared_error(y_test, self.m_Regressor.predict(X_test))
            rmse = math.pow(mse,0.5)
            print("RMSE: %.4f" % rmse)

    def predict(self, x):
         return self.m_Regressor.predict(x)