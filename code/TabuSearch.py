import math
import random
from random import randint
import copy
from sklearn.metrics import mean_squared_error
import numpy as np
from pyspark import SparkContext, SparkConf
import time
import pandas as pd
import sys


class Chromosome:
    def __init__(self):
        self.geneSerial = []
        self.v = []
        self.fitness = 0
        self.sigmaCost = 0
        self.sigmaDemand = 0
        self.sigmaCapacity = 0
        self.mmd = 0
        self.pbest = None


class Customer:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.demand = 0


class Provider:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.capacity = 0
        self.cost = 0


class ProviderPlus:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.cnt = 0
        self.capacity = []
        self.cost = []


class PO:
    def __init__(self):
        self.PROVIDERS = []
        self.CUSTOMERS = []


class Match:
    def __init__(self):
        self.o = 0
        self.p = 0
        self.w = 0
        self.dis = 0


class Queue:
    def __init__(self):
        self.num = 0
        self.parent = 0


class SwapChainSolver:

    def __init__(self, providers, customers):
        self.P = providers
        self.O = customers
        self.Assignment = []

    def Solver(self):

        self.initiallize_assignment()
        while True:
            extremeMatch = copy.deepcopy(self.find_d_satisfiable())

            if not extremeMatch:
                break
            else:
                self.swap(extremeMatch)

        self.Assignment = sorted(self.Assignment, key=self.returnDis)
        return self.Assignment[len(self.Assignment) - 1].dis

    def swap(self, m):
        self.sub_match(m)
        chain = []
        while True:
            chain = self.find_chain(m)
            if not chain:
                break
            else:
                # chain breaking
                ws = float('inf')
                ws = min(ws, self.P[chain[0] - len(self.O)].capacity)
                ws = min(ws, self.O[chain[len(chain) - 1]].demand)

                for i in range(1, len(chain) - 1, 2):
                    # if i%2 == 1:
                    tmpo = chain[i]
                    tmpp = chain[i + 1] - len(self.O)
                    for tmp in self.Assignment:
                        if tmp.o == tmpo and tmp.p == tmpp:
                            ws = min(ws, tmp.w)
                            break
                for i in range(1, len(chain) - 1, 2):
                    # if i%2 == 1:
                    tmpo = chain[i]
                    tmpp = chain[i + 1] - len(self.O)
                    for tmp in self.Assignment:
                        if tmp.o == tmpo and tmp.p == tmpp:
                            tmpm = copy.deepcopy(tmp)
                            self.sub_match(tmp)
                            if tmpm.w != ws:
                                tmpm.w = tmpm.w - ws
                                self.add_match(tmpm)
                            break
                # chain matching
                for i in range(0, len(chain), 2):
                    tmpo = chain[i + 1]
                    tmpp = chain[i] - len(self.O)
                    tmpm = Match()
                    tmpm.o = tmpo
                    tmpm.p = tmpp
                    tmpm.w = ws
                    tmpm.dis = math.sqrt(
                        (self.O[tmpo].x - self.P[tmpp].x) ** 2 + (self.O[tmpo].y - self.P[tmpp].y) ** 2)
                    self.add_match(tmpm)

                if self.O[m.o].demand == 0:
                    break

        # post matching
        if self.O[m.o].demand > 0:
            tmpm = Match()
            tmpm.o = m.o
            tmpm.p = m.p
            tmpm.w = self.O[m.o].demand
            tmpm.dis = math.sqrt((self.O[m.o].x - self.P[m.p].x) ** 2 + (self.O[m.o].y - self.P[m.p].y) ** 2)
            self.add_match(tmpm)

    def find_chain(self, m):
        chain = []
        flag = False
        maxDis = m.dis
        Q = []
        hash = []
        for i in range(0, 2 * (len(self.O) + len(self.P))):
            Q.append(Queue())
            hash.append(0)
        head = 0
        tail = 0
        hash[m.o] = 1
        Q[head].num = m.o
        Q[head].parent = -1
        tail = tail + 1

        while not flag and head != tail:
            CurrentNode = Q[head].num
            if CurrentNode < len(self.O):
                for i in range(0, len(self.P)):
                    tmpDis = math.sqrt(
                        (self.O[CurrentNode].x - self.P[i].x) ** 2 + (self.O[CurrentNode].y - self.P[i].y) ** 2)
                    if tmpDis < maxDis and hash[i + len(self.O)] == 0:
                        Q[tail].num = i + len(self.O)
                        Q[tail].parent = head
                        hash[i + len(self.O)] = 1
                        tail = (tail + 1) % len(Q)
            else:
                pNode = CurrentNode - len(self.O)
                if self.P[pNode].capacity == 0:
                    for tmp in self.Assignment:
                        if tmp.p == pNode and hash[tmp.o] == 0:
                            hash[tmp.o] = 1
                            Q[tail].num = tmp.o
                            Q[tail].parent = head
                            tail = (tail + 1) % len(Q)
                else:
                    flag = True
                    tmp = head
                    while tmp >= 0:
                        chain.append(Q[tmp].num)
                        tmp = Q[tmp].parent
            head = (head + 1) % len(Q)

        if flag:
            return chain
        else:
            return flag

    def find_d_satisfiable(self):
        hash = []
        myQueue = []
        haveFound = False
        for i in range(0, len(self.O) + len(self.P)):
            hash.append(0)
        for i in range(0, 2 * (len(self.O) + len(self.P))):
            myQueue.append(Queue())

        self.Assignment = sorted(self.Assignment, key=self.returnDis)
        maxDis = self.Assignment[len(self.Assignment) - 1].dis

        k = len(self.Assignment) - 1
        extremeMatch = False
        while not haveFound and self.Assignment[k].dis == maxDis and k >= 0:
            for tmp in hash:
                tmp = 0
            for tmp in myQueue:
                tmp.num = 0
                tmp.parent = 0

            head = 0
            tail = 0

            hash[self.Assignment[k].o] = 1
            myQueue[head].num = self.Assignment[k].o
            myQueue[head].parent = -1
            tail += 1

            extremeMatch = copy.deepcopy(self.Assignment[k])
            self.sub_match(extremeMatch)

            while head != tail and not haveFound:
                CurrentNode = myQueue[head].num

                if CurrentNode < len(self.O):
                    for i in range(0, len(self.P)):
                        tmpDis = math.sqrt(
                            (self.O[CurrentNode].x - self.P[i].x) ** 2 + (self.O[CurrentNode].y - self.P[i].y) ** 2)
                        if tmpDis < maxDis and hash[i + len(self.O)] == 0:
                            myQueue[tail].num = i + len(self.O)
                            myQueue[tail].parent = head
                            hash[i + len(self.O)] = 1
                            tail = (tail + 1) % len(myQueue)
                else:
                    pNode = CurrentNode - len(self.O)
                    if self.P[pNode].capacity == 0:
                        for tmp in self.Assignment:
                            if tmp.p == pNode and hash[tmp.o] == 0:
                                hash[tmp.o] = 1
                                myQueue[tail].num = tmp.o
                                myQueue[tail].parent = head
                                tail = (tail + 1) % len(myQueue)
                    else:
                        haveFound = True
                head = (head + 1) % len(myQueue)

            self.add_match(extremeMatch)
            k = k - 1

        if haveFound:
            return extremeMatch
        else:
            return False

    def distance(self, s):
        return s['distance']

    def returnDis(self, s):
        return s.dis

    def add_match(self, m):

        flag = False

        for tmp in self.Assignment:
            if (m.o == tmp.o and m.p == tmp.p):
                tmp.w += m.w
                flag = True
                break

        if flag == False:
            self.Assignment.append(copy.deepcopy(m))

        self.P[m.p].capacity -= m.w
        self.O[m.o].demand -= m.w

    def sub_match(self, m):
        self.P[m.p].capacity += m.w
        self.O[m.o].demand += m.w

        for tmp in self.Assignment:
            if m.o == tmp.o and m.p == tmp.p:
                tmp.w -= m.w
                if tmp.w == 0:
                    self.Assignment.remove(tmp)
                break

    def initiallize_assignment(self):

        distanceList = []
        for i in range(0, len(self.O)):
            distanceList = []
            for j in range(0, len(self.P)):
                dis = math.sqrt((self.O[i].x - self.P[j].x) ** 2 + (self.O[i].y - self.P[j].y) ** 2)
                tmp = {'p': j, 'distance': dis}
                distanceList.append(tmp)

            distanceList = sorted(distanceList, key=self.distance)

            for j in range(0, len(self.P)):
                tmp = min(self.O[i].demand, self.P[distanceList[j]['p']].capacity)
                if (tmp > 0):
                    m = Match()
                    m.o = i
                    m.p = distanceList[j]['p']
                    m.w = tmp
                    m.dis = distanceList[j]['distance']
                    self.add_match(m)
                if self.O[i].demand == 0:
                    break

        self.Assignment = sorted(self.Assignment, key=self.returnDis)
        # print for debug
        '''for i in range(0,len(self.Assignment)):
            print(self.Assignment[i].o, self.Assignment[i].p, self.Assignment[i].w, self.Assignment[i].dis)
        '''


class Surrogate:
    def __init__(self, dataPool):
        self.m_X = dataPool['X']
        self.m_Y = dataPool['Y']
        # self.m_SampleSize = sampleSize
        # self.m_Data = data
        self.m_Params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2,
                         'learning_rate': 0.01, 'loss': 'ls'}
        self.m_Regressor = ensemble.GradientBoostingRegressor()

    '''
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
        # self.genenrateData()
        X = np.array(self.m_X)
        Y = np.array(self.m_Y)
        offset = int(X.shape[0] * 0.9)
        X_train, y_train = X[:offset, ], Y[:offset]
        X_test, y_test = X[offset:, ], Y[offset:]
        self.m_Regressor.fit(X_train, y_train)
        mse = mean_squared_error(y_test, self.m_Regressor.predict(X_test))
        # rmse = math.pow(mse, 0.5)
        print("MSE: %.4f" % mse)

    def predict(self, x):
        return self.m_Regressor.predict(x)


def LoadDataFromText(txtpath):
    """
        load data from text,return PROVIDERS,CUSTOMERS
    """
    fp = open(txtpath, "r")
    arr = []
    for line in fp.readlines():
        arr.append(line.replace("\n", "").split(" "))
    fp.close()
    NumberOfProviders = int(arr[0][0])
    PROVIDERS = []
    for i in range(1, NumberOfProviders + 1):
        tmp = arr[i]
        tmpProvider = ProviderPlus()
        tmpProvider.x = float(tmp[0])
        tmpProvider.y = float(tmp[1])
        tmpProvider.cnt = int(tmp[2])
        for j in range(0, tmpProvider.cnt):
            tmpProvider.capacity.append(float(tmp[j + 3]))
            tmpProvider.cost.append(float(tmp[j + 3 + tmpProvider.cnt]))
        PROVIDERS.append(tmpProvider)
    NumberOfCustomers = int(arr[NumberOfProviders + 1][0])
    CUSTOMERS = []
    for i in range(0, NumberOfCustomers):
        tmp = arr[i + NumberOfProviders + 2]
        tmpCustomer = Customer()
        tmpCustomer.x = float(tmp[0])
        tmpCustomer.y = float(tmp[1])
        tmpCustomer.demand = float(tmp[2])
        CUSTOMERS.append(tmpCustomer)
    return PROVIDERS, CUSTOMERS

class TabuSearch:

    def __init__(self, tabuMaxLength, maxIter, maxNumCandidate, surrogateFlag, po, D, alpha, beta, blockMax):
        self.m_TabuList = []
        self.m_CandidateList = []
        self.m_TabuMaxLength = tabuMaxLength
        self.m_MaxIter = maxIter
        self.m_MaxNumCandidate = maxNumCandidate
        self.m_SurrogateFlag = surrogateFlag
        self.m_PO = po
        self.m_D = D
        self.m_Alpha = alpha
        self.m_Beta = beta
        self.m_CurrentSolution = self.generateRandomChromosome()
        self.m_BestSolution = self.m_CurrentSolution
        self.m_BlockMax = blockMax
        self.m_Block = 0
        self.m_Runtime = 0

    def calcFitnessParallel(self, geneSerial, data, D):

        # alpha and beta are weight factor
        alpha = self.m_Alpha
        beta = self.m_Beta
        customers = []
        fitness = 0
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
        mmd = self.m_D * 1000.0
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
            if mmd > D:
                fitness = -4.0
            else:
                if sigmaCost != 0:
                    fitness = float(4.0 / sigmaCost)
                else:
                    fitness = 8.0
        else:
            fitness = -8.0
        # print("fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand:",fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand)
        # return math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand

        return (geneSerial, math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand)

    def evaluate(self, sc):
        startTime = time.time()
        iter = 0
        while iter < self.m_MaxIter:# and self.m_Block < self.m_BlockMax:
            # randomly decide the transformation method, 0 for swap, 1 for add(reduce) 1
            self.m_CandidateList = []
            raw_data = []
            for _ in range(self.m_MaxNumCandidate):
                flag = randint(0, 1)
                geneSerial = self.m_CurrentSolution.geneSerial
                if flag == 0:
                    pointA = randint(0, len(self.m_CurrentSolution.geneSerial) - 1)
                    pointB = randint(0, len(self.m_CurrentSolution.geneSerial) - 1)
                    tmp = geneSerial[pointA]
                    geneSerial[pointA] = geneSerial[pointB]
                    geneSerial[pointB] = tmp
                else:
                    pointA = -1
                    pointB = randint(0, len(self.m_CurrentSolution.geneSerial) - 1)
                    geneSerial[pointB] = (geneSerial[pointB] + 1) % self.m_PO.PROVIDERS[
                        pointB].cnt
                if (flag, pointA, pointB) not in set(self.m_TabuList):
                    raw_data.append(geneSerial)

            # parallelly compute the fitness for each individual
            distPop = sc.parallelize(raw_data)
            fitnessCalc = distPop.map(
            lambda geneSerial: self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO),
                                                                    copy.copy(self.m_D)))
            chromosomeCollect = fitnessCalc.collect()

            for (geneSerial, fitness, mmd, sigmaCapacity, sigmaCost, sigmaDemand) in chromosomeCollect:
                chromosome = Chromosome()
                chromosome.geneSerial = geneSerial
                chromosome.fitness = fitness
                chromosome.mmd = mmd
                chromosome.sigmaCapacity = sigmaCapacity
                chromosome.sigmaCost = sigmaCost
                chromosome.sigmaDemand = sigmaDemand
                self.m_CandidateList.append((chromosome, chromosome.fitness, (flag, pointA, pointB)))

            nextBestChromosome, nextBestFitness, tabu = sorted(self.m_CandidateList, key=lambda x: x[1], reverse=True)[
                0]

            if self.m_BestSolution.fitness <= nextBestFitness:
                self.m_BestSolution = copy.deepcopy(nextBestChromosome)
                self.m_Block = 0
            elif math.fabs(self.m_BestSolution.fitness - nextBestFitness) <= 0.001:
                self.m_Block += 1

            if len(self.m_TabuList) >= self.m_TabuMaxLength:
                self.m_TabuList.pop(0)
            self.m_TabuList.append(tabu)
            self.m_CurrentSolution = nextBestChromosome
            #print iter, "th iteration"
            #print "the current individual serial, fitness, mmd, sigmaCost, sigmaCapacity, sigmaDemand ", \
            #    self.m_CurrentSolution.geneSerial, self.m_CurrentSolution.fitness, self.m_CurrentSolution.mmd, self.m_CurrentSolution.sigmaCost, self.m_CurrentSolution.sigmaCapacity, self.m_CurrentSolution.sigmaDemand
            #print self.m_CurrentSolution.sigmaCost
            iter += 1
            endTime = time.time()
            self.m_Runtime = endTime-startTime

    def calcFitness(self, geneSerial, data, D):
        """
            usage ChromosomeNumber,geneSerial,data,D
            return fitness for this1  Chromosome
        """
        alpha = self.m_Alpha
        beta = self.m_Beta
        # alpha and beta are weight factor
        customers = []
        fitness = 0
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
        mmd = self.m_D * 1000.0
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
            if mmd > D:
                fitness = -4.0
            else:
                if sigmaCost != 0:
                    fitness = float(4.0 / sigmaCost)
                else:
                    fitness = 8.0
        else:
            fitness = -8.0
        # print("fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand:",fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand)
        return math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand

    def calcFitnessWithSurrogate(self, geneSerial, data, D):
        """
            usage ChromosomeNumber,geneSerial,data,D
            return fitness for this1  Chromosome
        """
        alpha = self.m_Alpha
        beta = self.m_Beta
        # alpha and beta are weight factor
        customers = []
        fitness = 0
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
        mmd = self.m_D * 1000.0
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
            x = np.array(geneSerial).reshape(1, -1)
            mmd = self.m_Surrogate.predict(x)[0]
            if mmd > D:
                fitness = -1000
            elif mmd > 0:
                if sigmaCost != 0:
                    fitness = float(4.0 / sigmaCost)
                else:
                    fitness = 8.0
            else:
                fitness = -6.0
        else:
            fitness = -8.0
        # print"fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand:",fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand
        return math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand

    def generateRandomChromosome(self):
        chromosome = Chromosome()
        for i in range(len(self.m_PO.PROVIDERS)):
            chromosome.geneSerial.append(randint(0, self.m_PO.PROVIDERS[i].cnt - 1))
        if self.m_SurrogateFlag:
            chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitnessWithSurrogate(
                chromosome.geneSerial, self.m_PO, self.m_D)
        else:
            chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitness(
                chromosome.geneSerial, self.m_PO, self.m_D)
        return chromosome


if __name__ == "__main__":

    tabuMaxLength = 10
    maxIter = 100
    maxNumCandidate = 100
    surrogateFlag = False
    D = 40.0
    alpha = 10000000.00
    beta = 0.01
    blockMax = 3
    surrogateFlag = False
    core_num = int(sys.argv[1])

    conf = SparkConf().setMaster("spark://noah007:7077") \
        .setAppName("SPC-POSM-TS") \
        .set("spark.submit.deployMode", "client") \
        .set("spark.cores.max", core_num) \
        .set("spark.executor.cores", "10") \
        .set("spark.executor.memory", "10g") \
        .set("spark.driver.memory", "40g")

    sc = SparkContext(conf=conf)
    aveAns, aveRuntime = [], []

    for i in range(60):
        print i, 'th instance ...'
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        sumAns, sumRuntime = 0, 0
        times = 5
        for _ in range(times):
            tabuSearch = TabuSearch(tabuMaxLength, maxIter, maxNumCandidate, surrogateFlag, po, D, alpha, beta,blockMax)
            tabuSearch.evaluate(sc)
            sumAns += tabuSearch.m_BestSolution.sigmaCost
            sumRuntime += tabuSearch.m_Runtime

        aveAns.append(sumAns / (times * 1.0))
        aveRuntime.append(sumRuntime / (times * 1.0))

    df = pd.DataFrame({'cost': aveAns, 'TS runtime': aveRuntime})
    df.to_csv('../midResult/tsResult.csv')
