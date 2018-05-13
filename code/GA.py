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
        self.cluster = None

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
        #rmse = math.pow(mse, 0.5)
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

class GA:
    def __init__(self, maxIter, maxBlock, populationSize, probMutate, probCross, probSelect, D, po, alpha, beta, surrogateFlag):
        self.m_MaxIter = maxIter
        self.m_MaxBlock = maxBlock
        self.m_PopulationSize = populationSize
        self.m_Population = []
        self.m_SurrogateFlag = surrogateFlag
        self.m_Runtime = 0

        self.m_ProbMutate = probMutate
        self.m_ProbCross = probCross
        self.m_ProbSelect = probSelect
        self.m_PO = po
        self.m_D = D
        self.m_Alpha = alpha
        self.m_Beta = beta
        self.m_Block = 0
        self.m_BestSolution = None
        self.m_BestFitness = -1000

        self.m_Iter = 0
        self.m_TabuList = []
        self.m_CandidateList = []
        self.m_TabuMaxLength = tabuMaxLength
        self.m_TabuMaxIter = tabuMaxIter
        self.m_MaxNumCandidate = maxNumCandidate
        self.m_CurrentSolution = None
        self.m_BestCostPerGen = []
        self.m_ConverGen = 0 # mark the generation when algorithm converges

    def select(self):
        nextPopulation = []
        pi = []
        fitnessSum = 0
        self.m_Population = sorted(self.m_Population, key=lambda x:x.fitness)
        nextPopulation.append(copy.deepcopy(self.m_Population[-1]))

        for ind in self.m_Population:
            fitnessSum = fitnessSum + ind.fitness
        pi.append(self.m_Population[0].fitness / fitnessSum)
        for ri in range(1, len(self.m_Population)):
            pi.append(self.m_Population[ri].fitness / fitnessSum + pi[ri - 1])
        copyNum = len(self.m_Population) - 1
        for ri in range(1, len(self.m_Population)):
            randnum = random.random()
            for j in range(len(pi)):
                if randnum <= pi[j]:
                    copyNum = j
                    break
            nextPopulation.append(copy.deepcopy(self.m_Population[copyNum]))
        self.m_Population = nextPopulation

    def crossover(self):
        # chromosomes cross
        hash = []
        for ci in range(len(self.m_Population)):
            hash.append(0)
        hash[0] = 1
        for ci in range(1, len(self.m_Population) / 2):
            hash[ci] = 1
            j = 0
            while hash[j] == 1:
                j = len(self.m_Population) / 2 + randint(0, len(self.m_Population) / 2 - 1)
            hash[j] = 1
            if random.random() > self.m_ProbCross:
                # cross gene between pointA and pointB
                pointA = randint(0, len(self.m_Population[0].geneSerial) - 1)
                pointB = randint(0, len(self.m_Population[0].geneSerial) - 1)
                if pointA >= pointB:
                    tmp = pointA
                    pointA = pointB
                    pointB = tmp
                if ci != 0 and j != 0:
                    for k in range(pointA, pointB + 1):
                        tmp = self.m_Population[ci].geneSerial[k]
                        self.m_Population[ci].geneSerial[k] = self.m_Population[j].geneSerial[k]
                        self.m_Population[j].geneSerial[k] = tmp

    def mutate(self):
        # chromosomes mutation
        for k in range(0, int(len(self.m_Population) * len(self.m_Population[0].geneSerial) * self.m_ProbMutate) + 1):
            mi = randint(0, len(self.m_Population) - 1)
            ik = randint(0, len(self.m_Population[0].geneSerial) - 1)
            vk = randint(0, self.m_PO.PROVIDERS[ik].cnt - 1)
            self.m_Population[mi].geneSerial[ik] = vk

    def calcPopulationFitness(self, sc):
        
        '''
        for chromosome in self.m_Population:
            if self.m_SurrogateFlag:
                chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitnessWithSurrogate(
                    chromosome.geneSerial, self.m_PO, self.m_D)
            else:
                chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitness(
                    chromosome.geneSerial, self.m_PO, self.m_D)
        '''
        raw_data = []

        for chromosome in self.m_Population:
            raw_data.append(chromosome.geneSerial)

        self.m_Population = []
        distPop = sc.parallelize(raw_data)
        fitnessCalc = distPop.map(lambda geneSerial: self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO), copy.copy(self.m_D)))
        chromosomeCollect = fitnessCalc.collect()
        for (geneSerial, fitness, mmd, sigmaCapacity, sigmaCost, sigmaDemand) in chromosomeCollect:
            chromosome = Chromosome()
            chromosome.geneSerial = geneSerial
            chromosome.fitness = fitness
            chromosome.mmd = mmd
            chromosome.sigmaCapacity = sigmaCapacity
            chromosome.sigmaCost = sigmaCost
            chromosome.sigmaDemand = sigmaDemand
            self.m_Population.append(chromosome)

    def LocalSearch(self, sc):
        
        # local search using tabu search
        self.m_Iter, self.m_Block = 0, 0
        self.m_CurrentSolution = self.m_BestSolution
        
        while self.m_Iter < self.m_TabuMaxIter and self.m_Block < self.m_MaxBlock:
            
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
            fitnessCalc = distPop.map(lambda geneSerial: self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO), copy.copy(self.m_D)))
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
            
            self.m_Iter += 1


    def GASearch(self, sc):
        
        self.m_Iter, self.m_Block = 0, 0
        
        for _ in range(self.m_PopulationSize):
            self.m_Population.append(self.generateRandomChromosome())
        self.calcPopulationFitness(sc)
        tmp = sorted(self.m_Population, key=lambda x:x.fitness, reverse=True)
        self.m_BestSolution = tmp[0]
        self.m_BestFitness = self.m_BestSolution.fitness

        
        # startTime = time.time()
	while self.m_Iter < self.m_MaxIter and self.m_Block < self.m_MaxBlock:
            #print "the " + str(iter) + " th iteration"
            self.select()
            self.crossover()
            self.mutate()
            self.calcPopulationFitness(sc)
            sortedPopulation = sorted(self.m_Population, key=lambda x: x.fitness, reverse=True)
            if sortedPopulation[0].fitness > self.m_BestFitness:
                self.m_BestFitness = sortedPopulation[0].fitness
                self.m_BestSolution = copy.deepcopy(sortedPopulation[0])
                self.m_Block = 0
            elif math.fabs(sortedPopulation[0].fitness - self.m_BestFitness) <= 0.001:
                self.m_Block += 1
            self.m_BestCostPerGen.append(self.m_BestSolution.sigmaCost)
            #print "the best individual serial, fitness, mmd, sigmaCost, sigmaCapacity, sigmaDemand ",\
            #    sortedPopulation[0].geneSerial, sortedPopulation[0].fitness,sortedPopulation[0].mmd, sortedPopulation[0].sigmaCost, sortedPopulation[0].sigmaCapacity, sortedPopulation[0].sigmaDemand
            #print sortedPopulation[0].sigmaCost

            self.m_Iter += 1
	#endTime = time.time()
	#self.m_Runtime = endTime - startTime
    	self.m_ConverGen = self.m_Iter

    def Search(self, sc):
        
            startTime = time.time()
            self.GASearch(sc)
            #self.LocalSearch(sc)
            endTime = time.time()
            self.m_Runtime = endTime - startTime


    def generateRandomChromosome(self):
        chromosome = Chromosome()
        for i in range(len(self.m_PO.PROVIDERS)):
            chromosome.geneSerial.append(randint(0, self.m_PO.PROVIDERS[i].cnt - 1))
        #if self.m_SurrogateFlag:
        #    chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitnessWithSurrogate(
        #        chromosome.geneSerial, self.m_PO, self.m_D)
        #else:
        #    chromosome.fitness, chromosome.mmd, chromosome.sigmaCapacity, chromosome.sigmaCost, chromosome.sigmaDemand = self.calcFitness(
        #        chromosome.geneSerial, self.m_PO, self.m_D)
        return chromosome

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
                fitness = -10.0
            else:
                if sigmaCost != 0:
                    fitness = float(20.0 / sigmaCost)
                else:
                    fitness = 20.0
        else:
            fitness = -20.0
        # print("fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand:",fitness,mmd,sigmaCapacity,sigmaCost,sigmaDemand)
        # return math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand

        return (geneSerial, math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand)

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

if __name__ == "__main__":

    popSize = 100
    iterMax = 100
    blockMax = 101
    probMutate = 0.0001
    probCross = 0.7
    probSelect = 0.1
    D = 40.0
    alpha = 10000000.00
    beta = 0.01
    surrogateFlag = False
    tabuMaxLength = 10
    tabuMaxIter = 100
    maxNumCandidate = 10

    core_num = int(sys.argv[1])
    conf = SparkConf().setMaster("spark://noah007:7077") \
        .setAppName("SPC-POSM-GA") \
        .set("spark.submit.deployMode", "client") \
        .set("spark.cores.max", core_num) \
        .set("spark.executor.cores", "10") \
        .set("spark.executor.memory", "20g") \
        .set("spark.driver.memory", "40g")

    sc = SparkContext(conf=conf)

    '''
        experiment on different datasets
    '''

    '''
    #instanceSet = ['nuoxi2G']  #, 'nuoxi3G', 'huawei2G', 'huawei3G']
    instanceSet = [i for i in range(60)]
    aveAns, aveRuntime, aveConverGen = [], [], []    
    
    for i in instanceSet:
        print i, 'th instance ...'
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        sumAns, sumRuntime, sumConverGen = 0, 0, 0
        times = 5
        for _ in range(times): 
	    ga = GA(iterMax, blockMax, popSize, probMutate, probCross, probSelect, D, po, alpha, beta,
                    surrogateFlag)
	    
            ga.Search(sc)

            sumAns += ga.m_BestSolution.sigmaCost
            sumRuntime += ga.m_Runtime
            sumConverGen = ga.m_ConverGen
        aveAns.append(sumAns / (times*1.0))
	aveRuntime.append(sumRuntime / (times*1.0))
        aveConverGen.append(sumConverGen / (times*1.0))

    df = pd.DataFrame({'cost': aveAns, 'GA runtime': aveRuntime, 'ConverGen':aveConverGen})
    df.to_csv('../midResult/gaResult.csv')
    '''
    '''
           experiment of convergence
    '''
    '''
    instList = [4, 25, 47]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        ga = GA(iterMax, blockMax, popSize, probMutate, probCross, probSelect, D, po, alpha, beta,
                    surrogateFlag)
        ga.Search(sc)
        costPerGenList.append(ga.m_BestCostPerGen)

    df = pd.DataFrame({'small': costPerGenList[0], 'medium': costPerGenList[1], 'large': costPerGenList[2]})
    df.to_csv('../midResult/gaResultBestCostPerGen.csv')
    '''

    '''
            experiment of convergence
    '''
    instNum = 20

    instList = [i for i in range(instNum)]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        ga = GA(iterMax, blockMax, popSize, probMutate, probCross, probSelect, D, po, alpha, beta,
                surrogateFlag)
        ga.Search(sc)
        costPerGenList.append(ga.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})

    df.to_csv('../midResult/gaResultBestCostPerGen1.csv')

    instNum = 40

    instList = [i for i in range(20,instNum)]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        ga = GA(iterMax, blockMax, popSize, probMutate, probCross, probSelect, D, po, alpha, beta,
                surrogateFlag)
        ga.Search(sc)
        costPerGenList.append(ga.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})

    df.to_csv('../midResult/gaResultBestCostPerGen2.csv')

    instNum = 60

    instList = [i for i in range(40,instNum)]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        ga = GA(iterMax, blockMax, popSize, probMutate, probCross, probSelect, D, po, alpha, beta,
                surrogateFlag)
        ga.Search(sc)
        costPerGenList.append(ga.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})

    df.to_csv('../midResult/gaResultBestCostPerGen3.csv')