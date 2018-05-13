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
from sklearn.cluster import KMeans


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
        self.calcAccurate = False

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


class PSO:
    def __init__(self, po, D, populationSize, maxIter, alpha, beta, maxBlock, c1, c2, w, surrogateFlag, surrogateCoeff):
        self.m_MaxIter = maxIter
        self.m_MaxBlock = maxBlock
        self.m_PopulationSize = populationSize
        self.m_Population = []
        self.m_SurrogateFlag = surrogateFlag
        self.m_SurrogateCoeff = surrogateCoeff
        self.Runtime = 0

        self.m_C1 = c1
        self.m_C2 = c2
        self.m_W = w
        self.m_PO = po
        self.m_D = D
        self.m_Alpha = alpha
        self.m_Beta = beta
        self.m_Block = 0

        for _ in range(self.m_PopulationSize):
            self.m_Population.append(self.generateRandomChromosome())

        self.m_BestSolution = None

        self.m_Iter = 0
        self.m_TabuList = []
        self.m_CandidateList = []
        self.m_TabuMaxLength = tabuMaxLength
        self.m_TabuMaxIter = tabuMaxIter
        self.m_MaxNumCandidate = maxNumCandidate
        self.m_CurrentSolution = None
        self.m_BestCostPerGen = []
        self.m_ConverGen = 0 # mark the generation when algorithm converges

    def calcFitnessParallel(self, geneSerial, data, D, idx):
        #start_time = time.time()
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
        #end_time = time.time()
        #print end_time-start_time
        return (geneSerial, idx, math.exp(fitness), mmd, sigmaCapacity, sigmaCost, sigmaDemand)

    def calcPopulationFitnessWithSurrogate(self, sc):

        # cluster chromosome
        raw_data = []
        for i in range(len(self.m_Population)):
            raw_data.append(self.m_Population[i].geneSerial)
        raw_data = np.array(raw_data)
        num_cluster = int(self.m_PopulationSize * self.m_SurrogateCoeff)
        kmeans_startTime = time.time()
        kmeans = KMeans(n_clusters=num_cluster, random_state=0).fit(raw_data)
        kmeans_endTime = time.time()
        print 'kmeans calculate time', kmeans_endTime - kmeans_startTime
        distances = kmeans.transform(raw_data).sum(axis=1)
        labels = kmeans.labels_
        for i in range(len(self.m_Population)):
            self.m_Population[i].cluster = labels[i]

        find_center_startTime = time.time()
        raw_data_1, closet_item_idx = [], []
        for clst in range(num_cluster):
            min_idx, min_dist = -1, -1
            for idx in range(len(distances)):
                if labels[idx] == clst:
                    if min_dist < 0 and min_dist < 0:
                        min_idx = idx
                        min_dist = distances[idx]
                    elif min_dist > distances[idx]:
                        min_idx = idx
                        min_dist = distances[idx]
            raw_data_1.append((raw_data[min_idx], min_idx))
            closet_item_idx.append(min_idx)
        find_center_endTime = time.time()
        print 'finding cluster time', find_center_endTime-find_center_startTime
        raw_data = raw_data_1

        swap_chain_startTime = time.time()
        distPop = sc.parallelize(raw_data)
        fitnessCalc = distPop.map(
            lambda (geneSerial, idx): self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO), copy.copy(self.m_D),idx))
        chromosomeCollect = fitnessCalc.collect()
        swap_chain_endTime = time.time()
        print 'parallel calculate swap chain time', swap_chain_endTime - swap_chain_startTime

        for (geneSerial, idx, fitness, mmd, sigmaCapacity, sigmaCost, sigmaDemand) in chromosomeCollect:
            self.m_Population[idx].fitness = fitness
            self.m_Population[idx].mmd = mmd
            self.m_Population[idx].sigmaCapacity = sigmaCapacity
            self.m_Population[idx].sigmaCost = sigmaCost
            self.m_Population[idx].sigmaDemand = sigmaDemand
            self.m_Population[idx].calcAccurate = True

        for i in range(self.m_PopulationSize):
            if i not in closet_item_idx:
                self.m_Population[i].fitness = self.m_Population[closet_item_idx[self.m_Population[i].cluster]].fitness
                self.m_Population[i].calcAccurate = False

        # update personal best for each individual
        for chromosome in self.m_Population:
            if not chromosome.pbest:
                chromosome.pbest = copy.deepcopy(chromosome)
            else:
                if chromosome.fitness > chromosome.pbest.fitness:
                    chromosome.pbest = copy.deepcopy(chromosome)

        sortedPopulation = sorted(self.m_Population, key=lambda x: x.fitness, reverse=True)


        if not self.m_BestSolution:
            for i in range(len(sortedPopulation)):
                if sortedPopulation[i].calcAccurate:
                    self.m_BestSolution = copy.deepcopy(sortedPopulation[i])
                    break
        else:
            calcAccurateIdx = None
            for i in range(len(sortedPopulation)):
                if sortedPopulation[i].calcAccurate:
                    calcAccurateIdx = i
                    break
            if self.m_BestSolution.fitness < sortedPopulation[calcAccurateIdx].fitness:
                self.m_BestSolution = copy.deepcopy(sortedPopulation[calcAccurateIdx])
                self.m_Block = 0
            elif math.fabs(self.m_BestSolution.fitness - sortedPopulation[calcAccurateIdx].fitness) <= 0.001:
                self.m_Block += 1

    def calcPopulationFitness(self, sc):

        # calculate the fitness of each individual
        raw_data = []

        for i in range(len(self.m_Population)):
            raw_data.append((self.m_Population[i].geneSerial,i))
        swap_chain_startTime = time.time()
        distPop = sc.parallelize(raw_data)
        fitnessCalc = distPop.map(
            lambda (geneSerial, idx): self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO), copy.copy(self.m_D), idx))
        chromosomeCollect = fitnessCalc.collect()
        swap_chain_endTime = time.time()
        print 'parallel calculate swap chain time', swap_chain_endTime - swap_chain_startTime

        for (geneSerial, idx, fitness, mmd, sigmaCapacity, sigmaCost, sigmaDemand) in chromosomeCollect:
            self.m_Population[idx].fitness = fitness
            self.m_Population[idx].mmd = mmd
            self.m_Population[idx].sigmaCapacity = sigmaCapacity
            self.m_Population[idx].sigmaCost = sigmaCost
            self.m_Population[idx].sigmaDemand = sigmaDemand

        # update personal best for each individual
        for chromosome in self.m_Population:
            if not chromosome.pbest:
                chromosome.pbest = copy.deepcopy(chromosome)
            else:
                if chromosome.fitness > chromosome.pbest.fitness:
                    chromosome.pbest = copy.deepcopy(chromosome)

        sortedPopulation = sorted(self.m_Population, key=lambda x: x.fitness, reverse=True)

        if not self.m_BestSolution:
            self.m_BestSolution = copy.deepcopy(sortedPopulation[0])
        else:
            if self.m_BestSolution.fitness < sortedPopulation[0].fitness:
                self.m_BestSolution = copy.deepcopy(sortedPopulation[0])
                self.m_Block = 0
            elif math.fabs(self.m_BestSolution.fitness - sortedPopulation[0].fitness) <= 0.001:
                self.m_Block += 1

    def update(self):
        for chromosome in self.m_Population:
            for d in range(len(chromosome.geneSerial)):
                r1, r2 = random.random(), random.random()
                vLast = chromosome.v[d]
                chromosome.v[d] = (self.m_W * chromosome.v[d] \
                                   + self.m_C1 * r1 * (chromosome.pbest.geneSerial[d] - chromosome.geneSerial[d]) \
                                   + self.m_C2 * r2 * (self.m_BestSolution.geneSerial[d] - chromosome.geneSerial[d])) % \
                                  self.m_PO.PROVIDERS[d].cnt
                chromosome.geneSerial[d] = int((chromosome.geneSerial[d] + vLast) % self.m_PO.PROVIDERS[d].cnt)

    def LocalSearch(self, sc):
        # local search using tabu search
        self.m_Iter, self.m_Block = 0, 0
        self.m_CurrentSolution = self.m_BestSolution
        
        while self.m_Iter < self.m_TabuMaxIter and self.m_Block < self.m_MaxBlock:
            print "the " + str(self.m_Iter) + " th local search iteration"
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
                    raw_data.append((geneSerial,0))

            # parallelly compute the fitness for each individual
            distPop = sc.parallelize(raw_data)    
            fitnessCalc = distPop.map(lambda (geneSerial,idx): self.calcFitnessParallel(geneSerial, copy.copy(self.m_PO), copy.copy(self.m_D), idx))
            chromosomeCollect = fitnessCalc.collect()

            for (geneSerial, idx, fitness, mmd, sigmaCapacity, sigmaCost, sigmaDemand) in chromosomeCollect:
                chromosome = Chromosome()
                chromosome.geneSerial = geneSerial
                chromosome.fitness = fitness
                chromosome.mmd = mmd
                chromosome.sigmaCapacity = sigmaCapacity
                chromosome.sigmaCost = sigmaCost
                chromosome.sigmaDemand = sigmaDemand
                self.m_CandidateList.append((chromosome, chromosome.fitness, (flag, pointA, pointB)))

            nextBestChromosome, nextBestFitness, tabu = sorted(self.m_CandidateList, key=lambda x: x[1], reverse=True)[0]

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

    def PSOSearch(self, sc):
        self.m_Iter, self.m_Block = 0, 0
        
        while self.m_Iter < self.m_MaxIter and self.m_Block < self.m_MaxBlock:
            print "the " + str(self.m_Iter) + " th PSO iteration"
            if not self.m_SurrogateFlag:
                self.calcPopulationFitness(sc)
            else:
                self.calcPopulationFitnessWithSurrogate(sc)
            self.m_BestCostPerGen.append(self.m_BestSolution.sigmaCost)
            self.update()

            #print "the best individual serial, fitness, mmd, sigmaCost, sigmaCapacity, sigmaDemand ", \
            #    self.m_BestSolution.geneSerial, self.m_BestSolution.fitness, self.m_BestSolution.mmd, self.m_BestSolution.sigmaCost, \
            #    self.m_BestSolution.sigmaCapacity, self.m_BestSolution.sigmaDemand
            #print self.m_BestSolution.sigmaCost

            self.m_Iter += 1

        self.m_ConverGen = self.m_Iter

    def Search(self, sc):
        
            startTime = time.time()
            self.PSOSearch(sc)
            #self.LocalSearch(sc)
            endTime = time.time()
            self.m_Runtime = endTime - startTime

    def generateRandomChromosome(self):
        chromosome = Chromosome()
        for i in range(len(self.m_PO.PROVIDERS)):
            chromosome.geneSerial.append(randint(0, self.m_PO.PROVIDERS[i].cnt - 1))
            chromosome.v.append(randint(0, self.m_PO.PROVIDERS[i].cnt - 1))
        return chromosome

'''
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
                fitness = -10.0
            else:
                if sigmaCost != 0:
                    fitness = float(20.0 / sigmaCost)
                else:
                    fitness = 20.0
        else:
            fitness = -20.0
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
'''

if __name__ == "__main__":

    popSize = 100
    iterMax = 100
    blockMax = 101
    alpha = 10000000.00
    beta = 0.01
    D = 40.0
    c1 = 0.1
    c2 = 0.2
    w = 0.5
    surrogateFlag = False
    surrogateCoeff = 0.1 # denote the cluster ratio in surrograte model
    tabuMaxLength = 10
    tabuMaxIter = 100
    maxNumCandidate = 10


    core_num = int(sys.argv[1])
    conf = SparkConf().setMaster("spark://noah007:7077") \
        .setAppName("SPC-POSM-PSO") \
        .set("spark.submit.deployMode", "client") \
        .set("spark.cores.max", core_num) \
        .set("spark.executor.cores", "10") \
        .set("spark.executor.memory", "20g") \
        .set("spark.driver.memory", "40g")

    sc = SparkContext(conf=conf)


    '''
        experiment for accuracy on different dataset
    '''
    '''
    instanceSet = ['nuoxi2G']  # , 'nuoxi3G', 'huawei2G', 'huawei3G']
    '''
    '''
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
            pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
            pso.Search(sc)
            sumAns += pso.m_BestSolution.sigmaCost
            sumRuntime += pso.m_Runtime
            sumConverGen = pso.m_ConverGen

        aveAns.append(sumAns / (times * 1.0))
        aveRuntime.append(sumRuntime / (times * 1.0))
        aveConverGen.append(sumConverGen / (times*1.0))

    df = pd.DataFrame({'cost': aveAns, 'PSO runtime': aveRuntime, 'ConverGen':aveConverGen})
    df.to_csv('../midResult/psoResult.csv')
    '''

    '''
        experiment for exploring algorithm convergence
    '''
    '''
    instList = [4, 25, 47]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
        pso.Search(sc)
        costPerGenList.append(pso.m_BestCostPerGen)

    df = pd.DataFrame({'small': costPerGenList[0], 'medium': costPerGenList[1], 'large': costPerGenList[2]})
    df.to_csv('../midResult/psoResultBestCostPerGen.csv')
    '''

    '''
        experiment for testing the algorithm accuracy against lp solver on tiny dataset
    '''
    '''
    geneSerialList, sigmaCostList = [], []

    D = [40,6,40,50,60,40]
    for i in range(1, 7):
        print i, 'th instance ...'
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/data' + str(i) + '.txt')

        pso = PSO(po, D[i-1], popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
        pso.Search(sc)
        geneSerialList.append(pso.m_BestSolution.geneSerial)
        sigmaCostList.append(pso.m_BestSolution.sigmaCost)

    df = pd.DataFrame({'sigmaCost': sigmaCostList, 'geneSerial': geneSerialList})
    df.to_csv('../midResult/psoTinyDatasetResult.csv')
    '''


    '''
        experiment for exploring the algorithm performance under different surrogate cluster ratio
    '''
    '''
    surrogateCoeffList = [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]

    for surrogateCoeff_tmp in surrogateCoeffList:
        surrogateCoeff = surrogateCoeff_tmp

        aveAns, aveRuntime, aveConverGen = [], [], []

        for i in instList:
            print i, 'th instance ...'
            # po is data contains informantion about PROVIDERS and CUSTOMERS
            po = PO()
            # read providers and customers data from text
            po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

            sumAns, sumRuntime, sumConverGen = 0, 0, 0
            times = 10
            for _ in range(times):
                pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
                pso.Search(sc)
                sumAns += pso.m_BestSolution.sigmaCost
                sumRuntime += pso.m_Runtime
                sumConverGen = pso.m_ConverGen

            aveAns.append(sumAns / (times * 1.0))
            aveRuntime.append(sumRuntime / (times * 1.0))
            aveConverGen.append(sumConverGen / (times * 1.0))

        df = pd.DataFrame({'cost': aveAns, 'PSO runtime': aveRuntime, 'ConverGen': aveConverGen})
        df.to_csv('../midResult/psoResult'+'Surrogate'+str(surrogateCoeff)+'.csv')
    '''
    '''
    surrogateFlag = True

    instList = [1]#, 21, 41]

    #surrogateCoeffList = [1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    surrogateCoeffList = [1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    for i in instList:
        print i, 'th instance ...'
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        aveAns, aveRuntime, aveConverGen = [], [], []

        for surrogateCoeff_tmp in surrogateCoeffList:

            surrogateCoeff = surrogateCoeff_tmp
            if surrogateCoeff_tmp == 1:
                surrogateFlag = False
            else:
                surrogateFlag = True
            print surrogateCoeff, ' surrogate coefficient'
            sumAns, sumRuntime, sumConverGen = 0, 0, 0
            times = 1
            for _ in range(times):
                pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
                pso.Search(sc)
                sumAns += pso.m_BestSolution.sigmaCost
                sumRuntime += pso.m_Runtime
                sumConverGen = pso.m_ConverGen

            aveAns.append(sumAns / (times * 1.0))
            aveRuntime.append(sumRuntime / (times * 1.0))
            aveConverGen.append(sumConverGen / (times * 1.0))

        df = pd.DataFrame({'clusterRatio': surrogateCoeffList,'cost': aveAns, 'runtime': aveRuntime})
        df.to_csv('../midResult/psoResultInstance' + str(i) + '.csv')
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

        pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
        pso.Search(sc)
        costPerGenList.append(pso.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})
    df.to_csv('../midResult/psoResultBestCostPerGen1.csv')

    instNum = 40

    instList = [i for i in range(20, instNum)]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
        pso.Search(sc)
        costPerGenList.append(pso.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})
    df.to_csv('../midResult/psoResultBestCostPerGen2.csv')

    instNum = 60

    instList = [i for i in range(40,instNum)]
    costPerGenList = []

    for i in instList:
        # po is data contains informantion about PROVIDERS and CUSTOMERS
        po = PO()
        # read providers and customers data from text
        po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

        pso = PSO(po, D, popSize, iterMax, alpha, beta, blockMax, c1, c2, w, surrogateFlag, surrogateCoeff)
        pso.Search(sc)
        costPerGenList.append(pso.m_BestCostPerGen)

    costPerGenNpArr = np.array(costPerGenList)
    # print costPerGenList
    # print costPerGenNpArr
    # print type(costPerGenNpArr)
    costPerGenNpArr = np.sum(costPerGenNpArr, axis=0)
    print costPerGenNpArr
    # costPerGenNpArr = costPerGenNpArr / float(instNum)
    df = pd.DataFrame({'aveCost': costPerGenNpArr})
    df.to_csv('../midResult/psoResultBestCostPerGen3.csv')

