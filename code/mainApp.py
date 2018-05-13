import sys
from pyspark import SparkContext, SparkConf
from PO import PO
from FileProcess import LoadDataFromText
from EDA import EDA

if __name__ == "__main__":

    popSize = 20
    iterMax = 100
    blockMax = 3
    alpha = 10000000.00
    beta = 0.01
    D = 40.0
    surrogateFlag = False
    ratioList = [i * 0.05 for i in range(1, 21)]

    core_num = int(sys.argv[1])

    conf = SparkConf().setMaster("spark://noah007:7077") \
        .setAppName("SPC-POSM-EDA") \
        .set("spark.submit.deployMode", "client") \
        .set("spark.cores.max", core_num) \
        .set("spark.executor.cores", "10") \
        .set("spark.executor.memory", "10g") \
        .set("spark.driver.memory", "40g")

    sc = SparkContext(conf=conf)
    sc.addPyFile("/home/tongxialiang/workspace/lixj/SPC-POSM/SPC-POSM.zip")

    with open('../midResult/edaResult.txt', 'w') as f:
        for i in range(20):
            print i, 'th instance ...'
            # po is data contains informantion about PROVIDERS and CUSTOMERS
            po = PO()
            # read providers and customers data from text
            po.PROVIDERS, po.CUSTOMERS = LoadDataFromText('../data/instance' + str(i) + '.txt')

            '''
            for surrogateSizeRatio in ratioList:
                print "surrogate size ratio", surrogateSizeRatio
                eda = EDA(popSize, iterMax, blockMax, po, alpha, beta, D, surrogateFlag, surrogateSizeRatio)
                eda.evaluate()
                print "the best solution serial, fitness, mmd, sigmaCost, sigmaCapacity, sigmaDemand ", \
                    eda.m_BestSolution.geneSerial, eda.m_BestSolution.fitness, eda.m_BestSolution.mmd, eda.m_BestSolution.sigmaCost, eda.m_BestSolution.sigmaCapacity, eda.m_BestSolution.sigmaDemand
                print "---------------------------------"
            '''
            sumAns = 0
            times = 5

            for _ in range(times):
                eda = EDA(sc, popSize, iterMax, blockMax, po, alpha, beta, D, surrogateFlag, 0)
                eda.evaluate()
                sumAns += eda.m_BestSolution.sigmaCost
            f.write(str(sumAns / times) + '\n')