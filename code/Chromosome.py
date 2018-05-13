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