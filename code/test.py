import numpy as np
from sklearn.cluster import KMeans

raw_data = []
fitness = [0,0,0,0,0,0]
Population = [[1, 2], [1, 4], [1, 0], [4, 2], [4, 4], [4, 0]]
for i in range(len(Population)):
    raw_data.append(Population[i])
raw_data = np.array(raw_data)
num_cluster = int(2)
kmeans = KMeans(n_clusters=num_cluster, random_state=0).fit(raw_data)
distances = kmeans.transform(raw_data).sum(axis=1)
labels = kmeans.labels_
print distances
print labels
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
    raw_data_1.append((raw_data[min_idx],min_idx))
    closet_item_idx.append(min_idx)

print raw_data_1
print closet_item_idx

for i in range(len(closet_item_idx)):
    fitness[closet_item_idx[i]] = i+1

print fitness

for i in range(len(Population)):
    if i not in closet_item_idx:
        fitness[i] = fitness[closet_item_idx[labels[i]]]

print fitness