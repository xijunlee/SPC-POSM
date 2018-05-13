'''
    this code is for transforming real data obtained from unicom to experimental data
'''

import pandas as pd
import numpy as np

file = 'huawei3G'

df = pd.read_csv('..\data\\' + file + '.csv')

mu, sigma = 0, 1

min_num_capacity, max_num_capacity = 5, 10

x_provider = np.round(df['Lon'].values, 2)
y_provider = np.round(df['Lat'].values, 2)

num_capacity = np.random.randint(min_num_capacity, max_num_capacity)
num_provider = x_provider.shape[0]
num_customer = num_provider * np.random.randint(2, 4)

x_customer = np.round(np.random.normal(mu, sigma, num_customer), 2)
y_customer = np.round(np.random.normal(mu, sigma, num_customer), 2)

min_x, max_x = min(x_provider), max(x_provider)
x_provider = (x_provider - min_x) / (max_x - min_x) * 100
min_y, max_y = min(y_provider), max(y_provider)
y_provider = (y_provider - min_y) / (max_y - min_y) * 100

min_x, max_x = min(x_customer), max(x_customer)
x_customer = (x_customer - min_x) / (max_x - min_x) * 100
min_y, max_y = min(y_customer), max(y_customer)
y_customer = (y_customer - min_y) / (max_y - min_y) * 100


min_demand, max_demand = 1, 10
min_capacity, max_capacity = 0, 50
min_cost, max_cost = 100, 1000

# ratio of old provider to all provider
old_ratio = 0.3

with open('..\data\\' + file + '.txt', 'w') as f:
    f.write(str(num_provider) + '\n')

    # generate the demand of each customer
    demand_customer = np.random.randint(min_demand, max_demand, num_customer)

    # generate the capacity and corresponding cost of provider
    for i in range(num_provider):
        flag = np.random.random()
        if flag < old_ratio:  # a provider is old if flag is less than old_ratio
            capacity = np.random.randint(min_capacity, max_capacity, num_capacity - 1)
            capacity = list(capacity) + [0]
            cost = np.random.randint(min_cost, max_cost, num_capacity - 2)
            cost = list(cost) + [0, 0]
        else:
            capacity = np.random.randint(min_capacity, max_capacity, num_capacity - 1)
            capacity = list(capacity) + [0]
            cost = np.random.randint(min_cost, max_cost, num_capacity - 1)
            cost = list(cost) + [0]

        cost = sorted(cost)
        capacity = sorted(capacity)
        f.write(str(x_provider[i]) + ' ' + str(y_provider[i]) + ' ' + str(num_capacity) + ' ')
        for j in range(len(capacity)):
            f.write(str(capacity[j]) + ' ')
        for j in range(len(cost) - 1):
            f.write(str(cost[j]) + ' ')
        f.write(str(cost[len(cost) - 1]) + '\n')

    f.write(str(num_customer) + '\n')
    for i in range(num_customer):
        f.write(str(x_customer[i]) + ' ' + str(y_customer[i]) + ' ' + str(demand_customer[i]) + '\n')
