#!/bin/bash

export PATH=/home/tongxialiang/software/anaconda2/bin:/home/tongxialiang/software/spark-2.2.1-bin-hadoop2.7/bin:/home/tongxialiang/software/scala-2.10.4/bin:/home/tongxialiang/software/hadoop-2.7.3/bin:/home/chenhaoyang/jdk1.8.0_111/bin:/home/chenhaoyang/jdk1.8.0_111/jre:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games
export SPARK_HOME=/home/tongxialiang/software/spark-2.2.1-bin-hadoop2.7
export PYTHONPATH=/home/tongxialiang/software/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip:/home/tongxialiang/software/spark-2.2.1-bin-hadoop2.7/python/:/home/tongxialiang/software/xgboost/python-package/:/home/tongxialiang/gongfei/gurobi751/linux64/lib:/home/tongxialiang/workspace/lixj:/home/tongxialiang/workspace/lixj/SPC-POSM/

python EDAParallel.py 150 1>std.io 2>spark
