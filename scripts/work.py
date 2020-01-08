import numpy as np
import pandas as pd
import random
import subprocess
import os
import json
import time
from os import path
from os.path import expanduser

home = expanduser("~")
mypath = home + '/signal.txt'


def signal(string):
    f = open(mypath, "w")
    f.write(string)
    f.close()


def begin():
    signal('+')


def end(info):
    signal('- ' + info)


def monitor_exit():
    signal('!')


workload = ['lda']
df = pd.read_csv("conf_chaoqin.csv")
for k in range(len(workload)):
    subprocess.call(['./bin/workloads/ml/' + workload[k] + '/prepare/prepare.sh'])
    for i in range(df.shape[0]):
    #for times in range(10):
 
    # subprocess.call(['./bin/workloads/ml/' + workload[k] + '/prepare/prepare.sh'])
        os.system("cp conf/spark.conf.template conf/spark.conf")
   # for i in range(df.shape[0]):
    #for times in range(100):
        # rewrite spark.conf
       # cur_path = os.path.dirname(__file__)
       # f = open(cur_path + '/conf/spark.conf', "w")
        cur_path = os.path.dirname(__file__)
        f = open(home + '/HiBench/conf/spark.conf', "w")
        f.write("hibench.spark.home      $SPARK_HOME\n")
        f.write("hibench.spark.master  local[*]\n")
        f.write("spark.eventLog.enabled           false\n")
        f.write("spark.sql.shuffle.partitions  ${hibench.default.shuffle.parallelism}\n")
        f.write("spark.default.parallelism     ${hibench.default.map.parallelism}\n")
        f.write('spark.reducer.maxSizeInFlight  ' + str(df.at[i,'spark.reducer.maxSizeInFlight']) + '\n')
        f.write('spark.shuffle.file.buffer  ' + str(df.at[i,'spark.shuffle.file.buffer']) + '\n')
        f.write('spark.shuffle.sort.bypassMergeThreshold  ' + str(df.at[i,'spark.shuffle.sort.bypassMergeThreshold']) + '\n')
        f.write('spark.speculation.interval  ' + str(df.at[i,'spark.speculation.interval']) + '\n')
        f.write('spark.speculation.multiplier  ' + str(df.at[i, 'spark.speculation.multiplier']) + '\n')
        f.write('spark.speculation.quantile  ' + str(df.at[i, 'spark.speculation.quantile']) + '\n')
        f.write('spark.broadcast.blockSize  ' + str(df.at[i, 'spark.broadcast.blockSize']) + '\n')
        f.write('spark.io.compression.snappy.blockSize  ' + str(df.at[i, 'spark.io.compression.snappy.blockSize']) + '\n')
        f.write('spark.kryoserializer.buffer.max  ' + str(df.at[i, 'spark.kryoserializer.buffer.max']) + '\n')
        f.write('spark.kryoserializer.buffer  ' + str(df.at[i, 'spark.kryoserializer.buffer']) + '\n')
        f.write('spark.driver.memory  ' + str(df.at[i, 'spark.driver.memory']) + '\n')
        f.write('spark.executor.memory  ' + str(df.at[i, 'spark.executor.memory']) + '\n')
        f.write('spark.network.timeout  ' + str(df.at[i, 'spark.network.timeout']) + '\n')
        f.write('spark.locality.wait  ' + str(df.at[i, 'spark.locality.wait']) + '\n')
        f.write('spark.task.maxFailures  ' + str(df.at[i, 'spark.task.maxFailures']) + '\n')
        f.write('spark.shuffle.compress  ' + str(df.at[i, 'spark.shuffle.compress']) + '\n')
        f.write('spark.memory.fraction  ' + str(df.at[i, 'spark.memory.fraction']) + '\n')
        f.write('spark.shuffle.spill.compress  ' +str(df.at[i, 'spark.shuffle.spill.compress']) + '\n')
        f.write('spark.broadcast.compress  ' + str(df.at[i, 'spark.broadcast.compress']) + '\n')
        f.write('spark.memory.storageFraction  ' + str(df.at[i, 'spark.memory.storageFraction']) + '\n')
        f.write('spark.eventLog.enabled           true\n')
        f.write('spark.eventLog.dir               /home/cc/spark-2.1.3-bin-hadoop2.7/mylog\n')
        f.write('spark.history.fs.logDirectory    /home/cc/spark-2.1.3-bin-hadoop2.7/mylog\n')
        f.close()
        # signal the monitor to start.
        begin()
        # start monitor and collect garbage.
        time.sleep(4.5)
        # run spark work load
        subprocess.call(['./bin/workloads/ml/' + workload[k] + '/spark/run.sh'])
        # signal the monitor to write log of memory consumption.
        end(workload[k] + ' ' + str(i))
        p = os.system("sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\"")
        time.sleep(4.5)
        os.system("rm conf/spark.conf")
    subprocess.call(['mv', 'report/hibench.report', 'report/spark_' + workload[k] + '.report'])
monitor_exit()
