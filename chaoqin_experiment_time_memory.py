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


workload = ['lr']
df = pd.read_csv("conf_chaoqin.csv")
for k in range(len(workload)):
    #subprocess.call(['./bin/workloads/ml/' + workload[k] + '/prepare/prepare.sh'])
    #for i in range(df.shape[0]):
    for times in range(100):
        i = 1
        # rewrite spark.conf
        cur_path = os.path.dirname(__file__)
        f = open(os.path.join(cur_path, '..\\conf\\spark.conf'), "w")
        f.write("hibench.spark.home      $SPARK_HOME")
        f.write("hibench.spark.master  local[*]")
        f.write("spark.eventLog.enabled           false")
        f.write("spark.sql.shuffle.partitions  ${hibench.default.shuffle.parallelism}")
        f.write("spark.default.parallelism     ${hibench.default.map.parallelism}")
        f.write('spark.reducer.maxSizeInFlight  ' + str(df.at[i,'spark.reducer.maxSizeInFlight']))
        f.write('spark.shuffle.file.buffer  ' + str(df.at[i,'spark.shuffle.file.buffer']))
        f.write('spark.shuffle.sort.bypassMergeThreshold  ' + str(df.at[i,'spark.shuffle.sort.bypassMergeThreshold']))
        f.write('spark.speculation.interval  ' + str(df.at[i,'spark.speculation.interval']))
        f.write('spark.speculation.multiplier  ' + str(df.at[i, 'spark.speculation.multiplier']))
        f.write('spark.speculation.quantile  ' + str(df.at[i, 'spark.speculation.quantile']))
        f.write('spark.broadcast.blockSize  ' + str(df.at[i, 'spark.broadcast.blockSize']))
        f.write('spark.io.compression.snappy.blockSize  ' + str(df.at[i, 'spark.io.compression.snappy.blockSize']))
        f.write('spark.kryoserializer.buffer.max  ' + str(df.at[i, 'spark.kryoserializer.buffer.max']))
        f.write('spark.kryoserializer.buffer  ' + str(df.at[i, 'spark.kryoserializer.buffer']))
        f.write('spark.driver.memory  ' + str(df.at[i, 'spark.driver.memory']))
        f.write('spark.executor.memory  ' + str(df.at[i, 'spark.executor.memory']))
        f.write('spark.network.timeout  ' + str(df.at[i, 'spark.network.timeout']))
        f.write('spark.locality.wait  ' + str(df.at[i, 'spark.locality.wait']))
        f.write('spark.task.maxFailures  ' + str(df.at[i, 'spark.task.maxFailures']))
        f.write('spark.shuffle.compress  ' + str(df.at[i, 'spark.shuffle.compress']))
        f.write('spark.memory.fraction  ' + str(df.at[i, 'spark.memory.fraction']))
        f.write('spark.shuffle.spill.compress  ' +str(df.at[i, 'spark.shuffle.spill.compress']))
        f.write('spark.broadcast.compress  ' + str(df.at[i, 'spark.broadcast.compress']))
        f.write('spark.memory.storageFraction  ' + str(df.at[i, 'spark.memory.storageFraction']))
        f.close()
        # signal the monitor to start.
        begin()
        # start monitor and collect garbage.
        time.sleep(4.5)
        # run spark work load
        #subprocess.call(['./bin/workloads/ml/' + workload[k] + '/spark/run.sh'])
        # signal the monitor to write log of memory consumption.
        end(workload[k] + ' ' + str(k))
        time.sleep(4.5)
    subprocess.call(['mv', 'report/hibench.report', 'report/spark_' + workload[k] + '.report'])
monitor_exit()