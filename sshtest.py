
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

def broadcast(filename):
    ips = ["10.140.81.48", "10.140.81.77"]
    for ip in ips:
        os.system("ssh", ip, "touch", filename)


def start(workload, i):
    broadcast("start-" + workload + "-" + str(i))


def end():
    broadcast("end")


def collect_io():
    rbytes = 0
    wbytes = 0
    filelist = os.listdir("/home/cc/spark-2.1.3-bin-hadoop2.7/mylog")
    for filename in filelist:
        if 'app' in filename:
            jfile = open(os.path.join('/home/cc/spark-2.1.3-bin-hadoop2.7/mylog', filename), 'r')
            lines = jfile.readlines()
            peak_memory = list()
            for line in lines:
                dict1 = json.loads(line)
                if dict1["Event"] == "SparkListenerStageCompleted":
                    for dict2 in dict1["Stage Info"]["Accumulables"]:
                        if "internal.metrics.input.bytesRead" in dict2.values():
                            rbytes += int(dict2["Value"])
                        if "internal.metrics.output.bytesWritten" in dict2.values():
                            wbytes += int(dict2["Value"])
            jfile.close()
    file = open("log.txt", "a")
    file.write(" bytesRead" + ": " + str(rbytes) + " ")
    file.write(" bytesWritten" + ": " + str(wbytes) + " ")
    file.close()


def collect_mem(workload, i):
    max = 0
    log = open("/home/cc/HiBench/report/" + workload +  "/spark/monitor.log", "r")
    lines = log.readlines()
    for line in lines:
        words = line.split()
        for word in words:
            if word[0:4] == "used":
                temp = int(word[5:-1])
                if temp > max:
                    max = temp
    file = open("log.txt", "a")
    file.write(workload + " " + str(i) + "\n memory(kb): " + str(max) + "\n")
    file.close()


def collect_cputime():
    cputime = 0
    filelist = os.listdir("/home/cc/spark-2.1.3-bin-hadoop2.7/mylog")
    for filename in filelist:
        if 'local' in filename:
            jfile = open(os.path.join('/home/cc/spark-2.1.3-bin-hadoop2.7/mylog', filename), 'r')
            lines = jfile.readlines()
            peak_memory = list()
            for line in lines:
                dict1 = json.loads(line)
                if dict1["Event"] == "SparkListenerStageCompleted":
                    for dict2 in dict1["Stage Info"]["Accumulables"]:
                        if "internal.metrics.executorCpuTime" in dict2.values():
                            cputime += int(dict2["Value"])
            jfile.close()
    file = open("log.txt", "a")
    file.write(" cputime" + ": " + str(cputime) + "\n")
    file.close()


def collect_perf_data():
    f = open("perf.txt", "r")
    lines = f.readlines()
    f.close()
    f1 = open("log.txt", "a")
    for line in lines:
        f1.write(line)
    f1.write("-" * 100 + "\n")
    f1.close()


workload = ['wordcount']
df = pd.read_csv("conf_chaoqin.csv")
for k in range(len(workload)):
    subprocess.call(['./bin/workloads/micro/' + workload[k] + '/prepare/prepare.sh'])
    os.system("rm /home/cc/HiBench/report/hibench.report")
    for i in range(4, -1, -1):
#        i = 1
   # for i in range(df.shape[0]):
        start(workload[k], i)
        time.sleep(2)
        os.system("rm conf/spark.conf")
        os.system("cp conf/spark.conf.template conf/spark.conf")
        os.system("rm /home/cc/spark-2.1.3-bin-hadoop2.7/mylog/*")
        os.system("rm /home/cc/HiBench/report/" + workload[k] +  "/spark/monitor.log")
        # rewrite spark.conf
        f = open(home + '/HiBench/conf/spark.conf', "w")
        f.write("hibench.spark.home      $SPARK_HOME\n")
        f.write("hibench.spark.master  local[*]\n")
        f.write("spark.eventLog.enabled           false\n")
        f.write("spark.sql.shuffle.partitions  ${hibench.default.shuffle.parallelism}\n")
        f.write("spark.default.parallelism     ${hibench.default.map.parallelism}\n")
        f.write('spark.reducer.maxSizeInFlight  ' + str(df.at[i, 'spark.reducer.maxSizeInFlight']) + '\n')
        f.write('spark.shuffle.file.buffer  ' + str(df.at[i, 'spark.shuffle.file.buffer']) + '\n')
        f.write('spark.shuffle.sort.bypassMergeThreshold  ' + str(df.at[i, 'spark.shuffle.sort.bypassMergeThreshold']) + '\n')
        f.write('spark.speculation.interval  ' + str(df.at[i, 'spark.speculation.interval']) + '\n')
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
        f.write('spark.shuffle.spill.compress  ' + str(df.at[i, 'spark.shuffle.spill.compress']) + '\n')
        f.write('spark.broadcast.compress  ' + str(df.at[i, 'spark.broadcast.compress']) + '\n')
        f.write('spark.memory.storageFraction  ' + str(df.at[i, 'spark.memory.storageFraction']) + '\n')
        f.write('spark.eventLog.enabled           true\n')
        f.write('spark.eventLog.dir               /home/cc/spark-2.1.3-bin-hadoop2.7/mylog\n')
        f.write('spark.history.fs.logDirectory    /home/cc/spark-2.1.3-bin-hadoop2.7/mylog\n')
        f.close()
        os.system("rm /home/cc/HiBench/perf.txt")
        time.sleep(2)
        subprocess.call(['perf', 'stat', '-B', '-e', 'branches,branch-misses,bus-cycles,cache-misses,'
                                                     'cache-references,cpu-cycles,instructions,ref-cycles,'
                                                     'alignment-faults,context-switches,cpu-clock,cpu-migrations,'
                                                     'emulation-faults,major-faults,minor-faults,page-faults,'
                                                     'task-clock', '-o', 'perf.txt', './bin/workloads/micro/' + workload[k] +
                         '/spark/run.sh'])
        time.sleep(2)
        end()
        time.sleep(4)
        collect_mem(workload[k], i)
        collect_io()
        collect_cputime()
        collect_perf_data()
        time.sleep(5)
        p = os.system("sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\"")



