import numpy as np
import pandas as pd
import random
import subprocess
import os
import json
import time
from os import path
# np.set_printoptions(precision=2, linewidth=100, suppress=True)
# pd.options.display.float_format = '{:,.2f}'.format
# pd.set_option('display.width',500)
workload = ['lda', 'lr']
df = pd.read_csv("conf_chaoqin.csv")
for k in range(len(workload)):
    subprocess.call(['./bin/workloads/ml/' + workload[k] + '/prepare/prepare.sh'])
    for i in range(pd.shape[0]):
        cur_path = os.path.dirname(__file__)
        f = open(os.path.join(cur_path, '..\\conf\\spark.conf'), "w")
        f.write("hibench.spark.home      $SPARK_HOME")
        f.write("hibench.spark.master  local[*]")
        f.write("spark.eventLog.enabled           true")
        f.write("spark.eventLog.dir               /home/cc/spark-2.1.3-bin-hadoop2.7/mylog")
        f.write("spark.history.fs.logDirectory    /home/cc/spark-2.1.3-bin-hadoop2.7/mylog")
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
        subprocess.call(['./bin/workloads/ml/' + workload[k] + '/spark/run.sh'])
        #time.sleep(15)
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
                            if "internal.metrics.peakExecutionMemory" in dict2.values():
                                peak_memory.append(dict2["Value"])
                jfile.close()
                mem_log = open("memoryLog", "a")
                mem_log.write(str(peak_memory) + "\n")
                #mem_log.write(str(len(filelist)) + "\n")
                mem_log.close()
       # subprocess.call(['rm', '$SPARK_HOME/mylog/*'])
                os.remove(os.path.join('/home/cc/spark-2.1.3-bin-hadoop2.7/mylog', filename))
                #time.sleep(5)                           
    subprocess.call(['mv', 'report/hibench.report', 'report/spark_' + workload[k] + '.report'])
