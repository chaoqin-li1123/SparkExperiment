import os
import time
from os import path
from os.path import expanduser
import subprocess

def detect_signal():
    filelist = os.listdir("/home/cc")
    for filename in filelist:
        if "start" in filename:
            file = open("slave1.txt", "a")
            file.write(filename + "\n")
            file.close()
            os.system("rm " + filename)
#            time.sleep(1.5)
            os.system("sudo perf stat -B -a -e branches,branch-misses,bus-cycles,cache-misses,cache-references,"
                      "cpu-cycles,instructions,ref-cycles,alignment-faults,context-switches,cpu-clock,cpu-migrations,"
                      "emulation-faults,major-faults,minor-faults,page-faults,task-clock -o perf.txt python3.7 nothing.py") 
            time.sleep(4)
        if "end" in filename:
            time.sleep(1)
            os.system("rm end")
            time.sleep(4)
            f = open("perf.txt", "r")
            lines = f.readlines()
            f.close()
            f1 = open("slave1.txt", "a")
            for line in lines:
                f1.write(line)
            f1.write("-" * 100 + "\n")
            f1.close()

while True:
    detect_signal()
    time.sleep(5)
