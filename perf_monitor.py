import os
import time
from os import path
from os.path import expanduser
import subprocess

def detect_signal():
    filelist = os.listdir("/home/cc")
    for filename in filelist:
        if "start" in filename:
            subprocess.call(['sudo', 'perf', 'stat', '-a', '-B', '-e', 'branches,branch-misses,bus-cycles,cache-misses,'
                                                         'cache-references,cpu-cycles,instructions,ref-cycles,'
                                                         'alignment-faults,context-switches,cpu-clock,cpu-migrations,'
                                                         'emulation-faults,major-faults,minor-faults,page-faults,'
                                                         'task-clock', '-o', 'perf.txt', 'python3.7', 'nothing.py'])
            os.system("rm start*")
            time.sleep(1)
            file = open("log.txt", "a")
            file.write(filename + "\n")
            file.close()
        if "end" in filename:
            time.sleep(1.5)
            os.system("rm end")
            time.sleep(1.5)
            f = open("perf.txt", "r")
            lines = f.readlines()
            f.close()
            f1 = open("log.txt", "a")
            for line in lines:
                f1.write(line)
            f1.write("-" * 100 + "\n")
            f1.close()

while True:
    detect_signal()
    time.sleep(0.5)
