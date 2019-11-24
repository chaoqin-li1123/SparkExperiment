import json
jfile = open("secret1.txt", 'r')
lines = jfile.readlines()
cpu = list()
for line in lines:
    dict1 = json.loads(line)
    if dict1["Event"] == "SparkListenerStageCompleted":
        for dict2 in dict1["Stage Info"]["Accumulables"]:
            if "internal.metrics.executorCpuTime" in dict2.values():
                cpu.append(dict2["Value"])
jfile.close()
print(cpu)