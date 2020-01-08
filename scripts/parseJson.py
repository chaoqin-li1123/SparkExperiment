import json

jfile = open("local-1573356121862", "r")
lines = jfile.readlines()
peak_memory = list()
for line in lines:
    dict1 = json.loads(line)
    if dict1["Event"] == "SparkListenerStageCompleted":
        for dict2 in dict1["Stage Info"]["Accumulables"]:
            if "internal.metrics.peakExecutionMemory" in dict2.values():
                peak_memory.append(dict2["Value"])
print(peak_memory)
