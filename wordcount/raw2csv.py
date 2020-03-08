import pandas
workload = "wordcount"

def for_each_raw2csv(raw_file, csv_file):
    raw = open(raw_file, "r")
    lines = raw.readlines()
    raw.close()
    csv = open(csv_file, "w+")
    csv.write("branches,branches(M/sec),branches(%),")
    csv.write("branch-misses,branch-misses(% of all),branch-misses(%),")
    csv.write("bus-cycles,bus-cycles(M/sec),bus-cycles(%),")
    csv.write("cache-misses,cache-misses(% of all),cache-misses(%),")
    csv.write("cache-references,cache-references(M/sec),cache-references(%),")
    csv.write("cpu-cycles,cpu-cyclesGHz,cpu-cycles(%),")
    csv.write("instructions,instructions(per cycle),instructions(%),")
    csv.write("ref-cycles,ref-cycles(M/sec),ref-cycles(%),")
    metrics0 = ["alignment-faults", "context-switches", "cpu-clock", "cpu-migrations", "emulation-faults", "major-faults", "minor-faults", "page-faults"]
    msec = ["cpu-clock", "minor-faults", "page-faults"]
    ksec = ["context-switches", "cpu-migrations", "emulation-faults", "major-faults"]
    for metric in metrics0:
        if metric in msec:
            csv.write(metric + "," + metric + "(M/sec),")
        elif metric in ksec:
            csv.write(metric + "," + metric + "(K/sec),")
    csv.write("task-clock(msec),CPUs,")
    csv.write("duration(s)\n")
    metrics1 = ["branches", "branch-misses", "bus-cycles", "cache-misses", "cache-references", "cpu-cycles", "instructions", "ref-cycles"]
    metrics2 = ["context-switches", "cpu-migrations", "major-faults", "minor-faults", "emulation-faults", "page-faults"]
    metrics3 = ["task-clock", "cpu-clock"]
    for line in lines:
        line.strip()
        words = line.split()
        if line[0:3] == "sta":
            words = line.split('-')
        if line[0] == "-":
            continue
        if len(words) == 0:
            continue
        if words[1] == workload:
            continue
        elif words[1] in metrics1:
            csv.write(words[0].replace(",", "") + ",")
            csv.write(words[3].replace(",", "") + ",")
            csv.write(words[-1][1:-1] + ",")
        elif words[1] in metrics2:
            csv.write(words[0].replace(",", "") + ",")
            csv.write(words[3].replace(",", "") + ",")
        elif words[1] in metrics3:
            csv.write(words[0].replace(",", "") + ",")
            csv.write(words[4].replace(",", "") + ",")
        elif words[1] == "seconds":
            csv.write(words[0].replace(",", "") + "\n")
    csv.close()


for_each_raw2csv("master.txt", "master.csv")
for_each_raw2csv("slave1.txt", "slave1.csv")
for_each_raw2csv("slave2.txt", "slave2.csv")


raw = open("master.txt", "r")
csv = open("mem_io.csv", "w+")
csv.write("m-network-i(kb),m-network-o(kb),m-peak-memory(kb),s1-network-i(kb),s1-network-o(kb),s1-peak-memory(kb),s2-network-i(kb),s2-network-o(kb),s2-peak-memory(kb)\n")
nodes = {'10.140.81.201': 1, '10.140.81.166': 2, '10.140.83.35': 0}
val = [[], [], []]
lines = raw.readlines()
for line in lines:
    line = line.replace(":", " ")
    words = line.split(" ")
    if line[0] == '-':
        l = ""
        for wordl in val:
            for word in wordl:
                l += word
                l += ','
        l = l[:-2]
        l += '\n'
        val = [[], [], []]
        csv.write(l)
        continue
    if len(words) <= 1 or words[1] != "network_i(kb)":
        continue
    node = nodes[words[0]]
    val[node].append(words[2])
    val[node].append(words[4])
    val[node].append(words[6].strip())
raw.close()
csv.close()


def df_transform(csvfile):
    node_dict = {"master.csv": "m", "slave1.csv": "s1", "slave2.csv": "s2"}
    node = node_dict[csvfile]
    df = pandas.read_csv(csvfile)
    if node == 's1' or node == 's2':
        del df["duration(s)"]
    cols = list(df.columns.values)
    for col in cols:
        df.rename(columns={col: node + "-" + col}, inplace=True)
    cols = list(df.columns.values)
    return df

df_io_mem = pandas.read_csv("mem_io.csv")
df_m = df_transform("master.csv")
df_s1 = df_transform("slave1.csv")
df_s2 = df_transform("slave2.csv")
result = df_io_mem.join(df_m).join(df_s1).join(df_s2)

def percentage_to_abs(per):
    return float(per[:-2]) / 100
for i in range(0, result.shape[0]):
    for j in range(0, result.shape[1]):
        if str(result.iloc[i, j])[-1] == '%':
            result.iloc[i, j] = percentage_to_abs(result.iloc[i, j])
filename = workload + ".csv"
result.to_csv(filename)















