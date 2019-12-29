workload = "svm"
raw = open(workload + ".log", "r")
lines = raw.readlines()
raw.close()
csv = open(workload + ".csv", "w+")
csv.write("workload_conf,memory(kb),bytesread,byteswrite,cputime,")
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
csv.write("task-clock(M/sec),CPUs,")
csv.write("duration(s)\n")
metrics1 = ["branches", "branch-misses", "bus-cycles", "cache-misses", "cache-references", "cpu-cycles", "instructions", "ref-cycles"]
metrics2 = ["context-switches", "cpu-migrations", "major-faults", "minor-faults", "emulation-faults", "page-faults"]
metrics3 = ["task-clock", "cpu-clock"]
for line in lines:
    line.strip()
    words = line.split()
    if line[0] == "-":
        continue
    if len(words) == 0:
        continue
    if words[0] == workload:
        csv.write(workload + words[1] + ",")
    elif words[0] == "memory(kb):":
        csv.write(words[1] + ",")
    elif words[0] == "bytesRead:":
        csv.write(words[1].replace(",", "") + ",")
        csv.write(words[3].replace(",", "") + ",")
        csv.write(words[5].replace(",", "") + ",")
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


