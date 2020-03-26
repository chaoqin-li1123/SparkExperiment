# Set up Hadoop cluster
- Follow the instruction in the following link to set up a hadoop cluster.[Hadoop Cluster Setup Tutorial](https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster)
- Set up passwordless ssh.
- Modify configuration files.
- Start the cluster manually.
- Run a small hadoop workload to make sure that the hadoop cluster work correctly.

# Set up spark cluster
- Follow the instruction in the following link to set up a spark cluster. [Spark Cluster Setup Tutorial](https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b)
- You can choose to use Spark stanalone mode or Yarn as resource manager.

# Build Hibench and configure it.
- Download Hibench and build it. [Hibench Github Repo](https://github.com/Intel-bigdata/HiBench)
- Configure "conf/hadoop.conf".
- Configure "conf/spark.conf".
- Configure "conf/hibench.conf".
- "sudo apt-get install bc", otherwise the monitoring system won't generate any report.
- Run a Hadoop bench and a Spark bench to make sure the benchmark suite work correctly.

# Run Benchmark
- Modify "conf/spark.conf" to reflect changes in spark configuration. For each row in configuration file, change the "conf/spark.conf" accordingly. All configuration parameters I used can be found in [spark configuration](https://github.com/chaoqin-li1123/spark_experiment_chaoqin/blob/master/conf_chaoqin.csv). If you plan to generate new configuration for further analysis, check [Spark configuration generator](https://github.com/chaoqin-li1123/spark_experiment_chaoqin/blob/master/scripts/conf_generator.py).

## conf_generator.py
- Generate N rows of random Spark parameters and convert into csv file.

## master_run.py
### Data preparation
- Prepare the data: subprocess.call(['./bin/workloads/micro/' + workload[k] + '/prepare/prepare.sh'])
### Repeat the following steps for N times.
- Open spark.conf and overwrite its content.
- Signal the slave node by creating an empty file in each slave node.
- Use perf to spawn the hibench process.       
subprocess.call(['perf', 'stat', '-B', '-e', 'branches,branch-misses,bus-cycles,cache-misses,'
                                                     'cache-references,cpu-cycles,instructions,ref-cycles,'
                                                     'alignment-faults,context-switches,cpu-clock,cpu-migrations,'
                                                     'emulation-faults,major-faults,minor-faults,page-faults,'
                                                     'task-clock', '-o', 'perf.txt', './bin/workloads/micro/' + workload[k] +
                                                     '/spark/run.sh'])
 - When the spark job finish, signal the slave nodes by creating an empty file in each slave node.
 
 
                                                   


