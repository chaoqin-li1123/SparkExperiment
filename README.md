# Set up Hadoop cluster
## Follow the instruction in the following link to set up a hadoop cluster.[Hadoop Cluster Setup Tutorial](https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster)
## Run a small hadoop workload to make sure that the hadoop cluster work correctly.

# Set up spark cluster
## Follow the instruction in the following link to set up a spark cluster. [Spark Cluster Setup Tutorial](https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b)

# Build Hibench and configure it.
## Download Hibench and build it. [Hibench Github Repo](https://github.com/Intel-bigdata/HiBench)
## Configure "conf/hadoop.conf"
## Configure "conf/spark.conf"
## Configure "conf/hibench.conf"
## "sudo apt-get install bc", otherwise the monitoring system won't generate any report.
## Run a Hadoop bench and a Spark bench to make sure the benchmark suite work correctly.


