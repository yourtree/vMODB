# TPC-C Proxy

This project is responsible to execute the coordinator service of vMODB for the TPC-C benchmark.

## Running the project

If you are in this project's root folder, run the project with the following command:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/proxy-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

If you are outside the vms-runtime-java folder, run:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar vms-runtime-java/tpcc/proxy-tpcc/target/proxy-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

If you are inside the vms-runtime-java folder, run:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar tpcc/proxy-tpcc/target/proxy-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Running experiments

When executing the project, a menu will show up on screen. The menu offers different functionalities to manage the parameters of a TPC-C experiment.

Make sure warehouse-tpcc, inventory-tpcc, and order-tpcc are deployed before initializing an experiment through <i>TPC-C Proxy</i>.

For number of warehouses == 8, it is recommended to increase the number of transaction workers to at least 2 in order to experience a higher throughput.

For number of warehouses >= 16, it is recommended to increase the number of transaction workers to 4 to get the highest throughput possible.

For number of warehouses >= 32, make sure to increase the heap size to match the size of the input tables. You can use the command below:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar tpcc/proxy-tpcc/target/proxy-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar -Xms100g
```

## Troubleshooting

Make sure to generate enough transactions for your experiment. It should account for the period selected. If less transaction inputs are available, that can lead to errors.

Run the following commands in case the proxy failed. That will allow identifying the PID in order to shut it down before proceeding with the experiments.
```
lsof -n -i :8091 | grep LISTEN
```
```
sudo netstat -nlp | grep :8091
```
