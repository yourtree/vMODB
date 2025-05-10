# TPC-C in vMODB

This project contains the modules to execute the warehouse, inventory, and order services of vMODB for the TPC-C benchmark.

## Running an experiment

Make sure that warehouse, inventory, and order services are up and running. Refer to their respective README files and the README found in the project's base folder to compile and execute the services.

After, you can start the "proxy" service, which represents the coordinator. Through the proxy, you manage an experiment lifecycle of vMODB. The following menu is presented to the user:

=== Main Menu ===
1. Create tables in disk
2. Load services with data from tables in disk
3. Create workload
4. Submit workload
5. Reset service states
0. Exit

