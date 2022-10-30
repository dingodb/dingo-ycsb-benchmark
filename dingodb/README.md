<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on [DingoDB](https://github.com/dingodb/dingo). 

### 1. Start DingoCluster

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

```shell
    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:dingodb-binding -am clean package
```

### 4. Create Table

#### 4.1 Copy package to Dingo cluster

***ycsb-dingodb-binding-*-SNAPSHOT.tar.gz*** will be found in directory ***dingodb/target***.


#### 4.2 Create table in Dingo cluster

- extract `tar.gz` package

- execute command to create table
```shell
java -cp "lib/*" site.ycsb.db.DingoDBTableCommand -c command=create -p coordinator.host=172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181 -n bench1 -f 10
```

#### 4.3 Other commands

If you want to drop table, you can use command as follows:
```shell
java -cp "lib/*" site.ycsb.db.DingoDBCreateTable -c command=drop -p coordinator.host=172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181 -n bench1
```

### 5. Provide DingoDB Connection Parameters
    
Set coordinator host list and table name  in the workload you plan to run. 

- `coordinator.host`
- `dingo.table`
  * The table created in [4.2](README.md).

Or, you can set configs with the shell command, EG:

```shell
    ./bin/ycsb load dingodb -s -P workloads/workloada -p "coordinator.host=172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181" -p "dingo.table=usertable" > outputLoad.txt
```

### 6. Load data and run tests 

Load the data:

```shell
    ./bin/ycsb load dingodb -s -P workloads/workloada -p "coordinator.host=172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181" -p "dingo.table=usertable" > outputLoad.txt
```

Run the workload test:

```shell
./bin/ycsb run dingodb -s -P workloads/workloada -p "coordinator.host=172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181" -p "dingo.table=usertable"
```
