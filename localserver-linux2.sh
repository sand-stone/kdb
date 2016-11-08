#!/bin/bash
java -server -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-1.cnf &
java -server -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-2.cnf &
java -server -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-3.cnf &
