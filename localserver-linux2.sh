#!/bin/bash
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-1.cnf &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-2.cnf &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2-3.cnf &
