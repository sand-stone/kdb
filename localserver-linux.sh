#!/bin/bash
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode1.properties &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux  -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2.properties &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux  -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode3.properties &
