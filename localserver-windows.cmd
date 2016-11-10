#!/bin/bash
start java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/windows -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode1.properties
start java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/windows -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode2.properties
start java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/windows -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode3.properties
