#!/bin/bash
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/darwin -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode1.properties &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/darwin -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode2.properties &
java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/darwin -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode3.properties &
