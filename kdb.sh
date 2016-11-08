#!/bin/bash -x
nohup java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport $1 &
