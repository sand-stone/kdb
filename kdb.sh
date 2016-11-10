#!/bin/bash -x
nohup java -server -Djava.library.path=./libs/linux -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport $1 &
