#!/bin/bash -x
nohup java -server -Djava.library.path=./libs/linux -Dorg.eclipse.jetty.server.Request.maxFormContentSize=10000000 -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport $1 &
