#!/bin/bash
java -Djava.library.path=./libs/darwin -Dio.netty.leakDetection.level=advanced -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode1.properties &
java -Djava.library.path=./libs/darwin -Dio.netty.leakDetection.level=advanced -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode2.properties &
java -Djava.library.path=./libs/darwin -Dio.netty.leakDetection.level=advanced -cp target/kdb-1.0-SNAPSHOT.jar kdb.JettyTransport conf/datanode3.properties &
