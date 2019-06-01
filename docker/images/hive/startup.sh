#!/bin/bash

hadoop fs -mkdir       /tmp
hadoop fs -mkdir -p    /user/hive/warehouse
hadoop fs -chmod g+w   /tmp
hadoop fs -chmod g+w   /user/hive/warehouse

cd ${HIVE_HOME}/bin
export AUX_CLASSPATH=file://${DAFLOW_BUNDLE}
./hiveserver2 --hiveconf hive.server2.enable.doAs=false  --hiveconf hive.aux.jars.path=file://${DAFLOW_BUNDLE}
