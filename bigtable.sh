#!/bin/sh

LOCAL_REPOSITORY=`mvn help:evaluate -Dexpression=settings.localRepository | grep -v '[INFO]'`
ALPN_VERSION=8.1.3.v20150130

mvn dependency:build-classpath -Dmdep.outputFile=cp.txt > /dev/null
CPATH=`cat cp.txt`
rm cp.txt

java -classpath target/classes:${CPATH} -Xbootclasspath/p:${LOCAL_REPOSITORY}/org/mortbay/jetty/alpn/alpn-boot/${ALPN_VERSION}/alpn-boot-${ALPN_VERSION}.jar sam.bigtable.BigtableTest $@
