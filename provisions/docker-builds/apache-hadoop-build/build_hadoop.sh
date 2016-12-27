#!/bin/bash

VERSION=${1:-2.7.1}
BUILD_DIR=/hadoop_build
HADOOP_URL=http://apache.osuosl.org/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}-src.tar.gz

rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR
cd $BUILD_DIR
curl -L $HADOOP_URL | tar xz --strip-components=1
ls -l
/usr/local/bin/mvn package -Pdist,native -DskipTests -Dmaven.test.skip -Dtar

