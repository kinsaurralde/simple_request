#!/bin/bash
set -e
LIB_DIR=$(pwd)/src/main/java/com/google/spanner/cloud/rain/lib
rm -rf $LIB_DIR
mkdir -p $LIB_DIR
cd java-spanner/google-cloud-spanner
mvn dependency:copy-dependencies -DoutputDirectory=$LIB_DIR
cd ../..
cp java-spanner/google-cloud-spanner/target/google-cloud-spanner-*.jar $LIB_DIR/
ls -l $LIB_DIR/