#!/bin/bash
set -e

echo "Building java-spanner..."
cd java-spanner
mvn clean install -DskipTests
cd ..

echo "Copying dependencies..."
./copy_deps.sh

#echo "Running simple_request..."
#bazel run src/main/java/com/google/spanner/cloud/rain:simple_request
