#!/bin/bash

# Navigate to the grpc-gcp directory
pushd grpc-gcp-java/grpc-gcp

# Fix the missing semicolon in GcpFallbackChannel.java
#sed -i 's/System.out.println("Make call to primaryChannel")/System.out.println("Make call to primaryChannel");/' src/main/java/com/google/cloud/grpc/fallback/GcpFallbackChannel.java

# Build grpc-gcp
./gradlew build

# Navigate back to the original directory
popd

# Copy the built jar to the target location
cp grpc-gcp-java/grpc-gcp/build/libs/grpc-gcp-1.7.1-SNAPSHOT.jar src/main/java/com/google/spanner/cloud/rain/lib/
