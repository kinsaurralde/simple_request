#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Building and setting up java-spanner dependencies..."

# Step 1: Navigate to grpc-gcp-java/grpc-gcp and build the project
echo "Step 1: Formatting code in grpc-gcp-java/grpc-gcp..."
cd grpc-gcp-java/grpc-gcp
./gradlew googleJavaFormat

# Step 2: Fix any formatting issues in grpc-gcp-java/grpc-gcp
echo "Step 2: Building grpc-gcp-java/grpc-gcp..."
./gradlew build

# Step 3: Publish grpc-gcp-java/grpc-gcp to the local Maven repository
echo "Step 3: Publishing grpc-gcp-java/grpc-gcp to local Maven repository..."
./gradlew publishToMavenLocal
cd ../.. # Go back to the root directory

# Step 4: Build java-spanner, which will now use the locally published grpc-gcp
echo "Step 4: Building java-spanner (using local grpc-gcp)..."
cd java-spanner
mvn clean install -DskipTests
cd .. # Go back to the root directory

echo "java-spanner dependencies setup complete!"
