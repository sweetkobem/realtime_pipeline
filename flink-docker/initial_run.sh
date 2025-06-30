#!/bin/bash

set -e

echo "Prequist: Download Jar Dependency..."
cd flink-scala-scripts
mvn dependency:copy-dependencies -DoutputDirectory=../flink-jars
cd ..

echo "Step 1: Building Scala Flink jobs with Maven..."
cd flink-scala-scripts
mvn clean package -U
cd ..

echo "Step 2: Restarting job inside Docker container..."
docker exec -it flink_jobmanager bash -c "/opt/flink/manage_scala_jobs.sh restart"