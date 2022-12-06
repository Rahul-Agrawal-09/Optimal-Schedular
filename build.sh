#!/bin/sh

echo 'Cleaning class directory...'
mvn clean

echo 'Compiling sources and building jar with dependencies...'
mvn assembly:assembly

echo 'Copying jar in Storm libraries'
cp target/autoscale-2.0.0-SNAPSHOT-jar-with-dependencies.jar $STORM_HOME/lib/

echo 'Autoscale library updated!'