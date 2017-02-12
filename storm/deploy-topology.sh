#!/bin/bash
echo "Waiting for storm nimbus..."

while ! nc -z nimbus 6627; do   
  sleep 0.1 
done
/apache-storm-1.0.2/bin/storm jar /apache-storm-*/examples/storm-starter/storm-starter-topologies-*.jar org.apache.storm.starter.WordCountTopology topology
