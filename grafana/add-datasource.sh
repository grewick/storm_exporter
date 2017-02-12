#!/bin/sh
echo "Waiting for Grafana..."

while ! nc -z grafana 3000; do   
  sleep 0.1 
done

echo "Waiting for Prometheus..."

while ! nc -z prometheus 9090; do   
  sleep 0.1 
done

curl -X POST -H "Content-Type: application/json" -H "Authorization: Basic YWRtaW46YWRtaW4=" -H "Cache-Control: no-cache" -H "Postman-Token: b291fea6-d850-9d64-4b6d-9e34c45e046b" -d '{
  "name":"Prometheus",
  "type":"prometheus",
  "url":"http://prometheus:9090",
  "access":"proxy",
  "basicAuth":false
}' "http://grafana:3000/api/datasources"