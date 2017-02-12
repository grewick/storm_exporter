# prometheus-storm-exporter
A prometheus exporter for apache storm metrics

Exports storm metrics exposed by the [storm-ui rest interface](http://storm.apache.org/releases/1.0.2/STORM-UI-REST-API.html) 

## Quick Start (with Grafana as frontend)
```
docker-compose up -d
```
- Login to http://localhost:3000/  (user:admin, password: admin)
- Create new dashboard
- Add storm-metrics to dashboard (choose prometheus as data source)

## Usage
As python script:
```
storm-metrics-consumer.py [StormUI Host] [HTTP port of the consumer] [Refresh Rate in Seconds]
```

As docker container:
```
docker build -t storm_exporter .
docker run --rm -e STORM_UI_HOST=storm-ui:8080 -e REFRESH_RATE=5 -p 8000:8000 --name storm_exp storm_exporter
```

## Notes
- This project uses the [prometheus python_client library](https://github.com/prometheus/client_python)
- The docker-compose setup uses
	- [storm images by 31z4](https://github.com/31z4/storm-docker)
	- [official zookeeper image](https://hub.docker.com/r/_/zookeeper/)
	- [official prometheus image](https://hub.docker.com/r/prom/prometheus/)
	- [grafana image](https://hub.docker.com/r/grafana/grafana/)

