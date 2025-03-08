.PHONY: build
## Builds the Flink base image with pyFlink and connectors installed
build:
	docker build .

.PHONY: up
## Builds the base Docker image and starts Flink cluster
up:
	docker compose  up --build --remove-orphans  -d

.PHONY: down
## Shuts down the Flink cluster
down:
	docker compose down --remove-orphans


.PHONY: stop
## Stops all services in Docker compose
stop:
	docker compose stop

.PHONY: start
## Starts all services in Docker compose
start:
	docker compose start


minio-start:
	docker exec minioserver

dash-app:
	python  /opt/airflow/dash_app/app.py

