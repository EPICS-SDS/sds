docker_image:
	docker build -t sds .

pull_elastic:
	docker pull docker.elastic.co/elasticsearch/elasticsearch:8.3.2

all: docker_image pull_elastic

run:
	docker compose -f docker-compose.yml up

debug:
	docker compose up