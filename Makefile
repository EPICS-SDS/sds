docker_image:
	docker build -t sds .

pull_elastic:
	docker pull docker.elastic.co/elasticsearch/elasticsearch:8.3.3

all: docker_image pull_elastic

run:
	docker compose -f docker-compose.yml up

debug:
	docker compose up

test_image: docker_image
	docker build -t sds_test tests

test_pre:
	rm -r esdata | true
	mkdir esdata
	docker compose -f docker-compose.yml -f docker-compose.tests.yml up -d elasticsearch sds_indexer sds_retriever

test: test_image
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm sds_tests

test_post:
	docker compose down