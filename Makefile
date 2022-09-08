docker_image:
	docker build -t sds .

pull_elastic:
	docker pull docker.elastic.co/elasticsearch/elasticsearch:8.4.1

all: docker_image pull_elastic

run:
	docker compose -f docker-compose.yml up

debug:
	docker compose up

test_image: docker_image
	docker build -t sds_test tests

test_pre: test_image
	@mkdir esdata | true
	@echo "Cleaning data directory"
	@rm -r data | true
	@mkdir data
	@echo "Starting elastic search service..."
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml up -d elasticsearch
	@echo "Waiting for elastic search service"
	@bash -c "until curl --silent --output /dev/null http://0.0.0.0:9200/_cat/health?h=st; do printf '.'; sleep 5; done; printf '\n'"

	@echo "Cleaning elastic search indices"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/collector"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/expiry"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/_data_stream/dataset"

test: test_pre
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm sds_tests

test_post:
	@docker compose down