docker_image: docker_image.lock

docker_image.lock:
	@echo "Building SDS Docker image"
	@docker build -t sds . > /dev/null
	@touch docker_image.lock

pull_elastic:
	docker pull docker.elastic.co/elasticsearch/elasticsearch:8.4.2
	docker pull docker.elastic.co/kibana/kibana:8.4.2

build: docker_image pull_elastic

run:
	docker compose -f docker-compose.yml up

debug:
	docker compose up

test_image: test_image.lock

test_image.lock: docker_image.lock
	@echo "Building test Docker image"
	@docker build -t sds_test tests > /dev/null

	@touch test_image.lock

test_services.lock: test_image.lock
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

	@touch test_services.lock

test_pre: test_services.lock

test: test_services.lock
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm sds_tests

test_ioc: test_services.lock
	@echo "Starting IOC for performance tests"
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml up -d sds_test_ioc

ifndef IOC_ADDR
test_perf: test_ioc
endif

ifdef IOC_ADDR
test_perf: test_services.lock
endif

test_perf:
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm sds_tests python -m pytest tests/performance --junitxml=junit.xml --cov-config tests/.coveragerc --cov-report html --cov '.'

test_post:
	@rm -f docker_image.lock
	@rm -f test_image.lock
	@rm -f test_services.lock
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml down
