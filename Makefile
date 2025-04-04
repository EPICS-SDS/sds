docker_image:
	@echo "Building SDS Docker image"
	@docker build -t sds . > /dev/null
	@touch docker_image.lock

docker_image.lock: 
	@echo "Building SDS Docker image"
	@docker build -t sds . > /dev/null
	@touch docker_image.lock

clean_docker_image_lock:
	@rm -f docker_image.lock

clean:
	@rm -f docker_image.lock
	@rm -f test_image.lock
	@rm -f test_services.lock
	@rm -f swagger_redoc_files.lock
	@rm -rf src/static

pull_elastic:
	docker compose pull elasticsearch
	docker compose pull kibana

swagger_redoc_files.lock: pull_swagger_redoc

pull_swagger_redoc:
	mkdir src/static | true
	curl -o src/static/swagger-ui-bundle.js https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js
	curl -o src/static/swagger-ui.css https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css
	curl -o src/static/redoc.standalone.js https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js
	touch swagger_redoc_files.lock

build: clean_docker_image_lock docker_image pull_elastic swagger_redoc_files.lock

run:
	docker compose -f docker-compose.yml up -d

debug:
	docker compose up -d

stop:
	docker compose down

test_image: test_image.lock

test_image.lock: docker_image.lock
	@echo "Building test Docker image"
	@docker build -t sds_test tests > /dev/null

	@touch test_image.lock

test_clean:
	@echo "Cleaning data directory"
	@rm -rf data/* | true

test_services.lock: test_image test_clean
	@mkdir -p esdata | true
	@mkdir -p data | true
	@chmod 777 esdata
	@chmod 777 data
	@echo "Starting elastic search service..."
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml up -d elasticsearch
	@echo "Waiting for elastic search service"
	@bash -c "until curl --silent --output /dev/null http://localhost:9200/_cat/health?h=st; do printf '.'; sleep 5; done; printf '\n'"

	@echo "Cleaning elastic search indices"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/collector"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/expiry"
	@bash -c "curl --silent --output /dev/null  -X DELETE localhost:9200/_data_stream/dataset"

	@touch test_services.lock

test_pre: test_services.lock

test: test_services.lock
	docker compose -f docker-compose.yml -f docker-compose.tests.yml run --rm sds_tests

test_ioc: test_image.lock
	@echo "Starting IOC for performance tests"
	@docker compose up -d test_ioc

stop_test_ioc: test_image.lock
	@echo "Stoping IOC for performance tests"
	@docker compose down test_ioc

ifndef IOC_ADDR
test_perf: test_ioc test_services.lock
endif

ifdef IOC_ADDR
test_perf: test_services.lock
endif

test_perf:
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml run -e EPICS_PVA_ADDR_LIST=${IOC_ADDR} --rm sds_tests python -m pytest tests/performance -s -v --no-header

test_post:
	@rm -f test_image.lock
	@rm -f test_services.lock
	@docker compose -f docker-compose.yml -f docker-compose.tests.yml down
