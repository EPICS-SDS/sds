indexer_image:
	docker build -t sds_indexer -f indexer/Dockerfile .

retriever_image:
	docker build -t sds_retriever -f retriever/Dockerfile .


all: indexer_image retriever_image

run:
	docker-compose up