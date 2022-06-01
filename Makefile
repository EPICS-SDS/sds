storage_image:
	docker build -t sds_storage -f storage/Dockerfile .

retriever_image:
	docker build -t sds_retriever -f retriever/Dockerfile .


all: storage_image retriever_image

run:
	docker-compose up