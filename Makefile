.PROJECT_ROOT=$(shell pwd)

.PHONY: test

test:
	docker container run --rm -it -v $(.PROJECT_ROOT):/app -w /app/ golang:1.16 go test -v ./...
