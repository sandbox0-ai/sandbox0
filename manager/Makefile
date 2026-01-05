.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o bin/manager cmd/manager/main.go

.PHONY: docker-build
docker-build:
	docker build -t sandbox0/manager:latest .

.PHONY: docker-push
docker-push:
	docker push sandbox0/manager:latest

.PHONY: deploy
deploy:
	kubectl apply -f deploy/k8s/

.PHONY: clean
clean:
	rm -rf bin/

.PHONY: test
test:
	go test -v ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

