SERVICES := scheduler prober resultprocessor api

.PHONY: build test lint fmt docker-build up down migrate

build:
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/$$s ./cmd/$$s; \
	done

test:
	go test ./... -race -count=1

lint:
	go vet ./...
	gofmt -l .

fmt:
	gofmt -w .

docker-build:
	@for s in $(SERVICES); do \
		docker build --build-arg SERVICE=$$s -t uptime-monitor-$$s:latest .; \
	done

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

migrate:
	@for f in migrations/*.sql; do \
		echo "applying $$f"; \
		psql "$$UPTIME_POSTGRES_DSN" -f $$f; \
	done
