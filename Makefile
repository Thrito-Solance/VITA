.PHONY: up down clean logs test test-integration seed inspect

up:
	docker compose up -d --build
	@echo "waiting for Kafka..."
	@docker compose exec -T kafka sh -c \
	  'until /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done'
	@echo "ready."

down:  ; docker compose down
clean: ; docker compose down -v
logs:  ; docker compose logs -f processor

test:
	docker compose run --rm --build processor-dev \
	  pytest -q tests/test_vita_resilience.py

test-integration: up
	docker compose run --rm processor-dev \
	  pytest -q -m integration tests/test_integration.py

seed:
	docker compose run --rm processor-dev python -m scripts.seed

inspect:
	docker compose run --rm processor-dev python -m vita.cli inspect
