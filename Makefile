.PHONY: run

run:
	 go run cmd/main.go \
  --nats-url "$NATS_URL" \
  --db-url "$DATABASE_URL" \
  --log-level debug 