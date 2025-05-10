.PHONY: run

run:
	go run cmd/main.go --config=config.dev.yaml


.PHONY: fmt
fmt:
	go fmt ./...
	go mod tidy

.PHONY: test
test:
	@echo "Running tests..."
	go test ./... -v
	@echo "Tests completed."

.PHONY: update
update:
	@echo "Updating dependencies..."
	go get -u ./...
	@echo "Dependencies updated."

.PHONY: push
push:
	@if [ -z "$(COMMIT_MSG)" ]; then \
		echo "❌ COMMIT_MSG is required. Usage: make push COMMIT_MSG=\"your message\""; \
		exit 1; \
	fi

	@echo "Running go fmt..."
	go fmt ./...

	@echo "Running go test..."
	go test ./... -v
	@if [ $$? -ne 0 ]; then \
		echo "❌ Tests failed. Aborting push."; \
		exit 1; \
	fi

	@echo "✅ Tests passed."
	@echo "Adding changes to git..."
	git add .

	@echo "Committing changes with message: $(COMMIT_MSG)"
	git commit -m "$(COMMIT_MSG)" || echo "No changes to commit."

	@echo "Pushing changes to remote repository..."
	git push origin main

	@echo "✅ All tasks completed successfully."
