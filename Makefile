.PHONY: run fmt test update push

run:
	go run cmd/main.go --config=./internal/config/config.dev.yaml

fmt:
	go fmt ./...
	go mod tidy

test:
	@echo "Running tests..."
	go test ./... -v
	@echo "Tests completed."

update:
	@echo "Updating dependencies..."
	go get -u ./...
	@echo "Dependencies updated."

push:
	@if [ -z "$(COMMIT_MSG)" ]; then \
		echo "❌ COMMIT_MSG is required. Usage: make push COMMIT_MSG=\"your message\""; \
		exit 1; \
	fi

	@$(MAKE) fmt
	@$(MAKE) test || { echo "❌ Tests failed. Aborting push."; exit 1; }

	@echo "✅ Tests passed."
	@echo "Adding changes to git..."
	git add .

	@echo "Committing changes with message: $(COMMIT_MSG)"
	git commit -m "$(COMMIT_MSG)" || echo "No changes to commit."

	@echo "Pushing changes to remote repository..."
	git push origin main

	@echo "✅ All tasks completed successfully."
