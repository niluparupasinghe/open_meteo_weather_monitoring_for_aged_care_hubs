.PHONY: setup build run verify repair test dashboard clean

# Copy .env.example to .env if not already present (cross-platform POSIX shell)
setup:
	@test -f .env || cp .env.example .env

# Build the Docker image (Python 3.11 + delta-rs + DuckDB + Streamlit)
build:
	docker compose build

# Run the full 7-step Medallion pipeline inside Docker
run:
	docker compose run --rm pipeline python -m src.cli run-all

# Verify all 10 tables and print summary statistics
verify:
	docker compose run --rm pipeline python -m src.cli verify

# Repair a failed pipeline run from its last failed step (pass RUN_ID=...)
repair:
	docker compose run --rm pipeline python -m src.cli repair --run-id $(RUN_ID)

# Execute the full automated pytest suite inside Docker
test:
	docker compose run --rm pipeline pytest -v

# Start both the pipeline (one-shot) and the interactive Streamlit dashboard on http://localhost:8501
dashboard:
	docker compose up

# Clean Docker containers and local runtime data/logs
clean:
	docker compose down -v --remove-orphans
	rm -rf ./data ./logs
