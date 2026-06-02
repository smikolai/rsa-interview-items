.PHONY: run clean

run:
	docker compose up --build -d
	@echo "Waiting for Jupyter to start..."
	@until curl -s http://localhost:8888/api > /dev/null; do sleep 1; done
	open http://localhost:8888

stop:
	docker compose down
	rm -rf notebooks/spark-warehouse
	rm -rf notebooks/.ipynb_checkpoints
	rm -rf notebooks/metastore_db
	rm -f notebooks/derby.log
	find . -type d -name __pycache__ -exec rm -rf {} +
