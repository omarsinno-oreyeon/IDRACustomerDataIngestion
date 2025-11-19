install:
	pip install -r requirements.txt

db-run:
	python app/offline_db.py

db-query:
	python app/online_db.py

db-ingest:
	python app/ingest.py
