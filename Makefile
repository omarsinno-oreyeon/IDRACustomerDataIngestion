USERID ?= 333
BUCKET ?= idra-commercial

install:
	pip install -r requirements.txt

db-run:
	python app/offline_db.py

db-query:
	python app/online_db.py

db-ingest:
	python app/ingest.py

ingest-data:
	python app/offline_db.py --run-id $(RUNID)
	python app/ingest.py --run-id $(RUNID) --user-id $(USERID) --bucket-name $(BUCKET)