format:
	black . --line-length=120
	isort . --profile black

load_db:
	python load_db.py

info:
	python info.py